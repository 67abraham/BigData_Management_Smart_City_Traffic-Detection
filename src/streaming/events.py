"""Replay event format, parsing, windows and verification helpers (spec T04). Pure Python.

Used by the producer, the Flink job (parse + window metrics run inside Flink's Python workers) and
the verifier, so all three agree on one definition. No Flink/Kafka imports here.

Message (JSON, one per Kafka record, key = station_id):
  event_id        "<station_id>|<event_time>"  deterministic, so a re-sent record is a duplicate
  record_kind     historical_event_replay | injected_test_event | replay_flush_marker
  station_id, road, direction
  event_time      hour start, UTC, "YYYY-MM-DDTHH:MM:SSZ"   (the event time Flink uses)
  traffic_volume  int >= 0, unit vehicles/hour
  source          lineage of the replayed record (dataset, version, Silver run id)
  replay_run_id, produced_at   when/which replay produced it (processing time, NOT event time)
"""
from __future__ import annotations

import datetime as dt
import json
from typing import Any, Iterable, Mapping

RECORD_KINDS = ("historical_event_replay", "injected_test_event", "replay_flush_marker")
REQUIRED = ("event_id", "record_kind", "station_id", "event_time", "traffic_volume")
TIME_FMT = "%Y-%m-%dT%H:%M:%SZ"
HOUR_MS = 3_600_000


def to_ms(iso: str) -> int:
    t = dt.datetime.strptime(iso, TIME_FMT).replace(tzinfo=dt.timezone.utc)
    return int(t.timestamp() * 1000)


def to_iso(ms: int) -> str:
    return dt.datetime.fromtimestamp(ms / 1000, tz=dt.timezone.utc).strftime(TIME_FMT)


def event_id(station_id: str, event_time: str) -> str:
    return f"{station_id}|{event_time}"


def make_event(station_id: str, event_time: str, traffic_volume: int, *, record_kind: str,
               replay_run_id: str, road: str = "I-94", direction: str = "westbound",
               source: str = "", produced_at: str | None = None) -> dict[str, Any]:
    if record_kind not in RECORD_KINDS:
        raise ValueError(f"unknown record_kind {record_kind!r}")
    to_ms(event_time)   # validates the format
    return {"event_id": event_id(station_id, event_time), "record_kind": record_kind,
            "station_id": station_id, "road": road, "direction": direction,
            "event_time": event_time, "traffic_volume": int(traffic_volume),
            "unit": "vehicles/hour", "source": source, "replay_run_id": replay_run_id,
            "produced_at": produced_at or dt.datetime.now(dt.timezone.utc).strftime(TIME_FMT)}


def parse(payload: str | bytes | None) -> tuple[dict[str, Any] | None, str | None]:
    """Return (event with 'event_ts_ms', None) or (None, reason) for a malformed message."""
    if payload is None:
        return None, "empty"
    if isinstance(payload, bytes):
        try:
            payload = payload.decode("utf-8")
        except UnicodeDecodeError:
            return None, "not_utf8"
    try:
        e = json.loads(payload)
    except (ValueError, TypeError):
        return None, "not_json"
    if not isinstance(e, dict):
        return None, "not_an_object"
    missing = [k for k in REQUIRED if e.get(k) in (None, "")]
    if missing:
        return None, "missing_" + "_".join(missing)
    if e["record_kind"] not in RECORD_KINDS:
        return None, "unknown_record_kind"
    try:
        ts = to_ms(str(e["event_time"]))
    except ValueError:
        return None, "bad_event_time"
    v = e["traffic_volume"]
    if isinstance(v, bool) or not isinstance(v, int):
        return None, "traffic_volume_not_integer"
    if v < 0:
        return None, "traffic_volume_negative"
    if e["event_id"] != event_id(str(e["station_id"]), str(e["event_time"])):
        return None, "event_id_mismatch"
    return {**e, "event_ts_ms": ts}, None


def window_start(ts_ms: int, size_hours: int) -> int:
    size = size_hours * HOUR_MS
    return ts_ms - (ts_ms % size)


def window_metrics(volumes: list[int], start_ms: int, size_hours: int, expected: int) -> dict[str, Any]:
    """Metrics of one window from its event volumes (same code in Flink and in the oracle)."""
    n = len(volumes)
    return {"window_start_utc": to_iso(start_ms), "window_end_utc": to_iso(start_ms + size_hours * HOUR_MS),
            "window_size_hours": size_hours, "event_count": n, "expected_events": expected,
            "completeness": round(n / expected, 4) if expected else None,
            "volume_sum": int(sum(volumes)), "volume_avg": round(sum(volumes) / n, 2) if n else None,
            "volume_min": min(volumes) if n else None, "volume_max": max(volumes) if n else None}


# ---------- verification ----------
def oracle(events: Iterable[Mapping[str, Any]], size_hours: int, expected: int,
           station_id: str | None = None) -> dict[tuple[str, str], dict[str, Any]]:
    """Expected final window results from the produced events, without Flink.

    Valid only when no event is later than the allowed lateness (true for the in-order replay).
    Duplicates (same event_id) count once; flush markers are ignored.
    """
    seen: set[str] = set()
    groups: dict[tuple[str, int], list[int]] = {}
    for e in events:
        if e["record_kind"] == "replay_flush_marker" or e["event_id"] in seen:
            continue
        if station_id and e["station_id"] != station_id:
            continue
        seen.add(e["event_id"])
        w = window_start(to_ms(e["event_time"]), size_hours)
        groups.setdefault((e["station_id"], w), []).append(int(e["traffic_volume"]))
    return {(s, to_iso(w)): window_metrics(v, w, size_hours, expected) for (s, w), v in groups.items()}


def final_table(results: Iterable[Mapping[str, Any]]) -> tuple[dict[tuple[str, str], dict[str, Any]], dict[str, int]]:
    """Reduce emitted window results (with updates and possible re-emissions) to one row per window.

    Keeps the highest update_seq per (station_id, window_start_utc) - the upsert a consumer must do.
    Also returns counts: emitted, on_time, late_update, exact re-emissions (at-least-once duplicates).
    """
    best: dict[tuple[str, str], dict[str, Any]] = {}
    seen: set[tuple[str, str, int]] = set()
    stats = {"emitted": 0, "on_time": 0, "late_update": 0, "re_emitted": 0}
    for r in results:
        stats["emitted"] += 1
        stats[r["firing"]] = stats.get(r["firing"], 0) + 1
        k = (r["station_id"], r["window_start_utc"])
        ident = (*k, int(r["update_seq"]))
        if ident in seen:
            stats["re_emitted"] += 1
        seen.add(ident)
        if k not in best or int(r["update_seq"]) >= int(best[k]["update_seq"]):
            best[k] = dict(r)
    return best, stats


COMPARED = ("event_count", "volume_sum", "volume_min", "volume_max", "volume_avg", "completeness")


def compare(final: Mapping[tuple[str, str], Mapping[str, Any]],
            expected: Mapping[tuple[str, str], Mapping[str, Any]]) -> dict[str, Any]:
    missing = sorted(set(expected) - set(final))
    extra = sorted(set(final) - set(expected))
    wrong = [{"window": k, "field": f, "got": final[k].get(f), "expected": expected[k][f]}
             for k in sorted(set(final) & set(expected)) for f in COMPARED
             if final[k].get(f) != expected[k][f]]
    return {"windows_expected": len(expected), "windows_got": len(final), "missing": missing[:20],
            "extra": extra[:20], "wrong": wrong[:20], "n_missing": len(missing), "n_extra": len(extra),
            "n_wrong": len(wrong), "match": not missing and not extra and not wrong}
