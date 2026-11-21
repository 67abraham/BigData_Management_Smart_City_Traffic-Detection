"""Edge-case scenario for the Flink job: late, duplicate, malformed and out-of-order events (spec T04).

All events use the synthetic station TEST-301 and record_kind injected_test_event, and go to the
test-* topics only, so injected anomalies never mix with the real replay (spec §8).

Settings it is designed for (configs/streaming.yaml): 6 h tumbling windows, watermark = max event
time - 60 min, allowed lateness 6 h. Volume of the event at hour h = 100 + h. Phases are separated by
6 s pauses so the watermark reaches every operator in between (periodic watermarks every 200 ms;
Python bundle size 1, see configs/streaming.yaml python_runtime).

Window W0 [00,06)  sent 00..04; 03 re-sent (duplicate, after W0 fired); 05 sent only in phase C when
                   the watermark (~13:59) is past W0's end + lateness (11:59:59.999) -> too_late
Window W1 [06,12)  sent 06, 07, 09, then 08 (out of order but within 60 min -> counted on time),
                   09 again (duplicate), 11; 10 sent in phase C while W1 is fired but within
                   lateness -> one late_update (update_seq 1)
Window W2 [12,18)  12..15 in phase B, 16..17 in phase D;  W3 [18,24) 18..23 in phase D
Flush              one replay_flush_marker 10 days later, so the watermark passes every window
Malformed          5 messages in phase A, each with a different reason

Run (spark-master container): python3 -m src.streaming.scenario --run-id R --events-out F
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path
from typing import Any

STATION = "TEST-301"
DAY = "2016-07-01"


def _ev(hour: int, run_id: str, day: str = DAY) -> dict[str, Any]:
    from src.streaming.events import make_event
    return make_event(STATION, f"{day}T{hour:02d}:00:00Z", 100 + hour, record_kind="injected_test_event",
                      replay_run_id=run_id, road="TEST", direction="test", source="src/streaming/scenario.py")


def phases(run_id: str) -> list[tuple[str, list[str], float]]:
    """[(phase name, raw Kafka values, pause after the phase in seconds)]"""
    e = lambda h: json.dumps(_ev(h, run_id))
    good = _ev(1, run_id)
    malformed = [
        "this is not json",
        json.dumps({k: v for k, v in good.items() if k != "traffic_volume"}),            # missing field
        json.dumps({**good, "event_time": "2016-07-01 25:00"}),                          # bad timestamp
        json.dumps({**_ev(2, run_id), "traffic_volume": -40}),                            # negative
        json.dumps({**_ev(3, run_id), "record_kind": "live_sensor_reading"}),             # not replay
    ]
    return [
        ("A: W0, W1 with out-of-order 08 and duplicates, malformed",
         [e(0), e(1), e(2), e(3), e(4), e(6), e(7), e(9), e(8), e(9), *malformed, e(3), e(11)], 6.0),
        ("B: advance watermark to ~13:59 (W0, W1 fire)", [e(12), e(13), e(14), e(15)], 6.0),
        ("C: late 10 for W1 (within lateness), too-late 05 for W0", [e(10), e(5)], 6.0),
        ("D: finish W2 and W3", [e(16), e(17), *[e(h) for h in range(18, 24)]], 6.0),
        ("E: flush marker", [json.dumps(_flush(run_id))], 0.0),
    ]


def _flush(run_id: str) -> dict[str, Any]:
    from src.streaming.events import make_event
    return make_event("FLUSH", "2016-07-11T00:00:00Z", 0, record_kind="replay_flush_marker",
                      replay_run_id=run_id, source="src/streaming/scenario.py")


def expected() -> dict[str, Any]:
    s = lambda hours: sum(100 + h for h in hours)
    return {
        "final": {   # window start -> (event_count, volume_sum) after all updates
            f"{DAY}T00:00:00Z": (5, s(range(0, 5))),
            f"{DAY}T06:00:00Z": (6, s(range(6, 12))),
            f"{DAY}T12:00:00Z": (6, s(range(12, 18))),
            f"{DAY}T18:00:00Z": (6, s(range(18, 24))),
        },
        "w1_on_time": (5, s([6, 7, 8, 9, 11])),   # before the late 10
        "late_updates": {f"{DAY}T06:00:00Z": 1},  # exactly one, for W1
        "dlq": {"duplicate": 2, "too_late": 1, "not_json": 1, "missing_traffic_volume": 1,
                "bad_event_time": 1, "traffic_volume_negative": 1, "unknown_record_kind": 1},
    }


def check(results: list[dict], dlq: list[dict]) -> dict[str, Any]:
    """Compare collected window results and DLQ records with expected(). Pure."""
    exp = expected()
    mine = [r for r in results if r["station_id"] == STATION]
    by_window: dict[str, list[dict]] = {}
    for r in mine:
        by_window.setdefault(r["window_start_utc"], []).append(r)
    failures = []
    for w, (n, total) in exp["final"].items():
        rows = sorted(by_window.get(w, []), key=lambda r: r["update_seq"])
        if not rows:
            failures.append(f"{w}: no result")
            continue
        last = rows[-1]
        if (last["event_count"], last["volume_sum"]) != (n, total):
            failures.append(f"{w}: final {(last['event_count'], last['volume_sum'])} != {(n, total)}")
        late = len({r["update_seq"] for r in rows if r["firing"] == "late_update"})
        if late != exp["late_updates"].get(w, 0):
            failures.append(f"{w}: {late} late updates, expected {exp['late_updates'].get(w, 0)}")
        on_time = [r for r in rows if r["firing"] == "on_time"]
        if not on_time:
            failures.append(f"{w}: no on_time firing")
        elif w == f"{DAY}T06:00:00Z" and (on_time[0]["event_count"], on_time[0]["volume_sum"]) != exp["w1_on_time"]:
            failures.append(f"{w}: on_time {(on_time[0]['event_count'], on_time[0]['volume_sum'])} != {exp['w1_on_time']}")
    extra = sorted(set(by_window) - set(exp["final"]))
    if extra:
        failures.append(f"unexpected windows {extra}")
    reasons: dict[str, int] = {}
    for d in dlq:
        reasons[d["reason"]] = reasons.get(d["reason"], 0) + 1
    if reasons != exp["dlq"]:
        failures.append(f"dlq reasons {reasons} != {exp['dlq']}")
    return {"passed": not failures, "failures": failures, "dlq_reasons": reasons,
            "windows": {w: [{k: r[k] for k in ("firing", "update_seq", "event_count", "volume_sum",
                                               "watermark_at_emit")} for r in sorted(rows, key=lambda r: r["update_seq"])]
                        for w, rows in sorted(by_window.items())}}


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--run-id", required=True)
    ap.add_argument("--events-out", required=True)
    args = ap.parse_args()
    import yaml
    from kafka import KafkaProducer

    from src.common.config import PROJECT_ROOT, load_config
    cfg = yaml.safe_load((PROJECT_ROOT / "configs" / "streaming.yaml").read_text())
    topic = cfg["topics"]["test_replay"]["name"]
    p = KafkaProducer(bootstrap_servers=load_config()["kafka"]["bootstrap_servers"], acks="all",
                      value_serializer=lambda v: v.encode())
    with open(args.events_out, "w", encoding="utf-8") as fh:
        for name, values, pause in phases(args.run_id):
            for v in values:
                p.send(topic, value=v)
                fh.write(json.dumps({"phase": name, "value": v}) + "\n")
            p.flush()
            print(f"sent {len(values):2d}  {name}; pause {pause}s", flush=True)
            time.sleep(pause)
    p.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
