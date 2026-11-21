"""Unit tests (no Kafka/Flink): replay event format, parsing, windows, oracle and final-table logic."""
import json

import pytest

from src.streaming.events import (compare, event_id, final_table, make_event, oracle, parse, to_iso, to_ms,
                                  window_metrics, window_start)


def ev(hour, vol=100, station="301", kind="historical_event_replay"):
    return make_event(station, f"2016-07-01T{hour:02d}:00:00Z", vol, record_kind=kind, replay_run_id="r")


# ---------- format / parse ----------
def test_make_event_is_labelled_replay_with_deterministic_id():
    e = ev(5)
    assert e["record_kind"] == "historical_event_replay"
    assert e["event_id"] == event_id("301", "2016-07-01T05:00:00Z") == "301|2016-07-01T05:00:00Z"


def test_make_event_rejects_unknown_kind_and_bad_time():
    with pytest.raises(ValueError):
        make_event("301", "2016-07-01T05:00:00Z", 1, record_kind="live", replay_run_id="r")
    with pytest.raises(ValueError):
        make_event("301", "2016-07-01 05:00", 1, record_kind="historical_event_replay", replay_run_id="r")


def test_parse_valid_adds_event_time_ms():
    e, reason = parse(json.dumps(ev(5)))
    assert reason is None and e["event_ts_ms"] == to_ms("2016-07-01T05:00:00Z")


@pytest.mark.parametrize("payload, reason", [
    (None, "empty"),
    (b"\xff\xfe", "not_utf8"),
    ("not json at all", "not_json"),
    ("[1, 2]", "not_an_object"),
    (json.dumps({k: v for k, v in ev(5).items() if k != "traffic_volume"}), "missing_traffic_volume"),
    (json.dumps({**ev(5), "record_kind": "live_sensor"}), "unknown_record_kind"),
    (json.dumps({**ev(5), "event_time": "2016-07-01 25:00"}), "bad_event_time"),
    (json.dumps({**ev(5), "traffic_volume": "100"}), "traffic_volume_not_integer"),
    (json.dumps({**ev(5), "traffic_volume": True}), "traffic_volume_not_integer"),
    (json.dumps({**ev(5), "traffic_volume": -3}), "traffic_volume_negative"),
    (json.dumps({**ev(5), "event_id": "301|other"}), "event_id_mismatch"),
])
def test_parse_malformed_reasons(payload, reason):
    assert parse(payload) == (None, reason)


# ---------- windows ----------
def test_window_start_is_epoch_aligned_6h():
    assert to_iso(window_start(to_ms("2016-07-01T05:59:59Z"), 6)) == "2016-07-01T00:00:00Z"
    assert to_iso(window_start(to_ms("2016-07-01T06:00:00Z"), 6)) == "2016-07-01T06:00:00Z"
    assert to_iso(window_start(to_ms("2016-07-01T23:00:00Z"), 6)) == "2016-07-01T18:00:00Z"


def test_window_metrics():
    m = window_metrics([100, 200, 300], to_ms("2016-07-01T06:00:00Z"), 6, 6)
    assert m["window_end_utc"] == "2016-07-01T12:00:00Z"
    assert (m["event_count"], m["volume_sum"], m["volume_avg"], m["completeness"]) == (3, 600, 200.0, 0.5)
    assert (m["volume_min"], m["volume_max"]) == (100, 300)


# ---------- oracle / final table / compare ----------
def test_oracle_counts_duplicates_once_and_ignores_flush_and_other_stations():
    events = [ev(0, 10), ev(1, 20), ev(1, 20), ev(7, 5),
              make_event("FLUSH", "2016-08-01T00:00:00Z", 0, record_kind="replay_flush_marker", replay_run_id="r"),
              ev(2, 99, station="TEST-301")]
    o = oracle(events, 6, 6, station_id="301")
    assert set(o) == {("301", "2016-07-01T00:00:00Z"), ("301", "2016-07-01T06:00:00Z")}
    assert o[("301", "2016-07-01T00:00:00Z")]["volume_sum"] == 30
    assert o[("301", "2016-07-01T00:00:00Z")]["event_count"] == 2


def _res(seq, firing, count, total, start="2016-07-01T06:00:00Z"):
    return {"station_id": "301", "window_start_utc": start, "update_seq": seq, "firing": firing,
            "event_count": count, "volume_sum": total}


def test_final_table_keeps_latest_update_and_counts_re_emissions():
    rows = [_res(0, "on_time", 5, 500), _res(1, "late_update", 6, 600),
            _res(0, "on_time", 5, 500)]                    # re-emitted after a restart (at-least-once)
    final, stats = final_table(rows)
    assert final[("301", "2016-07-01T06:00:00Z")]["volume_sum"] == 600
    assert stats == {"emitted": 3, "on_time": 2, "late_update": 1, "re_emitted": 1}


def test_compare_detects_missing_extra_and_wrong():
    exp = oracle([ev(0, 10), ev(7, 5)], 6, 6, "301")
    got = {k: dict(v) for k, v in exp.items()}
    assert compare(got, exp)["match"] is True
    got[("301", "2016-07-01T00:00:00Z")]["volume_sum"] = 11
    got[("301", "2016-07-02T00:00:00Z")] = dict(got[("301", "2016-07-01T06:00:00Z")])
    del got[("301", "2016-07-01T06:00:00Z")]
    c = compare(got, exp)
    assert c["match"] is False and (c["n_missing"], c["n_extra"], c["n_wrong"]) == (1, 1, 1)


# ---------- edge-case scenario checker ----------
def _perfect():
    from src.streaming.scenario import STATION, expected
    exp = expected()
    rows = []
    for w, (n, total) in exp["final"].items():
        if w.endswith("06:00:00Z"):
            n0, t0 = exp["w1_on_time"]
            rows.append({"station_id": STATION, "window_start_utc": w, "firing": "on_time", "update_seq": 0,
                         "event_count": n0, "volume_sum": t0, "watermark_at_emit": "x"})
            rows.append({"station_id": STATION, "window_start_utc": w, "firing": "late_update", "update_seq": 1,
                         "event_count": n, "volume_sum": total, "watermark_at_emit": "x"})
        else:
            rows.append({"station_id": STATION, "window_start_utc": w, "firing": "on_time", "update_seq": 0,
                         "event_count": n, "volume_sum": total, "watermark_at_emit": "x"})
    dlq = [{"reason": r} for r, k in exp["dlq"].items() for _ in range(k)]
    return rows, dlq


def test_scenario_check_passes_on_expected_output():
    from src.streaming.scenario import check
    rows, dlq = _perfect()
    assert check(rows, dlq)["passed"] is True


def test_scenario_check_fails_without_late_update_or_with_wrong_dlq():
    from src.streaming.scenario import check
    rows, dlq = _perfect()
    no_late = [r for r in rows if r["firing"] != "late_update"]
    assert check(no_late, dlq)["passed"] is False
    assert check(rows, dlq[1:])["passed"] is False


def test_scenario_phases_keep_injected_data_separate():
    from src.streaming.scenario import STATION, phases
    for _, values, _ in phases("r"):
        for v in values:
            try:
                e = json.loads(v)
            except ValueError:
                continue                       # the deliberately non-JSON message
            assert e.get("station_id") in (STATION, "FLUSH")
            assert e.get("record_kind") != "historical_event_replay"
