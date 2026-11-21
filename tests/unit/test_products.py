"""Unit tests (no Spark): data-product contracts, release block, final window rows."""
import copy

import pytest

from src.products import contracts
from src.products.release import release_block
from src.products.windows import window_rows

SPEC_ITEMS = {   # spec §7 / T07 item -> contract key that carries it
    "name/version": "version", "schema/fields/types": "fields", "source and licence": "sources",
    "temporal coverage": "temporal_coverage", "spatial coverage": "spatial_coverage",
    "granularity": "granularity", "keys/uniqueness": "primary_key", "time semantics": "time_semantics",
    "quality rules": "quality_rules", "location/access": "location", "production method": "production",
    "reading instructions": "reading_instructions", "known limitations": "known_limitations",
}


@pytest.mark.parametrize("name", contracts.PRODUCTS)
def test_contract_is_valid_and_covers_spec(name):
    c = contracts.load(name)
    assert contracts.validate(c) == []
    for item, key in SPEC_ITEMS.items():
        assert c.get(key), f"{name}: no {item}"


def test_measured_quantities_have_units():
    for name in ("traffic_hourly", "traffic_features", "traffic_window_metrics"):
        c = contracts.load(name)
        for f in c["fields"]:
            if "volume" in f["name"] and f["type"] in ("int", "bigint", "double") or f["name"].startswith("lag_"):
                assert f.get("unit"), f"{name}.{f['name']} has no unit"


def test_four_products_with_required_names():
    assert contracts.PRODUCTS == ("traffic_hourly", "traffic_features", "road_image_manifest",
                                  "traffic_window_metrics")


def test_t07_specific_contents():
    h, f = contracts.load("traffic_hourly"), contracts.load("traffic_features")
    m, w = contracts.load("road_image_manifest"), contracts.load("traffic_window_metrics")
    names = lambda c: set(contracts.field_names(c))
    assert {"hour_start_utc", "direction", "traffic_volume", "traffic_volume_unit", "timezone"} <= names(h)
    assert "target" in f and "temporal_split" in f and {"lag_1h", "lag_168h"} <= names(f)
    assert {"image_id", "image_path", "image_bytes", "image_format", "label_path", "split", "file_available"} <= names(m)
    assert {"window_start_utc", "window_end_utc", "window_size_hours", "final_update_seq"} <= names(w)
    assert "late_event_handling" in w


def test_validate_catches_problems():
    c = copy.deepcopy(contracts.load("traffic_hourly"))
    c.pop("licence")
    c["fields"][0]["type"] = "varchar"
    c["fields"][5]["nullable"] = True          # hour_start_utc is part of the key
    c["primary_key"].append("nope")
    errs = contracts.validate(c)
    assert any("licence" in e for e in errs) and any("varchar" in e for e in errs)
    assert any("must not be nullable" in e for e in errs) and any("nope" in e for e in errs)


def _built():
    return {"product": "x_v1", "run_id": "m4_test", "built_at": "t", "location": "hdfs://x", "row_count": 3,
            "size_bytes": 10, "files": 1, "inputs": ["a"],
            "quality_summary": {"null_counts": {}, "temporal_coverage": {"first": "a"}}}


def test_release_block_refuses_failed_readback():
    with pytest.raises(ValueError):
        release_block(_built(), {"passed": False, "checks": []}, "evidence/x")


def test_release_block_takes_numbers_from_reports():
    rb = {"passed": True, "run_id": "m4_test", "checked_at": "t", "checks": [{"passed": True}, {"passed": True}]}
    b = release_block(_built(), rb, "evidence/x")
    assert b["row_count"] == 3 and b["temporal_coverage"] == {"first": "a"}
    assert b["readback"]["checks_passed"] == b["readback"]["checks_total"] == 2


def test_write_release_keeps_hand_written_part(tmp_path, monkeypatch):
    src = contracts.path("traffic_hourly")
    (tmp_path / src.name).write_text(src.read_text().split("\n# ---- generated release block")[0])
    monkeypatch.setattr(contracts, "CONTRACTS", tmp_path)
    rb = {"passed": True, "run_id": "r", "checked_at": "t", "checks": [{"passed": True}]}
    block = {**release_block(_built(), rb, "e"), "run_id": "r"}
    for _ in range(2):                                     # re-release replaces, does not append
        contracts.write_release("traffic_hourly", block)
    text = (tmp_path / src.name).read_text()
    assert text.count("generated release block") == 1
    c = contracts.load("traffic_hourly")
    assert c["release"]["row_count"] == 3 and c["fields"]


def test_window_rows_keep_last_update_and_count_firings():
    base = {"station_id": "301", "window_start_utc": "2016-07-01T06:00:00Z", "window_end_utc": "2016-07-01T12:00:00Z",
            "window_size_hours": 6, "expected_events": 6, "volume_min": 1, "volume_max": 9, "watermark_at_emit": "w",
            "record_kind": "historical_event_replay_window", "source_topic": "t", "job_run_id": "j"}
    res = [{**base, "firing": "on_time", "update_seq": 0, "event_count": 5, "completeness": 0.8333,
            "volume_sum": 50, "volume_avg": 10.0},
           {**base, "firing": "late_update", "update_seq": 1, "event_count": 6, "completeness": 1.0,
            "volume_sum": 60, "volume_avg": 10.0},
           {**base, "firing": "on_time", "update_seq": 0, "event_count": 5, "completeness": 0.8333,
            "volume_sum": 50, "volume_avg": 10.0}]                 # re-emitted
    rows = window_rows(res, "r")
    assert len(rows) == 1
    r = rows[0]
    assert (r["event_count"], r["final_firing"], r["final_update_seq"], r["firings_seen"]) == (6, "late_update", 1, 3)
    assert r["window_year"] == 2016 and r["window_start_utc"].hour == 6


def test_manifest_csv_round_trips_values_with_commas(tmp_path):
    import csv
    from src.products.windows import write_manifest_csv
    cols = ["image_id", "file_available", "object_types", "origin", "image_path"]
    rows = [{"image_id": "000001", "file_available": True, "object_types": ["Car", "Van"],
             "origin": "KITTI 2D object detection benchmark, training split", "image_path": None}]
    f = tmp_path / "m.csv"
    write_manifest_csv(f, cols, rows)
    back = list(csv.DictReader(f.open(encoding="utf-8")))
    assert back == [{"image_id": "000001", "file_available": "true", "object_types": "Car;Van",
                     "origin": "KITTI 2D object detection benchmark, training split", "image_path": ""}]
