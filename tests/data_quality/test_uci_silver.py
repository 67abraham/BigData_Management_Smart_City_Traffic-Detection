"""Data-quality tests for the UCI Silver transform (spec T05: valid, missing, duplicate, invalid
numeric and abnormal timestamp cases, plus malformed lines, holidays and reconciliation).

Inputs are small synthetic Bronze rows built here. They exist only in the test and are never
written to the data lake (spec §8 integrity rule: injected anomalies stay separate from real data).
"""
import datetime as dt
from pathlib import Path

import pytest
import yaml

from src.batch.silver_uci import build_silver

CFG = yaml.safe_load((Path(__file__).resolve().parents[2] / "configs" / "quality.yaml").read_text())["uci_metro"]
STATION = {"id": "301", "road": "I-94", "direction": "westbound", "latitude": 44.95, "longitude": -93.14}
COLS = ["holiday", "temp", "rain_1h", "snow_1h", "clouds_all", "weather_main", "weather_description",
        "date_time", "traffic_volume", "_corrupt_record"]
SCHEMA = ", ".join(f"{c} string" for c in COLS) + (
    ", _source string, _source_file string, _source_version string, _sha256 string,"
    " _ingested_at timestamp, ingest_date date")


def row(date_time="2016-07-01 12:00:00", volume="5000", temp="295.0", rain="0.0", snow="0.0", clouds="40",
        main="Clouds", desc="scattered clouds", holiday="None", corrupt=None):
    return (holiday, temp, rain, snow, clouds, main, desc, date_time, volume, corrupt,
            "uci_metro", "Metro_Interstate_Traffic_Volume.csv.gz", "test", "0" * 64,
            dt.datetime(2026, 11, 21), dt.date(2026, 11, 21))


def run(spark, rows):
    bronze = spark.createDataFrame(rows, SCHEMA)
    typed, silver = build_silver(bronze, CFG, STATION, "test_run")
    return bronze, typed, silver.collect()


def rules_of(rec):
    return {v["rule"] for v in rec["_violations"]}


def one(recs, **match):
    hits = [r for r in recs if all(r[k] == v for k, v in match.items())]
    assert len(hits) == 1, f"expected 1 record matching {match}, got {len(hits)}"
    return hits[0]


# ---------- valid ----------
def test_valid_summer_row_is_accepted_with_cdt_offset(spark):
    _, _, recs = run(spark, [row()])
    r = recs[0]
    assert r["outcome"] == "accepted" and r["_violations"] == []
    assert r["traffic_volume"] == 5000 and r["traffic_volume_unit"] == "vehicles/hour"
    assert r["hour_start_utc"] == dt.datetime(2016, 7, 1, 17, 0)      # CDT = UTC-5
    assert r["utc_offset_hours"] == -5
    assert r["temp_c"] == pytest.approx(21.85)
    assert (r["station_id"], r["road"], r["direction"]) == ("301", "I-94", "westbound")


def test_valid_winter_row_has_cst_offset(spark):
    _, _, recs = run(spark, [row(date_time="2016-01-15 08:00:00")])
    assert recs[0]["hour_start_utc"] == dt.datetime(2016, 1, 15, 14, 0)   # CST = UTC-6
    assert recs[0]["utc_offset_hours"] == -6


# ---------- missing ----------
@pytest.mark.parametrize("kwargs, rule, outcome", [
    ({"volume": ""}, "missing_traffic_volume", "rejected"),
    ({"volume": None}, "missing_traffic_volume", "rejected"),
    ({"date_time": ""}, "key_date_time_missing", "rejected"),
    ({"main": ""}, "missing_weather_field", "quarantine"),
    ({"temp": None}, "missing_weather_field", "quarantine"),
])
def test_missing_values(spark, kwargs, rule, outcome):
    _, _, recs = run(spark, [row(**kwargs)])
    assert recs[0]["outcome"] == outcome
    assert rule in rules_of(recs[0])


# ---------- duplicate ----------
def test_duplicate_hour_same_volume_collapses_and_keeps_weather_set(spark):
    _, _, recs = run(spark, [row(main="Rain", desc="light rain"), row(main="Mist", desc="mist")])
    assert len(recs) == 1
    r = recs[0]
    assert r["outcome"] == "accepted" and r["source_rows"] == 2
    assert r["weather_main"] == ["Mist", "Rain"] and r["traffic_volume"] == 5000
    assert len(r["_raw_rows"]) == 2                                  # lineage to both source rows


def test_exact_duplicate_rows_collapse(spark):
    _, _, recs = run(spark, [row(), row(), row()])
    assert len(recs) == 1 and recs[0]["source_rows"] == 3 and recs[0]["outcome"] == "accepted"


def test_duplicate_hour_conflicting_volume_is_quarantined(spark):
    _, _, recs = run(spark, [row(volume="5000"), row(volume="4000")])
    r = recs[0]
    assert r["outcome"] == "quarantine" and r["traffic_volume"] is None
    assert "unique_hour_conflicting_volume" in rules_of(r)


def test_duplicate_hour_conflicting_temp_is_flagged_and_nulled(spark):
    _, _, recs = run(spark, [row(temp="290.0"), row(temp="291.0")])
    r = recs[0]
    assert r["outcome"] == "accepted" and r["temp_k"] is None and r["rain_1h_mm"] == 0.0
    assert "unique_hour_conflicting_weather" in rules_of(r)


# ---------- invalid numeric ----------
@pytest.mark.parametrize("kwargs, rule, outcome", [
    ({"volume": "abc"}, "type_traffic_volume", "rejected"),
    ({"volume": "12.5"}, "type_traffic_volume", "rejected"),
    ({"volume": "-5"}, "range_volume_negative", "rejected"),
    ({"volume": "0"}, "range_volume_zero", "quarantine"),
    ({"volume": "20000"}, "range_volume_high", "quarantine"),
    ({"temp": "0.0"}, "range_temp_k", "quarantine"),                 # seen in M1: 0 K
    ({"rain": "9831.3"}, "range_rain_1h", "quarantine"),             # seen in M1
    ({"clouds": "101"}, "range_clouds_all", "quarantine"),
    ({"snow": "x"}, "type_weather_numeric", "quarantine"),
])
def test_invalid_numeric(spark, kwargs, rule, outcome):
    _, _, recs = run(spark, [row(**kwargs)])
    assert recs[0]["outcome"] == outcome
    assert rule in rules_of(recs[0])


# ---------- abnormal timestamp ----------
@pytest.mark.parametrize("date_time, rule, outcome", [
    ("2016-13-01 00:00:00", "ts_unparseable", "rejected"),
    ("01/07/2016 12:00", "ts_unparseable", "rejected"),
    ("2016-07-01 12:30:00", "ts_unparseable", "rejected"),          # not on the hour
    ("2016-03-13 02:00:00", "ts_dst_nonexistent", "rejected"),      # spring-forward gap
    ("2016-11-06 01:00:00", "ts_dst_ambiguous", "quarantine"),      # fall-back hour
    ("2030-01-01 00:00:00", "ts_outside_coverage", "quarantine"),
])
def test_abnormal_timestamp(spark, date_time, rule, outcome):
    _, _, recs = run(spark, [row(date_time=date_time)])
    assert recs[0]["outcome"] == outcome
    assert rule in rules_of(recs[0])


def test_ambiguous_hour_has_no_utc_time(spark):
    _, _, recs = run(spark, [row(date_time="2016-11-06 01:00:00")])
    assert recs[0]["hour_start_utc"] is None and recs[0]["dst_ambiguous"] is True


# ---------- malformed line ----------
def test_malformed_line_is_rejected(spark):
    _, _, recs = run(spark, [row(corrupt="garbage,line")])
    assert recs[0]["outcome"] == "rejected" and "type_malformed_line" in rules_of(recs[0])


# ---------- holiday spreading ----------
def test_holiday_on_midnight_row_applies_to_whole_local_date(spark):
    _, _, recs = run(spark, [row(date_time="2016-07-04 00:00:00", holiday="Independence Day"),
                             row(date_time="2016-07-04 15:00:00"),
                             row(date_time="2016-07-05 15:00:00")])
    assert one(recs, local_date=dt.date(2016, 7, 4), traffic_volume=5000,
               hour_start_local=dt.datetime(2016, 7, 4, 15))["holiday_name"] == "Independence Day"
    assert one(recs, local_date=dt.date(2016, 7, 5))["is_holiday_date"] is False


# ---------- reconciliation ----------
def test_every_source_row_is_accounted_for(spark):
    rows = [row(), row(main="Mist"), row(volume="abc"), row(date_time="bad"),
            row(date_time="2016-07-01 13:00:00", temp="0.0"), row(date_time="2016-07-01 14:00:00")]
    bronze, _, recs = run(spark, rows)
    assert sum(r["source_rows"] for r in recs) == bronze.count() == 6
    assert {r["outcome"] for r in recs} == {"accepted", "quarantine", "rejected"}
    # the bad-volume row shares its hour with two good rows: it is rejected alone,
    # the good rows still form the accepted hour
    assert one(recs, outcome="accepted", hour_start_local=dt.datetime(2016, 7, 1, 12))["source_rows"] == 2
