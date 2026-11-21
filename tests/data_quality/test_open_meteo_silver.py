"""Data-quality tests: Open-Meteo Silver (spec T05 categories) and the UCI <-> ERA5 Gold join (spec T03).

Synthetic rows built here only; never written to the data lake (spec §8 integrity rule).
"""
import datetime as dt
import math
from pathlib import Path

import pytest
import yaml

from src.batch.gold_traffic_weather import build_gold
from src.batch.silver_open_meteo import build_silver
from src.batch.silver_uci import build_silver as build_uci

ROOT = Path(__file__).resolve().parents[2]
Q = yaml.safe_load((ROOT / "configs" / "quality.yaml").read_text())
CFG = Q["open_meteo"]
START, END = "2012-10-01 00:00:00", "2018-10-01 23:00:00"
SCHEMA = ("_source_file string, grid_latitude double, grid_longitude double, elevation double, timezone string,"
          " time string, temperature_2m double, precipitation double, rain double, snowfall double,"
          " cloud_cover bigint, weather_code bigint, wind_speed_10m double, _sha256 string,"
          " _source_version string, _ingested_at timestamp, _source string, ingest_date date")


def om(time="2016-07-01T17:00", temp=22.0, precip=0.0, rain=0.0, snow=0.0, cloud=40, code=1, wind=10.0,
       lat=45.0, lon=-93.25):
    return ("open_meteo_2016.json", lat, lon, 267.0, "GMT", time, temp, precip, rain, snow, cloud, code, wind,
            "0" * 64, "test", dt.datetime(2026, 11, 21), "open_meteo", dt.date(2026, 11, 21))


def run(spark, rows):
    bronze = spark.createDataFrame(rows, SCHEMA)
    _, silver = build_silver(bronze, CFG, START, END, "test_run")
    return bronze, silver.collect()


def rules_of(r):
    return {v["rule"] for v in r["_violations"]}


# ---------- valid ----------
def test_valid_row_accepted_with_labels_and_units(spark):
    _, recs = run(spark, [om(precip=1.2, rain=1.2)])
    r = recs[0]
    assert r["outcome"] == "accepted" and r["_violations"] == []
    assert (r["model"], r["data_kind"]) == ("era5", "reanalysis")
    assert r["time_utc"] == dt.datetime(2016, 7, 1, 17)
    assert r["sum_interval_start_utc"] == dt.datetime(2016, 7, 1, 16)      # preceding-hour sum
    assert r["temperature_2m_c"] == 22.0 and r["precipitation_mm"] == 1.2


# ---------- missing ----------
def test_missing_variable_quarantined(spark):
    _, recs = run(spark, [om(temp=None)])
    assert recs[0]["outcome"] == "quarantine" and "missing_variable" in rules_of(recs[0])


def test_missing_key_rejected(spark):
    _, recs = run(spark, [om(time=""), om(lat=None)])
    assert {r["outcome"] for r in recs} == {"rejected"} and len(recs) == 2
    assert all("key_missing" in rules_of(r) for r in recs)


# ---------- duplicate ----------
def test_identical_duplicates_collapse_and_are_flagged(spark):
    _, recs = run(spark, [om(), om()])
    assert len(recs) == 1 and recs[0]["source_rows"] == 2
    assert recs[0]["outcome"] == "accepted" and "unique_key_duplicate" in rules_of(recs[0])


def test_conflicting_duplicates_quarantined_with_null_values(spark):
    _, recs = run(spark, [om(temp=20.0), om(temp=21.0)])
    r = recs[0]
    assert r["outcome"] == "quarantine" and "unique_key_conflicting" in rules_of(r)
    assert r["temperature_2m_c"] is None and r["source_rows"] == 2


# ---------- invalid numeric ----------
@pytest.mark.parametrize("kwargs, rule", [
    ({"temp": 80.0}, "range_temperature_2m"),
    ({"precip": -1.0}, "range_precipitation"),
    ({"snow": 150.0}, "range_snowfall"),
    ({"cloud": 101}, "range_cloud_cover"),
    ({"wind": 300.0}, "range_wind_speed_10m"),
    ({"code": 42}, "range_weather_code_unknown"),
    ({"precip": 1.0, "rain": 2.0}, "range_rain_above_precipitation"),
    ({"temp": math.nan}, "type_non_finite"),
])
def test_invalid_numeric_quarantined(spark, kwargs, rule):
    _, recs = run(spark, [om(**kwargs)])
    assert recs[0]["outcome"] == "quarantine" and rule in rules_of(recs[0])


# ---------- abnormal timestamp ----------
@pytest.mark.parametrize("time, rule, outcome", [
    ("2016-07-01 17:00", "ts_unparseable", "rejected"),        # wrong format (no 'T')
    ("2016-07-01T17:30", "ts_unparseable", "rejected"),        # not on the hour
    ("2019-01-01T00:00", "ts_outside_request", "quarantine"),
    ("2012-09-30T23:00", "ts_outside_request", "quarantine"),
])
def test_abnormal_timestamp(spark, time, rule, outcome):
    _, recs = run(spark, [om(time=time)])
    assert recs[0]["outcome"] == outcome and rule in rules_of(recs[0])


# ---------- reconciliation ----------
def test_open_meteo_reconciliation(spark):
    rows = [om(), om(), om(time="2016-07-01T18:00", temp=None), om(time="bad"), om(time="2016-07-01T19:00")]
    bronze, recs = run(spark, rows)
    assert sum(r["source_rows"] for r in recs) == bronze.count() == 5


# ---------- Gold join: UCI hour [U, U+1h) <-> ERA5 ----------
UCI_COLS = ["holiday", "temp", "rain_1h", "snow_1h", "clouds_all", "weather_main", "weather_description",
            "date_time", "traffic_volume", "_corrupt_record"]
UCI_SCHEMA = ", ".join(f"{c} string" for c in UCI_COLS) + (
    ", _source string, _source_file string, _source_version string, _sha256 string,"
    " _ingested_at timestamp, ingest_date date")
STATION = {"id": "301", "road": "I-94", "direction": "westbound", "latitude": 44.95, "longitude": -93.14}


def uci_row(date_time):
    return ("None", "295.0", "0.0", "0.0", "40", "Clouds", "scattered clouds", date_time, "5000", None,
            "uci_metro", "Metro_Interstate_Traffic_Volume.csv.gz", "test", "0" * 64,
            dt.datetime(2026, 11, 21), dt.date(2026, 11, 21))


def gold_for(spark, uci_times, om_rows):
    _, uci = build_uci(spark.createDataFrame([uci_row(t) for t in uci_times], UCI_SCHEMA),
                       Q["uci_metro"], STATION, "t")
    _, silver_om = build_silver(spark.createDataFrame(om_rows, SCHEMA), CFG, START, END, "t")
    g = build_gold(uci.where("outcome = 'accepted'"), silver_om.where("outcome = 'accepted'"),
                   Q["traffic_weather"], CFG, "t")
    return g.collect()


def test_join_takes_instant_at_hour_start_and_sum_from_next_hour(spark):
    # UCI 2016-07-01 12:00 local (CDT) = 17:00 UTC, covering [17:00, 18:00) UTC
    recs = gold_for(spark, ["2016-07-01 12:00:00"],
                    [om(time="2016-07-01T17:00", temp=22.0, precip=0.0, rain=0.0),
                     om(time="2016-07-01T18:00", temp=24.0, precip=3.0, rain=3.0)])
    assert len(recs) == 1
    r = recs[0]
    assert r["era5_temperature_2m_c"] == 22.0 and r["era5_instant_time_utc"] == dt.datetime(2016, 7, 1, 17)
    assert r["era5_precipitation_mm"] == 3.0 and r["era5_sum_end_time_utc"] == dt.datetime(2016, 7, 1, 18)
    assert (r["era5_model"], r["era5_data_kind"]) == ("era5", "reanalysis")
    assert r["traffic_volume"] == 5000 and r["temp_k"] == 295.0            # UCI weather kept unchanged


def test_join_keeps_unmatched_uci_hours_with_null_weather(spark):
    recs = gold_for(spark, ["2016-07-01 12:00:00", "2016-07-01 13:00:00"],
                    [om(time="2016-07-01T17:00"), om(time="2016-07-01T18:00")])
    by_hour = {r["hour_start_local"].hour: r for r in recs}
    assert len(recs) == 2                                                  # left join, no fan-out
    assert by_hour[13]["era5_temperature_2m_c"] is not None                 # 18:00 UTC instant exists
    assert by_hour[13]["era5_precipitation_mm"] is None                     # 19:00 UTC sum missing
