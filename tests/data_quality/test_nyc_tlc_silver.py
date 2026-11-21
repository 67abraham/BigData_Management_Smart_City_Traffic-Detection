"""Data-quality tests: NYC TLC Silver (spec T05 categories, schema unification, zone join) and the
Gold zone-hourly aggregation (spec T03). Synthetic rows only; never written to the data lake.
"""
import datetime as dt
from pathlib import Path

import pytest
import yaml

from src.batch.gold_nyc_zone_hourly import aggregate
from src.batch.silver_nyc_tlc import transform, unify_month

CFG = yaml.safe_load((Path(__file__).resolve().parents[2] / "configs" / "quality.yaml").read_text())["nyc_tlc"]
PROV = ", _source string, _source_file string, _source_version string, _sha256 string, _ingested_at timestamp"
# 2023-01 style schema (bigint IDs, double passenger_count/RatecodeID, airport_fee)
JAN = ("VendorID bigint, tpep_pickup_datetime timestamp_ntz, tpep_dropoff_datetime timestamp_ntz,"
       " passenger_count double, trip_distance double, RatecodeID double, store_and_fwd_flag string,"
       " PULocationID bigint, DOLocationID bigint, payment_type bigint, fare_amount double, extra double,"
       " mta_tax double, tip_amount double, tolls_amount double, improvement_surcharge double,"
       " total_amount double, congestion_surcharge double, airport_fee double" + PROV)
# 2023-02 style schema (int IDs, bigint passenger_count/RatecodeID, Airport_fee)
FEB = (JAN.replace("VendorID bigint", "VendorID int").replace("PULocationID bigint", "PULocationID int")
          .replace("DOLocationID bigint", "DOLocationID int").replace("passenger_count double", "passenger_count bigint")
          .replace("RatecodeID double", "RatecodeID bigint").replace("airport_fee double", "Airport_fee double"))
ZONES = [("1", "EWR", "Newark Airport", "EWR"), ("132", "Queens", "JFK Airport", "Airports"),
         ("161", "Manhattan", "Midtown Center", "Yellow Zone"), ("264", "Unknown", "N/A", "N/A"),
         ("265", "N/A", "Outside of NYC", "N/A")]


def trip(pickup=dt.datetime(2023, 1, 10, 8, 0), minutes=20, vendor=2, pc=1, dist=3.0, rc=1, flag="N", pu=161,
         do=132, pay=1, fare=20.0, total=28.0, cong=2.5, airport=0.0, feb=False):
    drop = pickup + dt.timedelta(minutes=minutes) if pickup is not None else None
    pc_v = (int(pc) if pc is not None else None) if feb else (float(pc) if pc is not None else None)
    rc_v = (int(rc) if rc is not None else None) if feb else (float(rc) if rc is not None else None)
    return (vendor, pickup, drop, pc_v, dist, rc_v, flag, pu, do, pay, fare, 1.0, 0.5, 3.0, 0.0, 1.0,
            total, cong, airport, "nyc_tlc", "f.parquet", "test", "0" * 64, dt.datetime(2026, 11, 21))


def run(spark, jan_rows, feb_rows=()):
    parts = []
    for rows, schema, month in ((jan_rows, JAN, "2023-01"), (feb_rows, FEB, "2023-02")):
        if rows:
            u, _ = unify_month(spark.createDataFrame(list(rows), schema), month, CFG)
            parts.append(u)
    trips = parts[0] if len(parts) == 1 else parts[0].unionByName(parts[1])
    zones = spark.createDataFrame(ZONES, "LocationID string, Borough string, Zone string, service_zone string")
    return transform(trips, zones, CFG, "t")


def rules_of(r):
    return {v["rule"] for v in r["_violations"]}


# ---------- valid + schema unification + zone join ----------
def test_valid_trips_from_both_schemas_are_unified_and_accepted(spark):
    df = run(spark, [trip()], [trip(pickup=dt.datetime(2023, 2, 3, 9), feb=True)])
    recs = df.collect()
    assert [r["outcome"] for r in recs] == ["accepted", "accepted"]
    assert dict(df.dtypes)["passenger_count"] == "int" and dict(df.dtypes)["vendor_id"] == "int"
    r = [x for x in recs if x["source_month"] == "2023-02"][0]
    assert r["airport_fee_usd"] == 0.0 and r["pu_borough"] == "Manhattan" and r["do_zone"] == "JFK Airport"
    assert r["trip_minutes"] == 20.0 and r["pickup_hour_local"] == 9


# ---------- missing ----------
def test_flex_fare_nulls_are_flagged_not_quarantined(spark):
    r = run(spark, [trip(pay=0, pc=None, rc=None, flag=None, cong=None, airport=None)]).collect()[0]
    assert r["outcome"] == "accepted" and "missing_fields_flex_fare" in rules_of(r) and r["is_flex_fare"]


def test_same_nulls_on_card_trip_are_quarantined(spark):
    r = run(spark, [trip(pay=1, pc=None, rc=None, flag=None, cong=None, airport=None)]).collect()[0]
    assert r["outcome"] == "quarantine" and "missing_fields_non_flex" in rules_of(r)


def test_missing_pickup_time_is_rejected(spark):
    r = run(spark, [trip(pickup=None)]).collect()[0]
    assert r["outcome"] == "rejected" and "key_missing" in rules_of(r)


# ---------- duplicate ----------
def test_duplicate_trips_are_all_quarantined(spark):
    recs = run(spark, [trip(), trip(), trip(pu=1)]).collect()
    dup = [r for r in recs if r["pu_location_id"] == 161]
    assert len(recs) == 3                                             # nothing dropped
    assert all(r["outcome"] == "quarantine" and "unique_trip_duplicate" in rules_of(r) for r in dup)
    assert [r for r in recs if r["pu_location_id"] == 1][0]["outcome"] == "accepted"


# ---------- invalid numeric / codes ----------
@pytest.mark.parametrize("kwargs, rule, outcome", [
    ({"dist": -1.0}, "range_distance_negative", "rejected"),
    ({"dist": 335004.33}, "range_distance_over_max", "quarantine"),   # max seen in the profile
    ({"fare": -20.0, "total": -28.0}, "range_amount_negative", "quarantine"),
    ({"total": 2208.1}, "range_total_over_max", "quarantine"),
    ({"vendor": 5}, "range_vendor_unknown", "quarantine"),
    ({"rc": 7}, "range_ratecode_unknown", "quarantine"),
    ({"pay": 9}, "range_payment_type_unknown", "quarantine"),
    ({"flag": "X"}, "range_store_fwd_unknown", "quarantine"),
    ({"pc": 1.5}, "type_non_integer_code", "quarantine"),
    ({"pu": 999}, "zone_not_in_lookup", "quarantine"),
    ({"pc": 0}, "flag_passenger_count_zero", "accepted"),
    ({"dist": 0.0}, "flag_distance_zero", "accepted"),
    ({"do": 265}, "zone_unknown_or_outside", "accepted"),
])
def test_invalid_numeric_and_codes(spark, kwargs, rule, outcome):
    r = run(spark, [trip(**kwargs)]).collect()[0]
    assert r["outcome"] == outcome and rule in rules_of(r)


# ---------- abnormal timestamp ----------
@pytest.mark.parametrize("kwargs, rule, outcome", [
    ({"minutes": -5}, "ts_dropoff_before_pickup", "rejected"),
    ({"minutes": 0}, "ts_zero_duration", "quarantine"),
    ({"minutes": 1500}, "range_duration_over_max", "quarantine"),
    ({"pickup": dt.datetime(2008, 12, 31, 23, 1)}, "ts_pickup_outside_file_month", "quarantine"),
])
def test_abnormal_timestamp(spark, kwargs, rule, outcome):
    r = run(spark, [trip(**kwargs)]).collect()[0]
    assert r["outcome"] == outcome and rule in rules_of(r)


# ---------- Gold aggregation ----------
def test_zone_hourly_aggregation_reconciles(spark):
    silver = run(spark, [trip(), trip(pu=1), trip(pickup=dt.datetime(2023, 1, 10, 8, 30), fare=10.0, total=15.0),
                         trip(pickup=dt.datetime(2023, 1, 10, 9, 5), pay=0, pc=None, rc=None, flag=None,
                              cong=None, airport=None)])
    acc = silver.where("outcome = 'accepted'")
    gold = {(r["pickup_hour_local"], r["pu_location_id"]): r for r in aggregate(acc, "t").collect()}
    assert sum(r["trips"] for r in gold.values()) == acc.count() == 4
    g = gold[(8, 161)]
    assert g["trips"] == 2 and g["fare_amount_usd_sum"] == 30.0 and g["pu_zone"] == "Midtown Center"
    assert gold[(9, 161)]["flex_fare_trips"] == 1 and gold[(9, 161)]["trips_without_passenger_count"] == 1
