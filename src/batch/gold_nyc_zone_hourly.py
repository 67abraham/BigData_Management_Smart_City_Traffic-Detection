"""Spark job: Silver NYC trips -> Gold `nyc_zone_hourly_demand` v1 (spec T03 temporal/regional aggregation).

Input   /datalake/silver/nyc_yellow_trips/v1/outcome=accepted
Output  /datalake/gold/nyc_zone_hourly_demand/v1/pickup_date=YYYY-MM-DD/
        --report-out FILE (JSON), --explain-out FILE (formatted plan: the Map-Shuffle-Reduce step)

One row per (pickup_date, pickup_hour_local, pu_location_id): trip count and sums/averages. It is
taxi demand by pickup zone, NOT road traffic volume (spec §4), and is never joined to UCI.
Grouped by LocationID, not zone name: some names repeat (56/57 "Corona", 103-105).

Plan shape (see the explain file): HashAggregate (partial, per input partition = map side)
-> Exchange hashpartitioning(group keys) (shuffle) -> HashAggregate (final = reduce side).

Run: spark-submit /workspace/src/batch/gold_nyc_zone_hourly.py --report-out R.json --explain-out P.txt
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import sys
from pathlib import Path

import yaml
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.common.config import PROJECT_ROOT, load_config

GOLD = {"dataset": "nyc_zone_hourly_demand", "version": "v1"}
KEYS = ["pickup_date", "pickup_hour_local", "pu_location_id"]


def aggregate(trips: DataFrame, run_id: str) -> DataFrame:
    # pu_borough/pu_zone depend only on pu_location_id, so grouping by them adds no groups; doing so
    # (instead of first()) keeps all buffers numeric, which lets Spark use HashAggregate (no sort).
    return (trips.groupBy(*KEYS, "pu_borough", "pu_zone")
            .agg(F.count("*").alias("trips"),
                 F.sum("passenger_count").alias("passengers_reported"),
                 F.sum(F.col("passenger_count").isNull().cast("int")).alias("trips_without_passenger_count"),
                 F.round(F.sum("trip_distance_mi"), 2).alias("trip_distance_mi_sum"),
                 F.round(F.avg("trip_distance_mi"), 3).alias("trip_distance_mi_avg"),
                 F.round(F.avg("trip_minutes"), 2).alias("trip_minutes_avg"),
                 F.round(F.sum("fare_amount_usd"), 2).alias("fare_amount_usd_sum"),
                 F.round(F.sum("total_amount_usd"), 2).alias("total_amount_usd_sum"),
                 F.sum((F.col("payment_type") == 1).cast("int")).alias("credit_card_trips"),
                 F.sum(F.col("is_flex_fare").cast("int")).alias("flex_fare_trips"))
            .withColumn("_gold_run_id", F.lit(run_id))
            .withColumn("_gold_version", F.lit(GOLD["version"])))


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--run-id", default=dt.datetime.now(dt.timezone.utc).strftime("m2_%Y%m%dT%H%M%SZ"))
    ap.add_argument("--report-out", required=True)
    ap.add_argument("--explain-out", required=True)
    args = ap.parse_args()

    spark = SparkSession.builder.appName("bdfp-m2-gold-nyc_zone_hourly").getOrCreate()
    platform = load_config()
    cfg = yaml.safe_load((PROJECT_ROOT / "configs" / "quality.yaml").read_text())["nyc_tlc"]
    root = f"{platform['hdfs']['uri']}{platform['hdfs']['datalake_root']}"
    src = f"{root}/silver/{cfg['silver_dataset']}/{cfg['version']}/outcome=accepted"
    trips = spark.read.option("basePath", src).parquet(src)

    gold = aggregate(trips, args.run_id)
    out = f"{root}/gold/{GOLD['dataset']}/{GOLD['version']}"
    Path(args.explain_out).write_text(
        spark._jvm.PythonSQLUtils.explainString(gold._jdf.queryExecution(), "formatted"), encoding="utf-8")
    gold.repartition("pickup_date").write.mode("overwrite").partitionBy("pickup_date").parquet(out)

    back = spark.read.parquet(out)
    silver_rows = trips.count()
    g = back.agg(F.count("*").alias("rows"), F.sum("trips").alias("trips"),
                 F.countDistinct("pu_location_id").alias("zones"),
                 F.countDistinct("pickup_date").alias("days")).first()
    keys_unique = back.select(*KEYS).distinct().count() == g["rows"]
    top = [r.asDict() for r in back.groupBy("pu_location_id", "pu_borough", "pu_zone")
           .agg(F.sum("trips").alias("trips")).orderBy(F.desc("trips")).limit(5).collect()]
    rep = {
        "dataset": GOLD["dataset"], "version": GOLD["version"], "input": src, "output": out,
        "run_id": args.run_id, "spark_app_id": spark.sparkContext.applicationId,
        "reconciliation": {"silver_accepted_rows": silver_rows, "sum_of_trips": int(g["trips"]),
                           "gold_rows": int(g["rows"]), "keys_unique": keys_unique,
                           "reconciled": silver_rows == int(g["trips"]) and keys_unique},
        "coverage": {"pickup_zones": int(g["zones"]), "pickup_days": int(g["days"])},
        "top_pickup_zones": top,
        "schema": back.schema.simpleString(),
        "samples": [{k: (v.isoformat() if hasattr(v, "isoformat") else v) for k, v in r.asDict().items()}
                    for r in back.orderBy(F.desc("trips")).limit(3).collect()],
    }
    Path(args.report_out).write_text(json.dumps(rep, indent=2, default=str), encoding="utf-8")
    print(f"report -> {args.report_out}; reconciliation={rep['reconciliation']}", flush=True)
    spark.stop()
    return 0 if rep["reconciliation"]["reconciled"] else 1


if __name__ == "__main__":
    sys.exit(main())
