"""Spark SQL analyses over the published products and Gold (spec T06: at least three SQL analyses).

  A1_hourly_pattern        traffic_hourly_v1: mean/median volume per local hour, weekday vs weekend
  A2_day_type              traffic_hourly_v1: daily volume on working days, weekends and holidays (full days only)
  A3_weather_impact        traffic_hourly_v1: volume in ERA5 weather classes relative to dry hours of the SAME
                           local hour (controls for the time-of-day mix); weekday daytime, non-holiday
  A4_nyc_boroughs          nyc_zone_hourly_demand: trips, share and average distance per pickup borough
  A5_nyc_top_zones         nyc_zone_hourly_demand: ten busiest pickup zones
NYC is analysed separately and never joined to UCI (spec §4). ERA5 is reanalysis, not road-side observation.

Output per analysis: <out>/<name>.csv, <name>.json and plans/<name>.txt; <out>/analyses.json summary.
Run: spark-submit /workspace/src/analytics/sql_analyses.py --out-dir /workspace/outputs/reports/analytics/<run>
"""
from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import sys
import time
from pathlib import Path

from pyspark.sql import SparkSession

from src.common.config import load_config
from src.products import contracts

ANALYSES = {
    "A1_hourly_pattern": {
        "question": "How does hourly volume at ATR 301 change over the day, on weekdays vs weekends?",
        "sql": """
            SELECT CASE WHEN dayofweek(local_date) IN (1, 7) THEN 'weekend' ELSE 'weekday' END AS day_type,
                   hour(hour_start_local) AS local_hour, count(*) AS hours,
                   round(avg(traffic_volume), 1) AS avg_volume,
                   percentile_approx(traffic_volume, 0.5) AS median_volume
            FROM traffic_hourly GROUP BY 1, 2 ORDER BY 1, 2"""},
    "A2_day_type": {
        "question": "Is daily volume lower on holidays than on working days and weekends?",
        "sql": """
            WITH d AS (SELECT local_date, max(is_holiday_date) AS holiday, dayofweek(local_date) AS dow,
                              count(*) AS hours, sum(traffic_volume) AS volume
                       FROM traffic_hourly GROUP BY local_date)
            SELECT CASE WHEN holiday THEN 'holiday' WHEN dow IN (1, 7) THEN 'weekend' ELSE 'working_day' END AS day_type,
                   count(*) AS full_days, round(avg(volume)) AS avg_daily_volume,
                   min(volume) AS min_daily_volume, max(volume) AS max_daily_volume
            FROM d WHERE hours = 24 GROUP BY 1 ORDER BY 1"""},
    "A3_weather_impact": {
        "question": "Does ERA5 rain or snow go with lower volume, compared with dry hours at the same local hour?",
        "sql": """
            WITH h AS (SELECT hour(hour_start_local) AS local_hour, traffic_volume,
                              CASE WHEN era5_snowfall_cm > 0 THEN 'snow'
                                   WHEN era5_precipitation_mm = 0 THEN 'dry'
                                   WHEN era5_precipitation_mm < 1 THEN 'rain_lt_1mm'
                                   ELSE 'rain_ge_1mm' END AS weather_class
                       FROM traffic_hourly
                       WHERE NOT is_holiday_date AND dayofweek(local_date) BETWEEN 2 AND 6
                             AND hour(hour_start_local) BETWEEN 7 AND 18),
                 base AS (SELECT local_hour, avg(traffic_volume) AS dry_avg FROM h
                          WHERE weather_class = 'dry' GROUP BY local_hour)
            SELECT weather_class, count(*) AS hours, round(avg(traffic_volume), 1) AS avg_volume,
                   round(avg(traffic_volume / dry_avg), 4) AS volume_vs_dry_same_hour
            FROM h JOIN base USING (local_hour) GROUP BY weather_class ORDER BY volume_vs_dry_same_hour DESC"""},
    "A4_nyc_boroughs": {
        "question": "Where in NYC do yellow-taxi trips start (2023-01..03)?",
        "sql": """
            SELECT pu_borough, sum(trips) AS trips, round(100 * sum(trips) / sum(sum(trips)) OVER (), 2) AS pct_trips,
                   round(sum(trip_distance_mi_sum) / sum(trips), 2) AS avg_trip_mi,
                   round(sum(total_amount_usd_sum) / sum(trips), 2) AS avg_total_usd
            FROM nyc_zone_hourly GROUP BY pu_borough ORDER BY trips DESC"""},
    "A5_nyc_top_zones": {
        "question": "Which ten pickup zones have the most trips, and at what local hour do they peak?",
        "sql": """
            WITH z AS (SELECT pu_location_id, pu_borough, pu_zone, sum(trips) AS trips
                       FROM nyc_zone_hourly GROUP BY 1, 2, 3),
                 hh AS (SELECT pu_location_id, pickup_hour_local, sum(trips) AS t,
                               row_number() OVER (PARTITION BY pu_location_id ORDER BY sum(trips) DESC) AS rk
                        FROM nyc_zone_hourly GROUP BY 1, 2)
            SELECT z.pu_location_id, z.pu_borough, z.pu_zone, z.trips, hh.pickup_hour_local AS peak_local_hour
            FROM z JOIN hh ON z.pu_location_id = hh.pu_location_id AND hh.rk = 1
            ORDER BY z.trips DESC LIMIT 10"""},
}


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--out-dir", required=True)
    ap.add_argument("--run-id", default=dt.datetime.now(dt.timezone.utc).strftime("m4_%Y%m%dT%H%M%SZ"))
    args = ap.parse_args()
    out = Path(args.out_dir)
    (out / "plans").mkdir(parents=True, exist_ok=True)
    spark = SparkSession.builder.appName("bdfp-m4-sql-analyses").getOrCreate()
    root = f"{load_config()['hdfs']['uri']}{load_config()['hdfs']['datalake_root']}"
    spark.read.parquet(contracts.load("traffic_hourly")["location"]["path"]).createOrReplaceTempView("traffic_hourly")
    spark.read.parquet(f"{root}/gold/nyc_zone_hourly_demand/v1").createOrReplaceTempView("nyc_zone_hourly")
    summary = {"run_id": args.run_id, "spark_app_id": spark.sparkContext.applicationId, "analyses": {}}
    for name, a in ANALYSES.items():
        df = spark.sql(a["sql"])
        (out / "plans" / f"{name}.txt").write_text(
            spark._jvm.PythonSQLUtils.explainString(df._jdf.queryExecution(), "formatted"))
        t0 = time.perf_counter()
        rows = [r.asDict() for r in df.collect()]
        el = time.perf_counter() - t0
        with (out / f"{name}.csv").open("w", newline="", encoding="utf-8") as fh:
            w = csv.DictWriter(fh, fieldnames=df.columns)
            w.writeheader()
            w.writerows(rows)
        (out / f"{name}.json").write_text(json.dumps({"question": a["question"], "sql": a["sql"].strip(),
                                                      "rows": rows}, indent=2, default=str))
        summary["analyses"][name] = {"question": a["question"], "rows": len(rows), "elapsed_s": round(el, 3)}
        print(f"{name}: {len(rows)} rows in {el:.2f} s", flush=True)
    (out / "analyses.json").write_text(json.dumps(summary, indent=2))
    spark.stop()
    return 0 if all(v["rows"] > 0 for v in summary["analyses"].values()) else 1


if __name__ == "__main__":
    sys.exit(main())
