"""Spark job: export small dashboard tables from the published products (spec T09: real outputs, traceable).

Writes outputs/dashboard/<run>/:
  traffic_daily.csv      local_date, hours, total_volume              <- traffic_hourly_v1
  window_metrics.csv     every final 6 h window (start, count, sum, completeness, firing)
                                                                       <- traffic_window_metrics_v1
  nyc_daily.csv          pickup_date, pu_borough, trips               <- nyc_zone_hourly_demand v1
  layer_counts.json      rows per layer/dataset read from HDFS now (Bronze -> Silver -> Gold -> products)
  manifest.json          run id, Spark app id, time, every input path and the product release runs
The dashboard (src/visualisation/app.py) reads only these files and the committed evidence.

Run: spark-submit /workspace/src/visualisation/export_dashboard_data.py --run-id R --out-dir /workspace/outputs/dashboard/R
"""
from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.common.config import load_config
from src.products import contracts


def write_csv(path: Path, rows: list[dict]) -> int:
    with path.open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=list(rows[0]))
        w.writeheader()
        w.writerows(rows)
    return len(rows)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--run-id", required=True)
    ap.add_argument("--out-dir", required=True)
    args = ap.parse_args()
    out = Path(args.out_dir)
    out.mkdir(parents=True, exist_ok=True)
    spark = SparkSession.builder.appName("bdfp-m5-dashboard-export").getOrCreate()
    cfg = load_config()
    root = f"{cfg['hdfs']['uri']}{cfg['hdfs']['datalake_root']}"
    p = {n: contracts.load(n)["location"]["path"].rstrip("/") for n in contracts.PRODUCTS}
    nyc_gold = f"{root}/gold/nyc_zone_hourly_demand/v1"

    files = {}
    hourly = spark.read.parquet(p["traffic_hourly"])
    rows = [r.asDict() for r in hourly.groupBy("local_date").agg(F.count("*").alias("hours"),
            F.sum("traffic_volume").alias("total_volume")).orderBy("local_date").collect()]
    files["traffic_daily.csv"] = write_csv(out / "traffic_daily.csv", rows)
    win = spark.read.parquet(p["traffic_window_metrics"])
    rows = [r.asDict() for r in win.select("window_start_utc", "event_count", "volume_sum", "completeness",
                                           "final_firing", "final_update_seq", "job_run_id")
            .orderBy("window_start_utc").collect()]
    files["window_metrics.csv"] = write_csv(out / "window_metrics.csv", rows)
    nyc = spark.read.parquet(nyc_gold)
    rows = [r.asDict() for r in nyc.groupBy("pickup_date", "pu_borough").agg(F.sum("trips").alias("trips"))
            .orderBy("pickup_date", "pu_borough").collect()]
    files["nyc_daily.csv"] = write_csv(out / "nyc_daily.csv", rows)

    def count(path: str, where: str | None = None) -> int:
        df = spark.read.parquet(path)
        return df.where(where).count() if where else df.count()

    def latest(path: str) -> str:
        return spark.read.parquet(path).select(F.max("ingest_date")).first()[0].isoformat()

    layers = {}
    for src, silver, gold, product in [
            ("uci_metro", "uci_traffic_hourly/v1", "uci_traffic_weather_hourly/v1", "traffic_hourly"),
            ("open_meteo", "open_meteo_hourly/v1", None, None),
            ("nyc_tlc", "nyc_yellow_trips/v1", "nyc_zone_hourly_demand/v1", None),
            ("kitti", None, None, "road_image_manifest")]:
        b = f"{root}/bronze/{src}/records"
        d = latest(b)
        entry = {"bronze_records": count(b, f"ingest_date = '{d}'"), "bronze_ingest_date": d}
        if silver:
            s = spark.read.parquet(f"{root}/silver/{silver}")
            entry["silver_records_by_outcome"] = {r["outcome"]: int(r["count"]) for r in s.groupBy("outcome").count().collect()}
            if "source_rows" in s.columns:
                entry["silver_source_rows"] = int(s.agg(F.sum("source_rows")).first()[0])
        if gold:
            entry["gold_rows"] = count(f"{root}/gold/{gold}")
        if product:
            entry["product_rows"] = {product: count(p[product])}
        layers[src] = entry
    layers["uci_metro"]["product_rows"].update({"traffic_features": count(p["traffic_features"]),
                                                "traffic_window_metrics": count(p["traffic_window_metrics"])})
    (out / "layer_counts.json").write_text(json.dumps(layers, indent=2))

    releases = {n: (contracts.load(n).get("release") or {}).get("run_id") for n in contracts.PRODUCTS}
    manifest = {"run_id": args.run_id, "spark_app_id": spark.sparkContext.applicationId,
                "generated_at": dt.datetime.now(dt.timezone.utc).isoformat(), "files": files,
                "inputs": {**p, "nyc_zone_hourly_demand": nyc_gold}, "product_release_runs": releases}
    (out / "manifest.json").write_text(json.dumps(manifest, indent=2))
    print(json.dumps({"files": files, "layers": layers}, default=str), flush=True)
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
