"""Spark job: build the four T07 data products from Gold/Bronze and the Flink results (spec T07).

  traffic_hourly          <- /datalake/gold/uci_traffic_weather_hourly/v1
  traffic_features        <- traffic_hourly product (lags from EARLIER hours only)
  road_image_manifest     <- /datalake/bronze/kitti/{image_inventory,records}  (+ CSV copy in data/manifests/)
  traffic_window_metrics  <- data/local/m3/<window-run>/replay_metrics.jsonl (Flink results of a replay run)

Columns and types are taken from contracts/<name>_v1.yaml (select + cast in contract order), so the
product cannot drift from its contract; src/products/readback.py checks the written data independently.
Output: /datalake/gold/products/<name>/v1/ (overwrite = idempotent) and --report-out JSON per product.

Run: spark-submit /workspace/src/products/build_products.py --product all --window-run R --run-id ID --report-out F
"""
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import sys
from pathlib import Path
from typing import Any, Mapping

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

from src.common.config import PROJECT_ROOT, load_config
from src.products import contracts
from src.products.windows import window_rows, write_manifest_csv

LAGS_H = (1, 2, 24, 168)
KITTI_ORIGIN = "KITTI 2D object detection benchmark, training split"


def conform(df: DataFrame, contract: Mapping[str, Any]) -> DataFrame:
    """Select exactly the contract's fields, in order, cast to the contract's types."""
    return df.select(*[F.col(f["name"]).cast(f["type"]).alias(f["name"]) for f in contract["fields"]])


# ---------- traffic_hourly ----------
def traffic_hourly(gold: DataFrame, run_id: str) -> DataFrame:
    return (gold.withColumnRenamed("temp_c", "uci_temp_c").withColumnRenamed("rain_1h_mm", "uci_rain_1h_mm")
                .withColumnRenamed("snow_1h_mm", "uci_snow_1h_mm")
                .withColumnRenamed("clouds_all_pct", "uci_clouds_all_pct")
                .withColumnRenamed("weather_main", "uci_weather_main")
                .withColumnRenamed("weather_numeric_conflict", "uci_weather_conflict")
                .withColumn("_product_run_id", F.lit(run_id)))


# ---------- traffic_features ----------
def traffic_features(hourly: DataFrame, run_id: str) -> DataFrame:
    """Features usable at prediction time = hour_start_utc: calendar of that hour, lags of EARLIER hours."""
    base = hourly.select("station_id", "hour_start_utc", "hour_start_local", "local_date", "is_holiday_date",
                         F.col("traffic_volume").alias("target_traffic_volume"))
    out = base
    for k in LAGS_H:   # value of the hour that started k hours before, matched on the exact UTC hour
        lag = hourly.select("station_id", (F.col("hour_start_utc") + F.expr(f"INTERVAL {k} HOURS")).alias("hour_start_utc"),
                            F.col("traffic_volume").alias(f"lag_{k}h"))
        out = out.join(lag, ["station_id", "hour_start_utc"], "left")
    secs = F.unix_timestamp("hour_start_utc")
    prev24 = Window.partitionBy("station_id").orderBy(secs).rangeBetween(-24 * 3600, -3600)   # t-24h .. t-1h
    dow = ((F.dayofweek("hour_start_local") + 5) % 7) + 1                                      # 1 = Monday
    return (out.withColumn("rolling_mean_prev_24h", F.avg("target_traffic_volume").over(prev24))
               .withColumn("rolling_count_prev_24h", F.count("target_traffic_volume").over(prev24))
               .withColumn("hour_of_day", F.hour("hour_start_local"))
               .withColumn("day_of_week", dow)
               .withColumn("month", F.month("hour_start_local"))
               .withColumn("is_weekend", dow >= 6)
               .withColumn("lags_complete", F.col("lag_1h").isNotNull() & F.col("lag_24h").isNotNull()
                           & F.col("lag_168h").isNotNull())
               .withColumn("split", F.when(F.col("local_date") < F.lit("2017-01-01").cast("date"), "train")
                                     .when(F.col("local_date") < F.lit("2018-01-01").cast("date"), "validation")
                                     .otherwise("test"))
               .withColumn("_product_run_id", F.lit(run_id)))


# ---------- road_image_manifest ----------
def road_image_manifest(inventory: DataFrame, labels: DataFrame, raw_path: str, run_id: str) -> DataFrame:
    ok = labels.where("well_formed")
    per = ok.groupBy("image_id").agg(
        F.count("*").alias("label_lines"),
        F.sum((F.col("type") != "DontCare").cast("long")).alias("object_count"),
        F.sum((F.col("type") == "DontCare").cast("long")).alias("dontcare_count"),
        F.array_sort(F.collect_set(F.when(F.col("type") != "DontCare", F.col("type")))).alias("object_types"))
    return (inventory.join(per, "image_id", "left")
            .withColumn("file_available", F.col("is_available"))
            .withColumn("width_px", F.col("width")).withColumn("height_px", F.col("height"))
            .withColumn("image_sha256", F.col("sha256"))
            .withColumn("label_path", F.concat(F.lit(f"{raw_path}/data_object_label_2.zip!"), F.col("label_member")))
            .fillna(0, ["label_lines", "object_count", "dontcare_count"])
            .withColumn("object_types", F.coalesce("object_types", F.expr("CAST(array() AS array<string>)")))
            .withColumn("origin", F.lit(KITTI_ORIGIN))
            .withColumn("label_archive_sha256", F.col("_label_archive_sha256"))
            .withColumn("_product_run_id", F.lit(run_id)))


# ---------- traffic_window_metrics ----------
WINDOW_SCHEMA = T.StructType([
    T.StructField("station_id", T.StringType()), T.StructField("window_size_hours", T.LongType()),
    T.StructField("event_count", T.LongType()), T.StructField("expected_events", T.LongType()),
    T.StructField("completeness", T.DoubleType()), T.StructField("volume_sum", T.LongType()),
    T.StructField("volume_avg", T.DoubleType()), T.StructField("volume_min", T.LongType()),
    T.StructField("volume_max", T.LongType()), T.StructField("watermark_at_emit", T.StringType()),
    T.StructField("record_kind", T.StringType()), T.StructField("source_topic", T.StringType()),
    T.StructField("job_run_id", T.StringType()), T.StructField("window_start_utc", T.TimestampType()),
    T.StructField("window_end_utc", T.TimestampType()), T.StructField("final_firing", T.StringType()),
    T.StructField("final_update_seq", T.LongType()), T.StructField("firings_seen", T.LongType()),
    T.StructField("_product_run_id", T.StringType()), T.StructField("window_year", T.LongType())])


# ---------- job ----------
def hdfs_size(spark: SparkSession, path: str) -> tuple[int, int]:
    jvm = spark._jvm
    p = jvm.org.apache.hadoop.fs.Path(path)
    cs = p.getFileSystem(spark._jsc.hadoopConfiguration()).getContentSummary(p)
    return int(cs.getLength()), int(cs.getFileCount())


def latest_partition(spark: SparkSession, path: str) -> str:
    return spark.read.parquet(path).select(F.max("ingest_date")).first()[0].isoformat()


def build(spark: SparkSession, name: str, root: str, run_id: str, window_run: str | None) -> dict[str, Any]:
    c = contracts.load(name)
    out = c["location"]["path"].rstrip("/")
    extra: dict[str, Any] = {}
    if name == "traffic_hourly":
        src = f"{root}/gold/uci_traffic_weather_hourly/v1"
        df = traffic_hourly(spark.read.parquet(src), run_id)
        inputs = [src]
    elif name == "traffic_features":
        src = contracts.load("traffic_hourly")["location"]["path"].rstrip("/")
        df = traffic_features(spark.read.parquet(src), run_id)
        inputs = [src]
    elif name == "road_image_manifest":
        d = latest_partition(spark, f"{root}/bronze/kitti/image_inventory")
        inv = spark.read.parquet(f"{root}/bronze/kitti/image_inventory/ingest_date={d}")
        lab = spark.read.parquet(f"{root}/bronze/kitti/records/ingest_date={d}")
        df = road_image_manifest(inv, lab, f"{root}/bronze/kitti/raw/ingest_date={d}", run_id)
        inputs = [f"{root}/bronze/kitti/image_inventory/ingest_date={d}", f"{root}/bronze/kitti/records/ingest_date={d}"]
    elif name == "traffic_window_metrics":
        if not window_run:
            raise SystemExit("--window-run is required for traffic_window_metrics")
        f = PROJECT_ROOT / "data" / "local" / "m3" / window_run / "replay_metrics.jsonl"
        raw = f.read_bytes()
        job = f"{window_run}_replay"
        results = [v for v in (json.loads(json.loads(l)["value"]) for l in raw.decode().splitlines() if l.strip())
                   if v.get("job_run_id") == job and v.get("station_id") != "FLUSH"]
        df = spark.createDataFrame(window_rows(results, run_id), WINDOW_SCHEMA)
        inputs = [str(f.relative_to(PROJECT_ROOT))]
        extra = {"input_sha256": hashlib.sha256(raw).hexdigest(), "result_rows_read": len(results),
                 "flink_job_run_id": job}
    else:
        raise SystemExit(f"unknown product {name}")

    df = conform(df, c)
    parts = c["location"].get("partitioning") or []
    w = (df.repartition(*parts) if parts else df.coalesce(1)).write.mode("overwrite")
    (w.partitionBy(*parts) if parts else w).parquet(out)
    back = spark.read.parquet(out)
    size, files = hdfs_size(spark, out)
    rep: dict[str, Any] = {"product": f"{c['name']}_{c['version']}", "run_id": run_id, "location": out,
                           "inputs": inputs, "row_count": back.count(), "size_bytes": size, "files": files,
                           "built_at": dt.datetime.now(dt.timezone.utc).isoformat(), **extra}
    nulls = back.select(*[F.sum(F.col(f["name"]).isNull().cast("int")).alias(f["name"])
                          for f in c["fields"] if f["nullable"]]).first()
    rep["quality_summary"] = {"null_counts": {k: int(v or 0) for k, v in (nulls.asDict() if nulls else {}).items()}}
    rep["quality_summary"].update(summary(name, back))
    if name == "road_image_manifest":
        rows = back.orderBy("image_id").collect()
        csv = PROJECT_ROOT / "data" / "manifests" / "road_image_manifest_v1.csv"
        cols = contracts.field_names(c)
        write_manifest_csv(csv, cols, [r.asDict() for r in rows])
        rep["csv_copy"] = {"path": str(csv.relative_to(PROJECT_ROOT)), "rows": len(rows),
                           "sha256": hashlib.sha256(csv.read_bytes()).hexdigest()}
    return rep


def summary(name: str, df: DataFrame) -> dict[str, Any]:
    """Product-specific quality summary numbers for the contract release block."""
    if name == "traffic_hourly":
        r = df.agg(F.min("hour_start_local").alias("lo"), F.max("hour_start_local").alias("hi"),
                   F.min("traffic_volume").alias("vmin"), F.max("traffic_volume").alias("vmax"),
                   F.sum(F.col("uci_weather_conflict").cast("int")).alias("conf"),
                   F.countDistinct("local_date").alias("days")).first()
        return {"temporal_coverage": {"first_hour_local": str(r["lo"]), "last_hour_local": str(r["hi"]),
                                      "local_dates": int(r["days"])},
                "traffic_volume_range": [int(r["vmin"]), int(r["vmax"])],
                "hours_with_uci_weather_conflict": int(r["conf"])}
    if name == "traffic_features":
        s = {r["split"]: int(r["count"]) for r in df.groupBy("split").count().collect()}
        r = df.agg(F.sum(F.col("lags_complete").cast("int")).alias("lc"), F.sum(F.col("lag_1h").isNull().cast("int")).alias("l1"),
                   F.min("local_date").alias("lo"), F.max("local_date").alias("hi")).first()
        return {"rows_per_split": s, "rows_with_complete_lags": int(r["lc"]), "rows_without_lag_1h": int(r["l1"]),
                "temporal_coverage": {"first_local_date": str(r["lo"]), "last_local_date": str(r["hi"])}}
    if name == "road_image_manifest":
        r = df.agg(F.count("*").alias("n"), F.sum(F.col("file_available").cast("int")).alias("avail"),
                   F.sum("object_count").alias("obj"), F.sum("dontcare_count").alias("dc")).first()
        return {"image_ids": int(r["n"]), "files_available": int(r["avail"]), "objects": int(r["obj"]),
                "dontcare_regions": int(r["dc"])}
    if name == "traffic_window_metrics":
        r = df.agg(F.count("*").alias("n"), F.min("window_start_utc").alias("lo"), F.max("window_start_utc").alias("hi"),
                   F.sum((F.col("completeness") == 1).cast("int")).alias("full"),
                   F.sum("event_count").alias("ev"), F.sum((F.col("final_update_seq") > 0).cast("int")).alias("late"),
                   F.countDistinct("job_run_id").alias("jobs")).first()
        return {"windows": int(r["n"]), "complete_windows": int(r["full"]), "events_in_windows": int(r["ev"]),
                "windows_with_late_update": int(r["late"]), "flink_job_runs": int(r["jobs"]),
                "temporal_coverage": {"first_window_start_utc": str(r["lo"]), "last_window_start_utc": str(r["hi"])}}
    return {}


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--product", default="all", choices=["all", *contracts.PRODUCTS])
    ap.add_argument("--window-run", help="M3 replay run whose collected Flink results to publish")
    ap.add_argument("--run-id", default=dt.datetime.now(dt.timezone.utc).strftime("m4_%Y%m%dT%H%M%SZ"))
    ap.add_argument("--report-out", required=True)
    args = ap.parse_args()
    spark = SparkSession.builder.appName("bdfp-m4-build-products").getOrCreate()
    platform = load_config()
    root = f"{platform['hdfs']['uri']}{platform['hdfs']['datalake_root']}"
    names = list(contracts.PRODUCTS) if args.product == "all" else [args.product]   # order: hourly before features
    reports = {n: build(spark, n, root, args.run_id, args.window_run) for n in names}
    for n, r in reports.items():
        r["spark_app_id"] = spark.sparkContext.applicationId
    Path(args.report_out).write_text(json.dumps(reports, indent=2, default=str), encoding="utf-8")
    print(json.dumps({n: r["row_count"] for n, r in reports.items()}), flush=True)
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
