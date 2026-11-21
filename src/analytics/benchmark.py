"""Spark job: storage/query strategy benchmark on NYC TLC trips (spec T06/AC06).

Data held constant: the accepted Silver NYC trips (/datalake/silver/nyc_yellow_trips/v1/outcome=accepted),
the same rows and the same 14 columns, written ONCE (setup, not timed) in three layouts:
  csv_flat           CSV with header, no partitioning
  parquet_flat       Parquet (snappy), no partitioning
  parquet_by_date    Parquet partitioned by pickup_date (the Silver layout)
  parquet_sorted     Parquet, no partitioning, rows sorted by pickup time (narrow row-group min/max stats)
Logic held constant: three SQL queries, identical text for every layout (only the table changes):
  Q1_full_agg      trips and average fare per pickup borough over all rows (reads 2 columns)
  Q2_one_week      trips per local pickup hour for 2023-02-06 .. 2023-02-12 (selective date filter)
  Q3_one_column    sum of total_amount_usd (reads 1 column)
Strategy pairs:
  A  csv_flat vs parquet_flat                 on Q1, Q3   (row text format vs columnar + column pruning)
  B  parquet_flat vs parquet_by_date          on Q2       (partition pruning)
  C  pushdown off vs on (spark.sql.parquet.filterPushdown) on Q2, for parquet_flat (rows in random
     order, so every row group spans all dates) and for parquet_sorted (row groups cover narrow date
     ranges, so min/max statistics can skip them). Run m4_20261121T103358Z showed no gain on the
     unsorted layout; the sorted layout shows when pushdown helps.
Per configuration: 1 warm-up run (recorded, reported separately) + N timed runs (default 3). Each run
records wall time and, from the driver's REST API, the bytes/records the scan stages read. Every run's
result must equal the first layout's result, or the job fails (proves the logic was the same).

Run: spark-submit /workspace/src/analytics/benchmark.py --run-id R --out-dir /workspace/outputs/benchmarks/R [--repeats 3]
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import platform
import statistics
import sys
import time
import urllib.request
from pathlib import Path
from typing import Any

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.common.config import load_config

COLUMNS = ["vendor_id", "pickup_datetime_local", "dropoff_datetime_local", "passenger_count", "trip_distance_mi",
           "pu_location_id", "do_location_id", "pu_borough", "payment_type", "fare_amount_usd", "tip_amount_usd",
           "total_amount_usd", "trip_minutes", "pickup_date"]
QUERIES = {
    # amounts as DECIMAL: exact sums/averages, so results are identical whatever order rows are read in
    "Q1_full_agg": "SELECT pu_borough, count(*) AS trips, round(avg(CAST(fare_amount_usd AS DECIMAL(12,2))), 4) AS avg_fare "
                   "FROM {t} GROUP BY pu_borough ORDER BY pu_borough",
    "Q2_one_week": "SELECT hour(pickup_datetime_local) AS h, count(*) AS trips FROM {t} "
                   "WHERE pickup_date BETWEEN DATE'2023-02-06' AND DATE'2023-02-12' GROUP BY 1 ORDER BY 1",
    "Q3_one_column": "SELECT sum(CAST(total_amount_usd AS DECIMAL(12,2))) AS total FROM {t}",
}
CONFIGS = [  # (pair, query, layout, sql conf overrides)
    ("A_csv_vs_parquet", "Q1_full_agg", "csv_flat", {}),
    ("A_csv_vs_parquet", "Q1_full_agg", "parquet_flat", {}),
    ("A_csv_vs_parquet", "Q3_one_column", "csv_flat", {}),
    ("A_csv_vs_parquet", "Q3_one_column", "parquet_flat", {}),
    ("B_partitioned_vs_flat", "Q2_one_week", "parquet_flat", {}),
    ("B_partitioned_vs_flat", "Q2_one_week", "parquet_by_date", {}),
    ("C_pushdown_off_vs_on", "Q2_one_week", "parquet_flat", {"spark.sql.parquet.filterPushdown": "false"}),
    ("C_pushdown_off_vs_on", "Q2_one_week", "parquet_flat", {"spark.sql.parquet.filterPushdown": "true"}),
    ("C_pushdown_off_vs_on", "Q2_one_week", "parquet_sorted", {"spark.sql.parquet.filterPushdown": "false"}),
    ("C_pushdown_off_vs_on", "Q2_one_week", "parquet_sorted", {"spark.sql.parquet.filterPushdown": "true"}),
]
CSV_SCHEMA = ("vendor_id INT, pickup_datetime_local TIMESTAMP_NTZ, dropoff_datetime_local TIMESTAMP_NTZ, "
              "passenger_count INT, trip_distance_mi DOUBLE, pu_location_id INT, do_location_id INT, "
              "pu_borough STRING, payment_type INT, fare_amount_usd DOUBLE, tip_amount_usd DOUBLE, "
              "total_amount_usd DOUBLE, trip_minutes DOUBLE, pickup_date DATE")


def hdfs_size(spark: SparkSession, path: str) -> dict[str, int]:
    p = spark._jvm.org.apache.hadoop.fs.Path(path)
    cs = p.getFileSystem(spark._jsc.hadoopConfiguration()).getContentSummary(p)
    return {"bytes": int(cs.getLength()), "files": int(cs.getFileCount())}


def setup(spark: SparkSession, root: str, force: bool) -> dict[str, Any]:
    """Write the three layouts once (skipped if present, unless --force). Not timed."""
    base = f"{root}/bench/nyc_trips"
    paths = {"csv_flat": f"{base}/csv_flat", "parquet_flat": f"{base}/parquet_flat",
             "parquet_by_date": f"{base}/parquet_by_date", "parquet_sorted": f"{base}/parquet_sorted"}
    fs_path = spark._jvm.org.apache.hadoop.fs.Path
    fs = fs_path(base).getFileSystem(spark._jsc.hadoopConfiguration())
    src = f"{root}/silver/nyc_yellow_trips/v1/outcome=accepted"
    df = spark.read.option("basePath", src).parquet(src).select(*COLUMNS)
    done = lambda k: fs.exists(fs_path(paths[k] + "/_SUCCESS")) and not force
    if not done("csv_flat"):
        df.repartition(16).write.mode("overwrite").option("header", True).csv(paths["csv_flat"])
    if not done("parquet_flat"):
        df.repartition(16).write.mode("overwrite").parquet(paths["parquet_flat"])
    if not done("parquet_by_date"):
        df.repartition("pickup_date").write.mode("overwrite").partitionBy("pickup_date").parquet(paths["parquet_by_date"])
    if not done("parquet_sorted"):   # global sort -> 16 range-partitioned files, each a narrow time range
        df.repartitionByRange(16, "pickup_datetime_local").sortWithinPartitions("pickup_datetime_local") \
          .write.mode("overwrite").parquet(paths["parquet_sorted"])
    return {k: {"path": p, **hdfs_size(spark, p)} for k, p in paths.items()}


def register(spark: SparkSession, layouts: dict[str, Any]) -> None:
    spark.read.option("header", True).schema(CSV_SCHEMA).csv(layouts["csv_flat"]["path"]).createOrReplaceTempView("csv_flat")
    spark.read.parquet(layouts["parquet_flat"]["path"]).createOrReplaceTempView("parquet_flat")
    spark.read.parquet(layouts["parquet_by_date"]["path"]).createOrReplaceTempView("parquet_by_date")
    spark.read.parquet(layouts["parquet_sorted"]["path"]).createOrReplaceTempView("parquet_sorted")


def stage_io(ui: str, app: str, group: str) -> dict[str, int]:
    """Input bytes/records of the stages of the jobs in this job group (driver REST API)."""
    def get(p: str) -> Any:
        return json.load(urllib.request.urlopen(f"{ui}/api/v1/applications/{app}{p}", timeout=10))
    jobs = [j for j in get("/jobs") if j.get("jobGroup") == group]
    stage_ids = {s for j in jobs for s in j["stageIds"]}
    tot = {"input_bytes": 0, "input_records": 0, "shuffle_read_bytes": 0, "tasks": 0}
    for s in get("/stages"):
        if s["stageId"] in stage_ids and s["status"] == "COMPLETE":
            tot["input_bytes"] += s["inputBytes"]; tot["input_records"] += s["inputRecords"]
            tot["shuffle_read_bytes"] += s["shuffleReadBytes"]; tot["tasks"] += s["numCompleteTasks"]
    return tot


def run_once(spark: SparkSession, sql: str, group: str) -> tuple[float, list, dict]:
    spark.sparkContext.setJobGroup(group, group)
    t0 = time.perf_counter()
    rows = spark.sql(sql).collect()
    elapsed = time.perf_counter() - t0
    time.sleep(1.0)   # let the listener publish stage metrics
    return elapsed, [tuple(r) for r in rows], stage_io(spark.sparkContext.uiWebUrl, spark.sparkContext.applicationId, group)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--run-id", required=True)
    ap.add_argument("--out-dir", required=True)
    ap.add_argument("--repeats", type=int, default=3)
    ap.add_argument("--force-setup", action="store_true")
    args = ap.parse_args()
    out = Path(args.out_dir)
    (out / "plans").mkdir(parents=True, exist_ok=True)
    spark = SparkSession.builder.appName("bdfp-m4-benchmark").getOrCreate()
    cfg = load_config()
    root = f"{cfg['hdfs']['uri']}{cfg['hdfs']['datalake_root']}"
    layouts = setup(spark, root, args.force_setup)
    register(spark, layouts)
    rows_per_layout = {k: spark.table(k).count() for k in layouts}

    sc = spark.sparkContext
    env = {"spark_version": spark.version, "master": sc.master, "app_id": sc.applicationId,
           "executor_memory": sc.getConf().get("spark.executor.memory"),
           "executor_cores": sc.getConf().get("spark.executor.cores"),
           "shuffle_partitions": spark.conf.get("spark.sql.shuffle.partitions"),
           "aqe": spark.conf.get("spark.sql.adaptive.enabled"),
           "driver_host_cpus": os.cpu_count(), "python": platform.python_version(),
           "storage": "HDFS 3.3.6, 3 DataNodes on one host, replication 2", "cache": "no DataFrame caching; "
           "OS page cache and HDFS short-circuit not controlled (warm-up run reported separately)"}
    results, reference = [], {}
    for pair, q, layout, conf in CONFIGS:
        for k, v in conf.items():
            spark.conf.set(k, v)
        sql = QUERIES[q].format(t=layout)
        label = f"{pair}|{q}|{layout}|" + ",".join(f"{k.split('.')[-1]}={v}" for k, v in conf.items())
        plan = spark._jvm.PythonSQLUtils.explainString(spark.sql(sql)._jdf.queryExecution(), "formatted")
        (out / "plans" / (label.replace("|", "__").replace(",", "_").replace("=", "-") + ".txt")).write_text(plan)
        runs = []
        for i in range(args.repeats + 1):   # run 0 = warm-up
            elapsed, rows, io = run_once(spark, sql, f"{args.run_id}:{label}:{i}")
            reference.setdefault(q, rows)
            if rows != reference[q]:
                raise SystemExit(f"result differs for {label} run {i}: {rows} != {reference[q]}")
            runs.append({"run": i, "warmup": i == 0, "elapsed_s": round(elapsed, 3), **io})
        timed = [r["elapsed_s"] for r in runs if not r["warmup"]]
        results.append({"pair": pair, "query": q, "layout": layout, "conf": conf, "sql": sql, "runs": runs,
                        "median_s": round(statistics.median(timed), 3), "min_s": min(timed), "max_s": max(timed),
                        "warmup_s": runs[0]["elapsed_s"], "input_bytes_median": statistics.median(
                            r["input_bytes"] for r in runs if not r["warmup"]),
                        "result": reference[q]})
        for k in conf:
            spark.conf.unset(k)
        print(f"{label}: median {results[-1]['median_s']} s, warm-up {results[-1]['warmup_s']} s", flush=True)

    report = {"run_id": args.run_id, "finished_at": dt.datetime.now(dt.timezone.utc).isoformat(),
              "environment": env, "layouts": layouts, "rows_per_layout": rows_per_layout,
              "repeats": args.repeats, "queries": QUERIES, "results": results}
    (out / "benchmark_results.json").write_text(json.dumps(report, indent=2, default=str))
    spark.stop()
    return 0 if len(set(rows_per_layout.values())) == 1 else 1


if __name__ == "__main__":
    sys.exit(main())
