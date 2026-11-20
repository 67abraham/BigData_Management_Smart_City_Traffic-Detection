"""M0 smoke test: Spark (standalone cluster) writes Parquet to HDFS and reads it back.

INFRASTRUCTURE CHECK ONLY. The rows are generated with spark.range(); they are not
business data and must never be reported as project results.

Run (from the host, inside the Codespace):
    docker compose exec spark-master spark-submit /workspace/src/batch/hdfs_smoke.py
"""
from __future__ import annotations

import json
import sys
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.common.config import load_config

N_ROWS = 100_000


def main() -> int:
    cfg = load_config()
    target = f"{cfg['hdfs']['uri']}{cfg['hdfs']['datalake_root']}/_smoke/m0_parquet"

    spark = SparkSession.builder.appName("bdfp-m0-hdfs-smoke").getOrCreate()
    sc = spark.sparkContext
    print(f"[smoke] spark={spark.version} master={sc.master} app_id={sc.applicationId}")
    print(f"[smoke] defaultParallelism={sc.defaultParallelism}")

    df = (
        spark.range(N_ROWS)
        .withColumn("bucket", (F.col("id") % 4).cast("int"))
        .withColumn("value", F.rand(seed=42))
    )
    t0 = time.perf_counter()
    df.write.mode("overwrite").partitionBy("bucket").parquet(target)
    write_s = time.perf_counter() - t0

    back = spark.read.parquet(target)
    read_count = back.count()
    agg = back.groupBy("bucket").agg(F.count("*").alias("n")).orderBy("bucket")
    print("[smoke] physical plan of the group-by (shows Exchange = shuffle):")
    agg.explain(mode="formatted")
    rows = [r.asDict() for r in agg.collect()]

    executors = sc._jsc.sc().getExecutorMemoryStatus().size() - 1  # minus the driver
    result = {
        "target": target,
        "rows_written": N_ROWS,
        "rows_read_back": read_count,
        "per_bucket": rows,
        "write_seconds": round(write_s, 3),
        "executors": executors,
        "ok": read_count == N_ROWS,
    }
    print("[smoke] RESULT " + json.dumps(result))
    spark.stop()
    return 0 if result["ok"] else 1


if __name__ == "__main__":
    sys.exit(main())
