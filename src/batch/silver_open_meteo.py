"""Spark job: Bronze Open-Meteo records -> Silver `open_meteo_hourly` v1 (spec T03, T05).

Input   hdfs:///datalake/bronze/open_meteo/records/ingest_date=D/  (latest D unless --ingest-date)
        hdfs:///datalake/bronze/open_meteo/raw/ingest_date=D/*.json (hourly_units, timezone, offset)
Output  hdfs:///datalake/silver/open_meteo_hourly/v1/outcome=.../year=YYYY/
        --report-out FILE (JSON), --explain-out FILE (formatted plan)

Every record is labelled model=era5, data_kind=reanalysis (spec §4: not a station observation) and
carries the "valid time" of its variables: instantaneous at time_utc, or a sum over the preceding
hour [time_utc - 1h, time_utc). Column names carry the units returned by the API.

Run: spark-submit /workspace/src/batch/silver_open_meteo.py --report-out R.json --explain-out P.txt
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import sys
from pathlib import Path
from typing import Any, Mapping

import yaml
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.batch.silver_uci import load_station
from src.common.config import PROJECT_ROOT, load_config
from src.quality.open_meteo_rules import (VARIABLES, key_rules, request_window, row_rules,
                                           station_in_cell, units_check)
from src.quality.spark_rules import apply_rules, rule_counts

# Bronze variable -> Silver column (unit in the name; units verified against the API response)
COLUMNS = {"temperature_2m": "temperature_2m_c", "precipitation": "precipitation_mm", "rain": "rain_mm",
           "snowfall": "snowfall_cm", "cloud_cover": "cloud_cover_pct", "weather_code": "weather_code",
           "wind_speed_10m": "wind_speed_10m_kmh"}
PROVENANCE = ["_source", "_source_file", "_source_version", "_sha256", "_ingested_at"]
KEY = ["grid_latitude", "grid_longitude", "time_utc"]


def load_configs() -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    q = yaml.safe_load((PROJECT_ROOT / "configs" / "quality.yaml").read_text())
    src = yaml.safe_load((PROJECT_ROOT / "configs" / "sources.yaml").read_text())
    return q["open_meteo"], q["uci_metro"], src


def type_rows(bronze: DataFrame, cfg: Mapping[str, Any], start: str, end: str) -> DataFrame:
    ts = F.to_timestamp(F.trim("time"), cfg["time_format"])     # session tz is UTC; API sent GMT
    df = bronze.withColumn("time_utc", F.when(ts.isNotNull() & (F.minute(ts) == 0), ts))
    df = df.withColumn("_raw", F.struct("time", *VARIABLES))
    return apply_rules(df, row_rules(cfg, start, end))


def collapse_keys(rows: DataFrame, cfg: Mapping[str, Any]) -> DataFrame:
    keep = F.col("_outcome") != "rejected"
    values = F.struct(*VARIABLES)
    aggs = [F.count("*").alias("source_rows"), F.countDistinct(values).alias("n_distinct_values"),
            F.array_distinct(F.flatten(F.collect_list("_violations"))).alias("_violations"),
            F.collect_list("_raw").alias("_raw_rows"), F.first("elevation").alias("elevation"),
            *[F.first(v).alias(v) for v in VARIABLES], *[F.first(c).alias(c) for c in PROVENANCE + ["ingest_date"]]]
    keys = rows.where(keep).groupBy(*KEY).agg(*aggs)
    for v in VARIABLES:   # conflicting duplicates: no value is chosen
        keys = keys.withColumn(v, F.when(F.col("n_distinct_values") <= 1, F.col(v)))
    keys = apply_rules(keys, key_rules(cfg), existing="_violations")
    rej = (rows.where(~keep).withColumn("source_rows", F.lit(1).cast("long"))
               .withColumn("n_distinct_values", F.lit(None).cast("long"))
               .withColumn("_raw_rows", F.array("_raw")))
    cols = [*KEY, "elevation", "source_rows", "n_distinct_values", *VARIABLES, "_violations", "_outcome",
            "_raw_rows", *PROVENANCE, "ingest_date"]
    return keys.select(*cols).unionByName(rej.select(*cols))


def finalize(keys: DataFrame, cfg: Mapping[str, Any], run_id: str) -> DataFrame:
    df = keys
    for v, col in COLUMNS.items():
        df = df.withColumnRenamed(v, col)
    df = (df.withColumn("weather_code", F.col("weather_code").cast("int"))
            .withColumn("cloud_cover_pct", F.col("cloud_cover_pct").cast("int"))
            # interval covered by the preceding-hour sums
            .withColumn("sum_interval_start_utc", F.col("time_utc") - F.expr("INTERVAL 1 HOUR"))
            .withColumn("model", F.lit(cfg["model"]))
            .withColumn("data_kind", F.lit(cfg["data_kind"]))
            .withColumn("year", F.year("time_utc"))
            .withColumn("_bronze_ingest_date", F.col("ingest_date").cast("string"))
            .withColumn("_silver_run_id", F.lit(run_id))
            .withColumn("_silver_version", F.lit(cfg["version"]))
            .withColumnRenamed("_outcome", "outcome")
            .withColumnRenamed("elevation", "elevation_m"))
    return df.select("grid_latitude", "grid_longitude", "elevation_m", "model", "data_kind",
                     "time_utc", "sum_interval_start_utc", "year", *COLUMNS.values(), "source_rows",
                     "_violations", "outcome", "_raw_rows", *PROVENANCE, "_bronze_ingest_date",
                     "_silver_run_id", "_silver_version")


def build_silver(bronze: DataFrame, cfg: Mapping[str, Any], start: str, end: str,
                 run_id: str) -> tuple[DataFrame, DataFrame]:
    rows = type_rows(bronze, cfg, start, end)
    return rows, finalize(collapse_keys(rows, cfg), cfg, run_id)


# ---------- report (dataset-level checks are in src/quality/open_meteo_rules.py) ----------
def report(bronze: DataFrame, rows: DataFrame, silver: DataFrame, cfg: Mapping[str, Any], start: str,
           end: str, units: Mapping[str, Any], cell: Mapping[str, Any]) -> dict[str, Any]:
    bronze_rows = bronze.count()
    by_outcome = {r["outcome"]: {"records": int(r["records"]), "source_rows": int(r["source_rows"])}
                  for r in silver.groupBy("outcome").agg(F.count("*").alias("records"),
                                                          F.sum("source_rows").alias("source_rows")).collect()}
    src_total = sum(v["source_rows"] for v in by_outcome.values())
    kept = silver.where("outcome <> 'rejected'")
    span = kept.agg(F.countDistinct("time_utc").alias("hours"), F.min("time_utc").alias("lo"),
                    F.max("time_utc").alias("hi")).first()
    expected = int((dt.datetime.fromisoformat(end) - dt.datetime.fromisoformat(start)).total_seconds() // 3600) + 1
    grids = [list(r) for r in kept.select("grid_latitude", "grid_longitude").distinct().collect()]
    dup = kept.where("source_rows > 1").agg(F.count("*").alias("k"), F.sum(F.col("source_rows") - 1).alias("x")).first()
    checks = [
        {"name": "units_timezone_match_api_response", "ok": units["ok"],
         "detail": f"{units['files_checked']} raw files checked; not matching: {units['files_not_matching']}"},
        {"name": "complete_requested_period", "ok": int(span["hours"]) == expected,
         "detail": f"{int(span['hours']):,} of {expected:,} requested hours present ({start} .. {end} UTC)"},
        {"name": "single_grid_point", "ok": len(grids) == 1, "detail": f"grid points: {grids}"},
        {"name": "station_inside_era5_cell", "ok": cell["station_inside_cell"],
         "detail": f"ATR 301 is {cell['distance_km']} km from grid point {cell['grid_point']}; "
                   f"inside the ±{cell['cell_half_width_deg']}° cell: {cell['station_inside_cell']}"},
    ]
    return {
        "dataset": cfg["silver_dataset"], "version": cfg["version"],
        "bronze_ingest_date": bronze.select(F.max("ingest_date")).first()[0].isoformat(),
        "reconciliation": {"bronze_rows": bronze_rows, "silver_source_rows": src_total,
                           "reconciled": bronze_rows == src_total},
        "outcomes": by_outcome,
        "row_rule_violations": rule_counts(rows),
        "record_rule_violations": rule_counts(silver.withColumnRenamed("outcome", "_outcome")),
        "duplicates": {"keys_with_several_rows": int(dup["k"] or 0), "extra_rows_collapsed": int(dup["x"] or 0)},
        "coverage": {"time_basis": "UTC", "first_hour": str(span["lo"]), "last_hour": str(span["hi"]),
                     "distinct_hours": int(span["hours"]), "hours_in_span": expected,
                     "absent_hours": expected - int(span["hours"])},
        "checks": checks, "units": units, "station_cell": cell,
        "labels": {"model": cfg["model"], "data_kind": cfg["data_kind"], "valid_time": cfg["valid_time"]},
    }


def samples(silver: DataFrame) -> list[dict[str, Any]]:
    picks = []
    for cond in ["outcome = 'accepted' AND precipitation_mm > 0", "outcome = 'accepted' AND snowfall_cm > 0",
                 "outcome = 'quarantine'", "outcome = 'rejected'"]:
        r = silver.where(cond).orderBy("time_utc").limit(1).collect()
        if r:
            d = r[0].asDict(recursive=True)
            picks.append({"case": cond, "bronze_rows": d.pop("_raw_rows"),
                          "silver": {k: (v.isoformat() if hasattr(v, "isoformat") else v) for k, v in d.items()}})
    return picks


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--ingest-date")
    ap.add_argument("--run-id", default=dt.datetime.now(dt.timezone.utc).strftime("m2_%Y%m%dT%H%M%SZ"))
    ap.add_argument("--report-out", required=True)
    ap.add_argument("--explain-out", required=True)
    args = ap.parse_args()

    spark = SparkSession.builder.appName("bdfp-m2-silver-open_meteo").getOrCreate()
    platform = load_config()
    cfg, uci_cfg, sources = load_configs()
    start, end = request_window(sources)
    root = f"{platform['hdfs']['uri']}{platform['hdfs']['datalake_root']}"
    bronze_all = spark.read.parquet(f"{root}/bronze/open_meteo/records")
    d = args.ingest_date or bronze_all.select(F.max("ingest_date")).first()[0].isoformat()
    bronze = bronze_all.where(F.col("ingest_date") == F.lit(d)).cache()

    raw = (spark.read.option("multiLine", True).json(f"{root}/bronze/open_meteo/raw/ingest_date={d}/open_meteo_*.json")
           .select(F.element_at(F.split(F.input_file_name(), "/"), -1).alias("file"), "hourly_units",
                   "timezone", "utc_offset_seconds", "latitude", "longitude"))
    raw_meta = [r.asDict(recursive=True) for r in raw.collect()]
    units = units_check(raw_meta, cfg)
    grid = (raw_meta[0]["latitude"], raw_meta[0]["longitude"])
    cell = station_in_cell(grid, load_station(uci_cfg), float(cfg["grid_resolution_deg"]))

    rows, silver = build_silver(bronze, cfg, start, end, args.run_id)
    rows.cache()
    out = f"{root}/silver/{cfg['silver_dataset']}/{cfg['version']}"
    Path(args.explain_out).write_text(
        spark._jvm.PythonSQLUtils.explainString(silver._jdf.queryExecution(), "formatted"), encoding="utf-8")
    silver.repartition("outcome", "year").write.mode("overwrite").partitionBy("outcome", "year").parquet(out)

    back = spark.read.parquet(out)
    rep = report(bronze, rows, back, cfg, start, end, units, cell)
    rep.update({"input": f"{root}/bronze/open_meteo/records/ingest_date={d}", "output": out, "run_id": args.run_id,
                "spark_app_id": spark.sparkContext.applicationId, "schema": back.schema.simpleString(),
                "samples": samples(back)})
    Path(args.report_out).write_text(json.dumps(rep, indent=2, default=str), encoding="utf-8")
    print(f"report -> {args.report_out}; reconciled={rep['reconciliation']}", flush=True)
    spark.stop()
    ok = rep["reconciliation"]["reconciled"] and units["ok"]
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
