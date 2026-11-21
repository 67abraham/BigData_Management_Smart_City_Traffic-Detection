"""Spark job: Bronze UCI Metro records -> Silver `uci_traffic_hourly` v1 (spec T03, T05).

Input   hdfs:///datalake/bronze/uci_metro/records/ingest_date=D/   (latest D unless --ingest-date)
Output  hdfs:///datalake/silver/uci_traffic_hourly/v1/outcome={accepted,quarantine,rejected}/year=YYYY/
        --report-out FILE   quality + reconciliation report (JSON)
        --explain-out FILE  formatted physical plan of the Silver query
        (written to files, not stdout: Spark log lines can interleave with long stdout lines)

Steps
 1. type: Bronze strings -> numbers; date_time -> local timestamp (timestamp_ntz, on the hour)
 2. row rules (src/quality/uci_rules.row_rules): reject / quarantine / flag per source row
 3. collapse: UCI repeats an hour once per reported weather condition (M1). Rows that were not
    rejected are grouped by local hour; traffic_volume must agree (else quarantine), weather text
    becomes a sorted set, a numeric weather field that disagrees becomes NULL (flagged).
 4. time: local -> UTC with the configured zone (America/Chicago); the fall-back hour is ambiguous
    and quarantined, its UTC hour left NULL. Holiday names (UCI sets them only on the 00:00 row)
    are applied to every hour of that local date.
 5. write Parquet partitioned by outcome and local year; read back and reconcile:
    sum(source_rows) over all outcomes == Bronze rows.

Run: spark-submit /workspace/src/batch/silver_uci.py --report-out R.json --explain-out P.txt
       [--ingest-date YYYY-MM-DD] [--run-id ID]
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import sys
from pathlib import Path
from typing import Any, Mapping

import yaml
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from src.common.config import PROJECT_ROOT, load_config
from src.quality.rules import dst_anomalies
from src.quality.spark_rules import apply_rules, outcome_counts, rule_counts
from src.quality.uci_rules import hour_rules, row_rules

RAW_COLUMNS = ["holiday", "temp", "rain_1h", "snow_1h", "clouds_all", "weather_main",
               "weather_description", "date_time", "traffic_volume"]
NUMERIC = {"temp": "temp_k", "rain_1h": "rain_1h_mm", "snow_1h": "snow_1h_mm", "clouds_all": "clouds_all_pct"}
PROVENANCE = ["_source", "_source_file", "_source_version", "_sha256", "_ingested_at"]


def load_quality_config() -> dict[str, Any]:
    return yaml.safe_load((PROJECT_ROOT / "configs" / "quality.yaml").read_text())["uci_metro"]


def load_station(cfg: Mapping[str, Any]) -> dict[str, Any]:
    """Station facts: id/road/direction from config, coordinates from the MnDOT lookup (M1)."""
    loc = json.loads((PROJECT_ROOT / "data" / "manifests" / "atr301_location.json").read_text())
    chosen = loc.get("chosen") or {}
    if str(loc.get("station_id")) != str(cfg["station"]["id"]) or "latitude" not in chosen:
        raise SystemExit("data/manifests/atr301_location.json has no chosen ATR 301 location")
    return {**cfg["station"], "latitude": float(chosen["latitude"]), "longitude": float(chosen["longitude"]),
            "location_source": loc.get("source"), "location_retrieved_at": loc.get("retrieved_at")}


# ---------- 1-2: type and row rules ----------
def type_rows(bronze: DataFrame, cfg: Mapping[str, Any]) -> DataFrame:
    years = range(int(cfg["coverage_start_local"][:4]) - 1, int(cfg["coverage_end_local"][:4]) + 2)
    nonexistent, ambiguous = dst_anomalies(cfg["timezone"], years[0], years[-1])
    ts = F.expr(f"to_timestamp_ntz(trim(date_time), '{cfg['timestamp_format']}')")
    on_hour = (F.minute(ts) == 0) & (F.second(ts) == 0)
    df = bronze.withColumn("hour_start_local", F.when(ts.isNotNull() & on_hour, ts))
    local_str = F.date_format("hour_start_local", "yyyy-MM-dd HH:mm:ss")
    df = (df.withColumn("is_dst_nonexistent", local_str.isin(nonexistent))
            .withColumn("is_dst_ambiguous", local_str.isin(ambiguous))
            .withColumn("volume_num", F.expr("try_cast(trim(traffic_volume) AS DOUBLE)")))
    for raw, typed in NUMERIC.items():
        df = df.withColumn(typed, F.expr(f"try_cast(trim({raw}) AS DOUBLE)"))
    df = df.withColumn("_raw", F.struct(*[F.col(c) for c in RAW_COLUMNS + ["_corrupt_record"]]))
    return apply_rules(df, row_rules(cfg))


# ---------- 3: collapse to one record per local hour ----------
def _text_set(col: str) -> Any:
    return F.array_sort(F.collect_set(F.when(F.trim(F.col(col)) != "", F.trim(F.col(col)))))


def collapse_hours(rows: DataFrame, cfg: Mapping[str, Any]) -> DataFrame:
    keep = (F.col("_outcome") != "rejected") & F.col("hour_start_local").isNotNull()
    good = rows.where(keep)
    aggs = [F.count("*").alias("source_rows"),
            F.countDistinct("volume_num").alias("n_distinct_volume"),
            F.max("volume_num").alias("_vol_max"),
            F.max("is_dst_ambiguous").alias("dst_ambiguous"),
            _text_set("weather_main").alias("weather_main"),
            _text_set("weather_description").alias("weather_description"),
            F.max(F.when(F.trim("holiday").isin("", "None") | F.col("holiday").isNull(), None)
                  .otherwise(F.trim("holiday"))).alias("holiday_name_row"),
            F.array_distinct(F.flatten(F.collect_list("_violations"))).alias("_violations"),
            F.collect_list("_raw").alias("_raw_rows"),
            *[F.first(c).alias(c) for c in PROVENANCE + ["ingest_date"]]]
    for typed in NUMERIC.values():
        aggs += [F.countDistinct(typed).alias(f"_nd_{typed}"), F.max(typed).alias(f"_v_{typed}")]
    hours = good.groupBy("hour_start_local").agg(*aggs)
    conflict = F.lit(False)
    for typed in NUMERIC.values():
        hours = hours.withColumn(typed, F.when(F.col(f"_nd_{typed}") <= 1, F.col(f"_v_{typed}")))
        conflict = conflict | (F.col(f"_nd_{typed}") > 1)
    hours = (hours.withColumn("weather_numeric_conflict", conflict)
                  .withColumn("traffic_volume",
                              F.when(F.col("n_distinct_volume") == 1, F.col("_vol_max").cast("int"))))
    hours = apply_rules(hours, hour_rules(cfg), existing="_violations")

    # rejected rows stay one record per source row, so nothing is lost silently
    rej = (rows.where(~keep)
               .withColumn("source_rows", F.lit(1).cast("long"))
               .withColumn("n_distinct_volume", F.lit(None).cast("long"))
               .withColumn("dst_ambiguous", F.col("is_dst_ambiguous"))
               .withColumn("weather_main", F.array_remove(F.array(F.trim("weather_main")), ""))
               .withColumn("weather_description", F.array_remove(F.array(F.trim("weather_description")), ""))
               .withColumn("holiday_name_row", F.when(~F.trim("holiday").isin("", "None"), F.trim("holiday")))
               .withColumn("_raw_rows", F.array("_raw"))
               .withColumn("weather_numeric_conflict", F.lit(False))
               .withColumn("traffic_volume", F.when(F.col("volume_num") == F.floor("volume_num"),
                                                     F.col("volume_num").cast("int")))
               .withColumn("_outcome", F.lit("rejected")))
    cols = ["hour_start_local", "source_rows", "n_distinct_volume", "dst_ambiguous", "traffic_volume",
            *NUMERIC.values(), "weather_main", "weather_description", "holiday_name_row",
            "weather_numeric_conflict", "_violations", "_outcome", "_raw_rows", *PROVENANCE, "ingest_date"]
    return hours.select(*cols).unionByName(rej.select(*cols))


# ---------- 4: time semantics, units, station, lineage ----------
def finalize(hours: DataFrame, cfg: Mapping[str, Any], station: Mapping[str, Any], run_id: str) -> DataFrame:
    tz = cfg["timezone"]
    local_ts = F.col("hour_start_local").cast("timestamp")   # same wall clock (session tz is UTC)
    utc = F.when(~F.coalesce(F.col("dst_ambiguous"), F.lit(False)), F.to_utc_timestamp(local_ts, tz))
    by_date = Window.partitionBy("local_date")
    return (hours
            .withColumn("hour_start_utc", utc)
            .withColumn("utc_offset_hours",
                        ((F.unix_timestamp(local_ts) - F.unix_timestamp("hour_start_utc")) / 3600).cast("int"))
            .withColumn("timezone", F.lit(tz))
            .withColumn("local_date", F.to_date("hour_start_local"))
            .withColumn("year", F.year("hour_start_local"))
            .withColumn("holiday_name",
                        F.when(F.col("local_date").isNotNull(), F.max("holiday_name_row").over(by_date)))
            .withColumn("is_holiday_date", F.col("holiday_name").isNotNull())
            .withColumn("temp_c", F.round(F.col("temp_k") - 273.15, 2))
            .withColumn("traffic_volume_unit", F.lit(cfg["units"]["traffic_volume"]))
            .withColumn("station_id", F.lit(station["id"]))
            .withColumn("road", F.lit(station["road"]))
            .withColumn("direction", F.lit(station["direction"]))
            .withColumn("latitude", F.lit(station["latitude"]))
            .withColumn("longitude", F.lit(station["longitude"]))
            .withColumn("_bronze_ingest_date", F.col("ingest_date").cast("string"))
            .withColumn("_silver_run_id", F.lit(run_id))
            .withColumn("_silver_version", F.lit(cfg["version"]))
            .withColumnRenamed("_outcome", "outcome")
            .select("station_id", "road", "direction", "latitude", "longitude",
                    "hour_start_local", "hour_start_utc", "timezone", "utc_offset_hours", "dst_ambiguous",
                    "local_date", "year", "traffic_volume", "traffic_volume_unit",
                    "temp_k", "temp_c", "rain_1h_mm", "snow_1h_mm", "clouds_all_pct",
                    "weather_main", "weather_description", "holiday_name", "is_holiday_date",
                    "source_rows", "weather_numeric_conflict", "_violations", "outcome", "_raw_rows",
                    *PROVENANCE, "_bronze_ingest_date", "_silver_run_id", "_silver_version"))


def build_silver(bronze: DataFrame, cfg: Mapping[str, Any], station: Mapping[str, Any],
                 run_id: str) -> tuple[DataFrame, DataFrame]:
    """Return (typed source rows with row-level violations, Silver records)."""
    rows = type_rows(bronze, cfg)
    return rows, finalize(collapse_hours(rows, cfg), cfg, station, run_id)


# ---------- 5: report ----------
def report(bronze: DataFrame, rows: DataFrame, silver: DataFrame, cfg: Mapping[str, Any]) -> dict[str, Any]:
    bronze_rows = bronze.count()
    by_outcome = {r["outcome"]: {"records": int(r["records"]), "source_rows": int(r["source_rows"])}
                  for r in silver.groupBy("outcome").agg(F.count("*").alias("records"),
                                                          F.sum("source_rows").alias("source_rows")).collect()}
    source_rows_total = sum(v["source_rows"] for v in by_outcome.values())
    kept = silver.where("outcome <> 'rejected'")
    span = kept.agg(F.min("hour_start_local").alias("lo"), F.max("hour_start_local").alias("hi"),
                    F.countDistinct("hour_start_local").alias("hours")).first()
    expected_hours = int((span["hi"] - span["lo"]).total_seconds() // 3600) + 1 if span["lo"] else 0
    files = {r["_source_file"]: int(r["count"]) for r in bronze.groupBy("_source_file").count().collect()}
    expected_files = list(cfg["expected_source_files"])
    dup = kept.where("source_rows > 1").agg(F.count("*").alias("hours"),
                                            F.sum(F.col("source_rows") - 1).alias("extra_rows")).first()
    return {
        "dataset": cfg["silver_dataset"], "version": cfg["version"],
        "bronze_ingest_date": bronze.select(F.max("ingest_date")).first()[0].isoformat(),
        "reconciliation": {"bronze_rows": bronze_rows, "silver_source_rows": source_rows_total,
                           "reconciled": bronze_rows == source_rows_total},
        "outcomes": by_outcome,
        "row_rule_violations": rule_counts(rows),        # counted on source rows
        "record_rule_violations": rule_counts(silver.withColumnRenamed("outcome", "_outcome")),
        "duplicates": {"hours_with_several_rows": int(dup["hours"] or 0),
                       "extra_rows_collapsed": int(dup["extra_rows"] or 0)},
        "coverage": {"first_hour_local": str(span["lo"]), "last_hour_local": str(span["hi"]),
                     "distinct_hours": int(span["hours"]), "hours_in_span": expected_hours,
                     "absent_hours": expected_hours - int(span["hours"])},
        "completeness": {"expected_files": expected_files, "files_in_bronze": files,
                         "all_expected_files_present": all(files.get(f, 0) > 0 for f in expected_files)},
        "dst": {"ambiguous_hours_in_data": kept.where("dst_ambiguous").count(),
                "utc_offsets": {str(r["utc_offset_hours"]): int(r["count"])
                                for r in kept.groupBy("utc_offset_hours").count().collect()}},
        "holidays": {"dates": silver.where("is_holiday_date").select("local_date").distinct().count(),
                     "hours": silver.where("is_holiday_date").count()},
    }


def samples(silver: DataFrame) -> list[dict[str, Any]]:
    """Before/after examples (AC03): Bronze source rows and the Silver record they became."""
    picks = []
    for cond in ["outcome = 'accepted' AND source_rows = 1",
                 "outcome = 'accepted' AND source_rows > 2",
                 "outcome = 'quarantine'",
                 "outcome = 'rejected'"]:
        r = silver.where(cond).orderBy("hour_start_local").limit(1).collect()
        if r:
            d = r[0].asDict(recursive=True)
            picks.append({"case": cond, "bronze_rows": d.pop("_raw_rows"),
                          "silver": {k: (v.isoformat() if hasattr(v, "isoformat") else v)
                                     for k, v in d.items()}})
    return picks


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--ingest-date", help="Bronze partition to read (default: latest)")
    ap.add_argument("--run-id", default=dt.datetime.now(dt.timezone.utc).strftime("m2_%Y%m%dT%H%M%SZ"))
    ap.add_argument("--report-out", required=True, help="path for the JSON report (driver-local)")
    ap.add_argument("--explain-out", required=True, help="path for the formatted plan (driver-local)")
    args = ap.parse_args()

    spark = SparkSession.builder.appName("bdfp-m2-silver-uci_metro").getOrCreate()
    platform, cfg = load_config(), load_quality_config()
    station = load_station(cfg)
    root = f"{platform['hdfs']['uri']}{platform['hdfs']['datalake_root']}"
    bronze_all = spark.read.parquet(f"{root}/bronze/uci_metro/records")
    d = args.ingest_date or bronze_all.select(F.max("ingest_date")).first()[0].isoformat()
    bronze = bronze_all.where(F.col("ingest_date") == F.lit(d)).cache()

    rows, silver = build_silver(bronze, cfg, station, args.run_id)
    rows.cache()
    out = f"{root}/silver/{cfg['silver_dataset']}/{cfg['version']}"
    plan = spark._jvm.PythonSQLUtils.explainString(silver._jdf.queryExecution(), "formatted")
    Path(args.explain_out).write_text(plan, encoding="utf-8")
    (silver.repartition("outcome", "year")          # one file per partition, no small-file spray
           .write.mode("overwrite").partitionBy("outcome", "year").parquet(out))

    back = spark.read.parquet(out)          # read-back: report on what was actually written
    rep = report(bronze, rows, back, cfg)
    rep.update({"input": f"{root}/bronze/uci_metro/records/ingest_date={d}", "output": out,
                "run_id": args.run_id, "spark_app_id": spark.sparkContext.applicationId,
                "station": station, "schema": back.schema.simpleString(),
                "samples": samples(back)})
    Path(args.report_out).write_text(json.dumps(rep, indent=2, default=str), encoding="utf-8")
    print(f"report -> {args.report_out}; reconciled={rep['reconciliation']}", flush=True)
    spark.stop()
    return 0 if rep["reconciliation"]["reconciled"] and rep["completeness"]["all_expected_files_present"] else 1


if __name__ == "__main__":
    sys.exit(main())
