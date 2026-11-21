"""Spark job: Bronze NYC TLC Yellow trips -> Silver `nyc_yellow_trips` v1 (spec T03, T05).

Input   /datalake/bronze/nyc_tlc/records/ingest_date=D/source_month=YYYY-MM/  (one per month)
        /datalake/bronze/nyc_tlc/zone_lookup/ingest_date=D/
Output  /datalake/silver/nyc_yellow_trips/v1/outcome=.../pickup_date=YYYY-MM-DD/
        --report-out FILE (JSON), --explain-out FILE (formatted plan)

Steps
 1. Each month is read on its own (M1: schemas differ, e.g. Airport_fee vs airport_fee, int vs
    bigint IDs, double vs bigint passenger_count) and mapped onto one Silver schema with
    src/quality/nyc_tlc_rules.unify_plan; the type changes per month go into the report.
 2. Derive trip_minutes, pickup month/date/hour; join the taxi zone lookup for pickup and drop-off
    (broadcast: 265 rows) - a valid join on TLC's own LocationID key.
 3. Duplicate detection on the natural key (no trip id exists), then the rules. 1 Bronze row ->
    1 Silver row (nothing is collapsed), so reconciliation is a per-month row count check.
Times are NYC local wall-clock (timestamp_ntz) and are not converted (see configs/quality.yaml).

Run: spark-submit /workspace/src/batch/silver_nyc_tlc.py --report-out R.json --explain-out P.txt
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
from src.quality.nyc_tlc_rules import row_rules, unify_plan
from src.quality.spark_rules import apply_rules, rule_counts

PROVENANCE = ["_source", "_source_file", "_source_version", "_sha256", "_ingested_at"]


def load_configs() -> tuple[dict[str, Any], dict[str, Any]]:
    q = yaml.safe_load((PROJECT_ROOT / "configs" / "quality.yaml").read_text())
    src = yaml.safe_load((PROJECT_ROOT / "configs" / "sources.yaml").read_text())
    return q["nyc_tlc"], src["nyc_tlc"]


def unify_month(df: DataFrame, month: str, cfg: Mapping[str, Any]) -> tuple[DataFrame, dict[str, Any]]:
    plan = unify_plan([(f.name, f.dataType.simpleString()) for f in df.schema.fields], cfg)
    if plan["missing"]:
        raise ValueError(f"{month}: Bronze lacks columns {plan['missing']}")
    by_silver = {s: b for b, s, _, _ in plan["mapping"]}
    out = df.select(*[F.col(b).cast(to).alias(s) for b, s, _, to in plan["mapping"]],
                    F.col(by_silver["passenger_count"]).cast("double").alias("passenger_count_src"),
                    F.col(by_silver["ratecode_id"]).cast("double").alias("ratecode_id_src"),
                    *PROVENANCE, F.lit(month).alias("source_month"))
    return out, plan


def zones_df(zones: DataFrame, prefix: str) -> DataFrame:
    return zones.select(F.col("LocationID").cast("int").alias(f"{prefix}_location_id"),
                        F.col("Borough").alias(f"{prefix}_borough"), F.col("Zone").alias(f"{prefix}_zone"),
                        F.col("service_zone").alias(f"{prefix}_service_zone"))


def transform(trips: DataFrame, zones: DataFrame, cfg: Mapping[str, Any], run_id: str) -> DataFrame:
    pu, do = F.col("pickup_datetime_local"), F.col("dropoff_datetime_local")
    df = (trips
          .withColumn("trip_minutes", (F.unix_timestamp(do.cast("timestamp")) - F.unix_timestamp(pu.cast("timestamp"))) / 60.0)
          .withColumn("pickup_month", F.date_format(pu, "yyyy-MM"))
          .withColumn("pickup_date", F.to_date(pu))
          .withColumn("pickup_hour_local", F.hour(pu))
          .join(F.broadcast(zones_df(zones, "pu")), "pu_location_id", "left")
          .join(F.broadcast(zones_df(zones, "do")), "do_location_id", "left")
          .withColumn("dup_count", F.count(F.lit(1)).over(Window.partitionBy(*cfg["duplicate_key"]))))
    df = apply_rules(df, row_rules(cfg))
    return (df.withColumn("is_flex_fare", F.col("payment_type") == F.lit(int(cfg["flex_fare_payment_type"])))
              .withColumn("_silver_run_id", F.lit(run_id))
              .withColumn("_silver_version", F.lit(cfg["version"]))
              .withColumnRenamed("_outcome", "outcome")
              .drop("passenger_count_src", "ratecode_id_src", "dup_count", "pickup_month"))


def read_bronze(spark: SparkSession, root: str, ingest_date: str, months: list[str],
                cfg: Mapping[str, Any]) -> tuple[DataFrame, dict[str, Any], dict[str, int]]:
    parts, plans, counts = [], {}, {}
    for m in months:
        d = spark.read.parquet(f"{root}/bronze/nyc_tlc/records/ingest_date={ingest_date}/source_month={m}")
        counts[m] = d.count()
        u, plans[m] = unify_month(d, m, cfg)
        parts.append(u)
    trips = parts[0]
    for p in parts[1:]:
        trips = trips.unionByName(p)
    return trips, plans, counts


def report(back: DataFrame, bronze_counts: Mapping[str, int], plans: Mapping[str, Any], zones_n: int,
           cfg: Mapping[str, Any], months: list[str]) -> dict[str, Any]:
    per = {(r["source_month"], r["outcome"]): int(r["count"])
           for r in back.groupBy("source_month", "outcome").count().collect()}
    silver_by_month = {m: sum(v for (mm, _), v in per.items() if mm == m) for m in months}
    outcomes: dict[str, dict[str, int]] = {}
    for (_, o), v in per.items():
        outcomes.setdefault(o, {"records": 0, "source_rows": 0})
        outcomes[o]["records"] += v
        outcomes[o]["source_rows"] += v
    acc = back.where("outcome = 'accepted'")
    span = acc.agg(F.min("pickup_datetime_local").alias("lo"), F.max("pickup_datetime_local").alias("hi"),
                   F.countDistinct("pickup_date").alias("days")).first()
    bronze_total = sum(bronze_counts.values())
    type_changes = {m: [{"bronze": b, "silver": s, "from": f, "to": t} for b, s, f, t in p["mapping"]
                        if f != t or b != s] for m, p in plans.items()}
    checks = [
        {"name": "rows_reconciled_per_month", "ok": all(bronze_counts[m] == silver_by_month[m] for m in months),
         "detail": f"bronze {dict(bronze_counts)} vs silver {silver_by_month}"},
        {"name": "all_expected_months_present", "ok": all(bronze_counts.get(m, 0) > 0 for m in months),
         "detail": f"expected {months}"},
        {"name": "zone_lookup_complete", "ok": zones_n == int(cfg["expected_zone_count"]),
         "detail": f"{zones_n} zones (expected {cfg['expected_zone_count']})"},
        {"name": "no_unexpected_columns", "ok": all(not p["extra"] for p in plans.values()),
         "detail": f"extra columns per month: {({m: p['extra'] for m, p in plans.items()})}"},
    ]
    return {
        "dataset": cfg["silver_dataset"], "version": cfg["version"],
        "reconciliation": {"bronze_rows": bronze_total, "silver_source_rows": sum(silver_by_month.values()),
                           "reconciled": bronze_total == sum(silver_by_month.values()),
                           "per_month": {m: {"bronze": bronze_counts[m], "silver": silver_by_month[m]} for m in months}},
        "outcomes": outcomes,
        "outcomes_per_month": {m: {o: v for (mm, o), v in per.items() if mm == m} for m in months},
        "row_rule_violations": rule_counts(back.withColumnRenamed("outcome", "_outcome")),
        "record_rule_violations": rule_counts(back.withColumnRenamed("outcome", "_outcome")),
        "schema_unification": type_changes,
        "accepted_pickup_span": {"first": str(span["lo"]), "last": str(span["hi"]), "days": int(span["days"])},
        "checks": checks,
        "labels": {"model": "TLC TPEP trip records (provider-submitted)",
                   "data_kind": "trip records; not road-counter traffic volume (spec §4)",
                   "valid_time": cfg["time_semantics"]},
    }


def samples(back: DataFrame) -> list[dict[str, Any]]:
    picks = []
    for cond in ["outcome = 'accepted' AND NOT is_flex_fare AND size(_violations) = 0",
                 "outcome = 'accepted' AND is_flex_fare", "outcome = 'quarantine'", "outcome = 'rejected'"]:
        r = back.where(cond).limit(1).collect()
        if r:
            picks.append({"case": cond, "silver": {k: (v.isoformat() if hasattr(v, "isoformat") else v)
                                                   for k, v in r[0].asDict(recursive=True).items()}})
    return picks


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--ingest-date")
    ap.add_argument("--run-id", default=dt.datetime.now(dt.timezone.utc).strftime("m2_%Y%m%dT%H%M%SZ"))
    ap.add_argument("--report-out", required=True)
    ap.add_argument("--explain-out", required=True)
    args = ap.parse_args()

    spark = SparkSession.builder.appName("bdfp-m2-silver-nyc_tlc").getOrCreate()
    platform = load_config()
    cfg, src = load_configs()
    months = list(src["months"])
    root = f"{platform['hdfs']['uri']}{platform['hdfs']['datalake_root']}"
    d = args.ingest_date or (spark.read.parquet(f"{root}/bronze/nyc_tlc/zone_lookup")
                             .select(F.max("ingest_date")).first()[0].isoformat())
    trips, plans, bronze_counts = read_bronze(spark, root, d, months, cfg)
    zones = spark.read.parquet(f"{root}/bronze/nyc_tlc/zone_lookup/ingest_date={d}").cache()
    zones_n = zones.select("LocationID").distinct().count()

    silver = transform(trips, zones, cfg, args.run_id).withColumn("_bronze_ingest_date", F.lit(d))
    out = f"{root}/silver/{cfg['silver_dataset']}/{cfg['version']}"
    Path(args.explain_out).write_text(
        spark._jvm.PythonSQLUtils.explainString(silver._jdf.queryExecution(), "formatted"), encoding="utf-8")
    (silver.repartition("outcome", "pickup_date")
           .write.mode("overwrite").partitionBy("outcome", "pickup_date").parquet(out))

    back = spark.read.parquet(out)
    rep = report(back, bronze_counts, plans, zones_n, cfg, months)
    rep.update({"input": f"{root}/bronze/nyc_tlc/records/ingest_date={d}", "output": out, "run_id": args.run_id,
                "bronze_ingest_date": d, "spark_app_id": spark.sparkContext.applicationId,
                "schema": back.schema.simpleString(), "samples": samples(back)})
    Path(args.report_out).write_text(json.dumps(rep, indent=2, default=str), encoding="utf-8")
    print(f"report -> {args.report_out}; reconciled={rep['reconciliation']}", flush=True)
    spark.stop()
    return 0 if rep["reconciliation"]["reconciled"] and all(c["ok"] for c in rep["checks"]) else 1


if __name__ == "__main__":
    sys.exit(main())
