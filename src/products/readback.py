"""Spark job: read every data product back and check it against its contract (spec T07 read-back test).

Per product: schema (names and types) = contract fields; row count = the build report; primary key
unique; non-nullable fields have no NULLs; every contract quality rule holds on every row. For
traffic_features also a leakage check: lag_1h / lag_24h recomputed from traffic_hourly must match.
Exit code 0 only if every check of every product passes.

Run: spark-submit /workspace/src/products/readback.py --build-report B.json --report-out R.json
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import sys
from pathlib import Path
from typing import Any, Mapping

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.products import contracts


def check_product(spark: SparkSession, c: Mapping[str, Any], built: Mapping[str, Any]) -> dict[str, Any]:
    df = spark.read.parquet(c["location"]["path"])
    checks: list[dict[str, Any]] = []

    def add(name: str, ok: bool, detail: Any) -> None:
        checks.append({"check": name, "passed": bool(ok), "detail": detail})

    got = {f.name: f.dataType.simpleString() for f in df.schema.fields}
    want = {f["name"]: f["type"] for f in c["fields"]}
    add("schema_matches_contract", got == want,
        {"missing": sorted(set(want) - set(got)), "extra": sorted(set(got) - set(want)),
         "type_mismatch": {k: [got[k], want[k]] for k in set(got) & set(want) if got[k] != want[k]}})
    n = df.count()
    add("row_count_matches_build", n == built["row_count"], {"read": n, "built": built["row_count"]})
    pk = c["primary_key"]
    distinct = df.select(*pk).distinct().count()
    add("primary_key_unique", distinct == n, {"key": pk, "distinct": distinct, "rows": n})
    required = [f["name"] for f in c["fields"] if not f["nullable"] and f["name"] in got]
    nulls = df.select(*[F.sum(F.col(k).isNull().cast("int")).alias(k) for k in required]).first().asDict()
    bad_nulls = {k: int(v) for k, v in nulls.items() if v}
    add("required_fields_not_null", not bad_nulls, bad_nulls)
    for r in c["quality_rules"]:
        broken = df.where(f"NOT ({r['rule']}) OR ({r['rule']}) IS NULL").count()
        add(f"rule:{r['name']}", broken == 0, {"rule": r["rule"], "rows_breaking": broken})
    if c["name"] == "traffic_features":
        add(*leakage_check(spark, df))
    sample = [{k: (v.isoformat() if hasattr(v, "isoformat") else v) for k, v in row.asDict().items()}
              for row in df.orderBy(*pk).limit(2).collect()]
    return {"product": f"{c['name']}_{c['version']}", "location": c["location"]["path"], "rows": n,
            "passed": all(x["passed"] for x in checks), "checks": checks, "sample_rows": sample}


def leakage_check(spark: SparkSession, feats: DataFrame) -> tuple[str, bool, Any]:
    """Recompute lag_1h and lag_24h from traffic_hourly (strictly earlier hours) and compare."""
    hourly = spark.read.parquet(contracts.load("traffic_hourly")["location"]["path"])
    res = {}
    for k in (1, 24):
        prev = hourly.select("station_id", (F.col("hour_start_utc") + F.expr(f"INTERVAL {k} HOURS")).alias("hour_start_utc"),
                             F.col("traffic_volume").alias("expected"))
        j = feats.select("station_id", "hour_start_utc", F.col(f"lag_{k}h").alias("got")).join(
            prev, ["station_id", "hour_start_utc"], "left")
        res[f"lag_{k}h_mismatches"] = j.where(~F.col("got").eqNullSafe(F.col("expected"))).count()
    return "leakage:lags_equal_earlier_hours", all(v == 0 for v in res.values()), res


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--build-report", required=True)
    ap.add_argument("--report-out", required=True)
    ap.add_argument("--run-id", default=dt.datetime.now(dt.timezone.utc).strftime("m4_%Y%m%dT%H%M%SZ"))
    args = ap.parse_args()
    built = json.loads(Path(args.build_report).read_text(encoding="utf-8"))
    spark = SparkSession.builder.appName("bdfp-m4-readback").getOrCreate()
    out = {}
    for name in contracts.PRODUCTS:
        c = contracts.load(name)
        key = f"{c['name']}"
        if key not in built:
            out[key] = {"passed": False, "checks": [], "error": "not in build report"}
            continue
        out[key] = check_product(spark, c, built[key])
        out[key].update({"run_id": args.run_id, "spark_app_id": spark.sparkContext.applicationId,
                         "checked_at": dt.datetime.now(dt.timezone.utc).isoformat()})
    Path(args.report_out).write_text(json.dumps(out, indent=2, default=str), encoding="utf-8")
    for k, v in out.items():
        print(k, "PASS" if v["passed"] else "FAIL", [c["check"] for c in v["checks"] if not c["passed"]], flush=True)
    spark.stop()
    return 0 if all(v["passed"] for v in out.values()) else 1


if __name__ == "__main__":
    sys.exit(main())
