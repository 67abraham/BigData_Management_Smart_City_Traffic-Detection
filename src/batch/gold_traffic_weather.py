"""Spark job: Silver UCI + Silver Open-Meteo -> Gold `uci_traffic_weather_hourly` v1 (spec T03 valid join).

Inputs  /datalake/silver/uci_traffic_hourly/v1/outcome=accepted
        /datalake/silver/open_meteo_hourly/v1/outcome=accepted
Output  /datalake/gold/uci_traffic_weather_hourly/v1/year=YYYY/
        --report-out FILE (JSON), --explain-out FILE (formatted plan)

Why the join is defensible (spec §4): same place (ATR 301 lies inside the single ERA5 grid cell; the
Open-Meteo Silver report checks this) and same period, joined on the UTC hour. Variables are aligned
by their valid time (src/quality/alignment.py): instant values at the UCI hour start, preceding-hour
sums one hour later, so both describe the same hour [U, U+1h).

It is a LEFT join from UCI: every accepted UCI hour appears exactly once; ERA5 columns are NULL where
no accepted weather row exists. UCI's own weather columns are kept unchanged next to the ERA5 ones;
they are a different source (not named on the UCI page) and are NOT replaced.

Run: spark-submit /workspace/src/batch/gold_traffic_weather.py --report-out R.json --explain-out P.txt
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

from src.batch.silver_open_meteo import COLUMNS as OM_COLUMNS
from src.common.config import PROJECT_ROOT, load_config
from src.quality.alignment import offset_hours, offsets_by_variable

UCI_DROP = ["_raw_rows", "_violations", "outcome"]


def load_configs() -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    q = yaml.safe_load((PROJECT_ROOT / "configs" / "quality.yaml").read_text())
    return q["traffic_weather"], q["open_meteo"], q["uci_metro"]


TIME_COLUMN = {"instant": "era5_instant_time_utc", "preceding_hour_sum": "era5_sum_end_time_utc"}


def weather_at_offset(om: DataFrame, variables: list[str], offset: int, kind: str) -> DataFrame:
    """Open-Meteo rows keyed by the UCI hour they describe: key = time_utc - offset hours.

    `kind` is the variables' valid time; the ERA5 timestamp used is kept as TIME_COLUMN[kind].
    """
    cols = [F.col(OM_COLUMNS[v]).alias(f"era5_{OM_COLUMNS[v]}") for v in variables]
    return om.select((F.col("time_utc") - F.expr(f"INTERVAL {offset} HOURS")).alias("_k"),
                     F.col("time_utc").alias(TIME_COLUMN[kind]), *cols)


def build_gold(uci: DataFrame, om: DataFrame, tw: Mapping[str, Any], om_cfg: Mapping[str, Any],
               run_id: str) -> DataFrame:
    out = uci.drop(*UCI_DROP)
    for offset, variables in offsets_by_variable(om_cfg["valid_time"], tw["uci_hour_label"]).items():
        kinds = {om_cfg["valid_time"][v] for v in variables}
        if len(kinds) != 1:
            raise ValueError(f"variables at offset {offset} mix valid times {kinds}")
        w = weather_at_offset(om, variables, offset, kinds.pop())
        out = out.join(w, out["hour_start_utc"] == w["_k"], "left").drop("_k")
    grid = om.select("grid_latitude", "grid_longitude").distinct()
    return (out.crossJoin(F.broadcast(grid.limit(1)))          # one grid point (checked in Open-Meteo Silver)
               .withColumnRenamed("grid_latitude", "era5_grid_latitude")
               .withColumnRenamed("grid_longitude", "era5_grid_longitude")
               .withColumn("era5_model", F.lit(om_cfg["model"]))
               .withColumn("era5_data_kind", F.lit(om_cfg["data_kind"]))
               .withColumn("uci_hour_label", F.lit(tw["uci_hour_label"]))
               .withColumn("_gold_run_id", F.lit(run_id))
               .withColumn("_gold_version", F.lit(tw["version"])))


def alignment_check(uci: DataFrame, om: DataFrame, offsets: list[int]) -> list[dict[str, Any]]:
    """Correlation of UCI temp/rain with ERA5 at several hour offsets (evidence for the label assumption)."""
    res = []
    for k in offsets:
        w = om.select((F.col("time_utc") - F.expr(f"INTERVAL {k} HOURS")).alias("_k"),
                      "temperature_2m_c", "precipitation_mm")
        j = uci.join(w, uci["hour_start_utc"] == w["_k"])
        r = j.agg(F.corr("temp_c", "temperature_2m_c").alias("t"),
                  F.corr("rain_1h_mm", "precipitation_mm").alias("p"), F.count("*").alias("n")).first()
        res.append({"era5_time_minus_uci_hour_h": k, "pairs": int(r["n"]),
                    "corr_temp_c": round(r["t"], 4) if r["t"] is not None else None,
                    "corr_rain_vs_precipitation": round(r["p"], 4) if r["p"] is not None else None})
    return res


def report(uci: DataFrame, gold: DataFrame, tw: Mapping[str, Any], om_cfg: Mapping[str, Any],
           om: DataFrame) -> dict[str, Any]:
    offs = offsets_by_variable(om_cfg["valid_time"], tw["uci_hour_label"])
    uci_rows, gold_rows = uci.count(), gold.count()
    distinct_hours = gold.select("hour_start_utc").distinct().count()
    inst = f"era5_{OM_COLUMNS[offs[min(offs)][0]]}"
    summ = f"era5_{OM_COLUMNS[offs[max(offs)][0]]}"
    m = gold.agg(F.sum(F.col(inst).isNotNull().cast("int")).alias("inst"),
                 F.sum(F.col(summ).isNotNull().cast("int")).alias("sum"),
                 F.sum((F.col(inst).isNotNull() & F.col(summ).isNotNull()).cast("int")).alias("both")).first()
    unmatched = [str(r[0]) for r in gold.where(F.col(inst).isNull() | F.col(summ).isNull())
                 .select("hour_start_utc").orderBy("hour_start_utc").limit(10).collect()]
    return {
        "dataset": tw["gold_dataset"], "version": tw["version"],
        "join": {"type": "left (UCI accepted hours) on UTC hour", "uci_hour_label": tw["uci_hour_label"],
                 "offsets_hours_by_variable": {str(k): v for k, v in offs.items()},
                 "example": {v: f"ERA5 at UCI hour + {offset_hours(om_cfg['valid_time'][v], tw['uci_hour_label'])} h"
                             for v in om_cfg["valid_time"]}},
        "reconciliation": {"uci_accepted_rows": uci_rows, "gold_rows": gold_rows,
                           "distinct_hours": distinct_hours,
                           "no_fan_out": uci_rows == gold_rows == distinct_hours},
        "matches": {"instant_matched": int(m["inst"]), "sum_matched": int(m["sum"]), "both_matched": int(m["both"]),
                    "unmatched_hours": uci_rows - int(m["both"]), "unmatched_examples_utc": unmatched},
        "alignment_check": alignment_check(uci, om, list(tw["alignment_check_offsets_hours"])),
    }


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--run-id", default=dt.datetime.now(dt.timezone.utc).strftime("m2_%Y%m%dT%H%M%SZ"))
    ap.add_argument("--report-out", required=True)
    ap.add_argument("--explain-out", required=True)
    args = ap.parse_args()

    spark = SparkSession.builder.appName("bdfp-m2-gold-traffic_weather").getOrCreate()
    platform = load_config()
    tw, om_cfg, uci_cfg = load_configs()
    root = f"{platform['hdfs']['uri']}{platform['hdfs']['datalake_root']}"
    uci_in = f"{root}/silver/{uci_cfg['silver_dataset']}/{uci_cfg['version']}/outcome=accepted"
    om_in = f"{root}/silver/{om_cfg['silver_dataset']}/{om_cfg['version']}/outcome=accepted"
    uci = spark.read.option("basePath", uci_in).parquet(uci_in).cache()
    om = spark.read.option("basePath", om_in).parquet(om_in).cache()

    gold = build_gold(uci, om, tw, om_cfg, args.run_id)
    out = f"{root}/gold/{tw['gold_dataset']}/{tw['version']}"
    Path(args.explain_out).write_text(
        spark._jvm.PythonSQLUtils.explainString(gold._jdf.queryExecution(), "formatted"), encoding="utf-8")
    gold.repartition("year").write.mode("overwrite").partitionBy("year").parquet(out)

    back = spark.read.parquet(out)
    rep = report(uci, back, tw, om_cfg, om)
    sample = (back.where("era5_precipitation_mm > 0").orderBy("hour_start_utc").limit(2).collect()
              + back.orderBy("hour_start_utc").limit(1).collect())
    rep.update({"inputs": [uci_in, om_in], "output": out, "run_id": args.run_id,
                "spark_app_id": spark.sparkContext.applicationId, "schema": back.schema.simpleString(),
                "samples": [{k: (v.isoformat() if hasattr(v, "isoformat") else v)
                             for k, v in r.asDict(recursive=True).items()} for r in sample]})
    Path(args.report_out).write_text(json.dumps(rep, indent=2, default=str), encoding="utf-8")
    print(f"report -> {args.report_out}; reconciliation={rep['reconciliation']}", flush=True)
    spark.stop()
    return 0 if rep["reconciliation"]["no_fan_out"] else 1


if __name__ == "__main__":
    sys.exit(main())
