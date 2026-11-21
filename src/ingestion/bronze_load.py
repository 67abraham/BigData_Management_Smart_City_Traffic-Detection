"""Spark job: raw files in HDFS Bronze -> Bronze Parquet records with provenance (spec T01, T02).

Input   hdfs:///datalake/bronze/<source>/raw/ingest_date=D/        (byte-identical landed files)
        data/local/landing/<source>/ingest_date=D/_manifest.json  (same manifest as in raw/)
Output  hdfs:///datalake/bronze/<source>/records/ingest_date=D/    (Parquet; values kept as delivered)
        --profile-out FILE -> evidence/AC01/<run>/profile_<source>.json (written to a file: a Spark log
        line can interleave with a long stdout line and corrupt it, run m1_20261121T111321Z)

Every record gets the provenance columns _source, _source_file, _ingested_at, _source_version,
_sha256. Bronze does NOT clean or type-cast; anomalies are counted and reported here and handled
in Silver (M2). Re-running overwrites only the ingest_date=D partition (idempotent).

Run: spark-submit /workspace/src/ingestion/bronze_load.py --source uci_metro --ingest-date 2026-11-21
"""
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import io
import json
import re
import sys
from pathlib import Path
import zipfile
from typing import Any

import yaml
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from src.common.config import PROJECT_ROOT, load_config

UCI_COLUMNS = ["holiday", "temp", "rain_1h", "snow_1h", "clouds_all", "weather_main",
               "weather_description", "date_time", "traffic_volume"]
KITTI_LABEL_FIELDS = ["type", "truncated", "occluded", "alpha", "bbox_left", "bbox_top", "bbox_right",
                      "bbox_bottom", "dim_height", "dim_width", "dim_length", "loc_x", "loc_y", "loc_z",
                      "rotation_y"]
KITTI_LABEL_RE = re.compile(r"^training/label_2/(\d{6})\.txt$")


def basename(col: Any) -> Any:
    return F.element_at(F.split(col, "/"), -1)


class Job:
    def __init__(self, spark: SparkSession, source: str, ingest_date: str) -> None:
        self.spark = spark
        self.source = source
        self.ingest_date = ingest_date
        cfg = load_config()
        self.sources = yaml.safe_load((PROJECT_ROOT / "configs" / "sources.yaml").read_text())
        hdfs = cfg["hdfs"]
        root = f"{hdfs['uri']}{hdfs['datalake_root']}/bronze/{source}"
        self.raw = f"{root}/raw/ingest_date={ingest_date}"
        self.root = root
        mpath = PROJECT_ROOT / cfg["local"]["data_dir"] / "landing" / source / f"ingest_date={ingest_date}" / "_manifest.json"
        self.manifest: dict[str, Any] = json.loads(mpath.read_text(encoding="utf-8"))
        self.profile: dict[str, Any] = {"source": source, "ingest_date": ingest_date,
                                        "raw_path": self.raw, "spark_app_id": spark.sparkContext.applicationId}

    # ---------- provenance & integrity ----------
    def provenance(self) -> DataFrame:
        rows = [(f["name"].rsplit("/", 1)[-1], f["sha256"], f["source_version"], f["downloaded_at"])
                for f in self.manifest["files"] if f["status"] != "failed"]
        schema = "_source_file string, _sha256 string, _source_version string, _ingested_at string"
        return self.spark.createDataFrame(rows, schema)

    def with_provenance(self, df: DataFrame, file_name: str) -> DataFrame:
        """Attach provenance of one landed file (by name, from the manifest).

        Literal values instead of input_file_name(): that function returns '' on cached or
        RDD-built DataFrames, which would silently null the provenance columns.
        """
        f = next(x for x in self.manifest["files"] if x["name"].rsplit("/", 1)[-1] == file_name)
        return (df.withColumn("_source", F.lit(self.source))
                  .withColumn("_source_file", F.lit(file_name))
                  .withColumn("_ingested_at", F.to_timestamp(F.lit(f["downloaded_at"])))
                  .withColumn("_source_version", F.lit(f["source_version"]))
                  .withColumn("_sha256", F.lit(f["sha256"])))

    def verify_integrity(self) -> None:
        """Recompute SHA-256 of every raw file as stored in HDFS and compare with the manifest."""
        sha = F.udf(lambda b: hashlib.sha256(bytes(b)).hexdigest(), T.StringType())
        got = (self.spark.read.format("binaryFile").option("recursiveFileLookup", "true").load(self.raw)
               .select(basename("path").alias("name"), "length", sha("content").alias("sha"))
               .collect())
        hdfs = {r["name"]: (r["sha"], r["length"]) for r in got}
        expected = {f["name"].rsplit("/", 1)[-1]: f["sha256"] for f in self.manifest["files"]
                    if f["status"] != "failed" and f.get("sha256")}
        mismatched = sorted(n for n, s in expected.items() if n in hdfs and hdfs[n][0] != s)
        missing = sorted(n for n in expected if n not in hdfs)
        self.profile["integrity"] = {
            "files_in_manifest": len(expected),
            "files_in_hdfs": len(hdfs),
            "sha256_match": sum(1 for n, s in expected.items() if n in hdfs and hdfs[n][0] == s),
            "sha256_mismatch": mismatched,
            "missing_in_hdfs": missing,
            "extra_in_hdfs": sorted(set(hdfs) - set(expected) - {"_manifest.json"}),
            "bytes_in_hdfs": sum(v[1] for v in hdfs.values()),
        }
        self.profile["manifest_status_counts"] = {
            s: sum(1 for f in self.manifest["files"] if f["status"] == s)
            for s in sorted({f["status"] for f in self.manifest["files"]})}
        self.profile["manifest_failures"] = [
            {"name": f["name"], "error": f["error"]} for f in self.manifest["files"] if f["status"] == "failed"]

    def write(self, df: DataFrame, subdir: str = "records", extra: str = "") -> str:
        path = f"{self.root}/{subdir}/ingest_date={self.ingest_date}{extra}"
        df.write.mode("overwrite").parquet(path)
        return path

    # ---------- sources ----------
    def uci_metro(self) -> None:
        src = self.sources["uci_metro"]
        path = f"{self.raw}/{src['member']}"
        text = self.spark.read.text(path)
        header = text.first()["value"]
        raw_lines = text.count() - 1
        schema = T.StructType([T.StructField(c, T.StringType()) for c in UCI_COLUMNS]
                              + [T.StructField("_corrupt_record", T.StringType())])
        df = (self.spark.read.option("header", True).option("mode", "PERMISSIVE")
              .option("columnNameOfCorruptRecord", "_corrupt_record").schema(schema).csv(path)).cache()
        rows = df.count()
        ts = F.to_timestamp("date_time", "yyyy-MM-dd HH:mm:ss")
        agg = df.agg(
            F.sum(F.col("_corrupt_record").isNotNull().cast("int")).alias("corrupt_records"),
            F.min("date_time").alias("date_time_min"), F.max("date_time").alias("date_time_max"),
            F.countDistinct("date_time").alias("distinct_date_time"),
            F.sum(ts.isNull().cast("int")).alias("unparseable_date_time"),
            F.sum(F.col("traffic_volume").cast("int").isNull().cast("int")).alias("non_integer_traffic_volume"),
            *[F.sum((F.col(c).isNull() | (F.trim(F.col(c)) == "")).cast("int")).alias(f"null_or_empty__{c}")
              for c in UCI_COLUMNS],
        ).first().asDict()
        self.profile.update({
            "header": header, "header_matches_expected": header.split(",") == UCI_COLUMNS,
            "raw_data_lines": raw_lines, "rows_parsed": rows,
            "lines_vs_rows_reconciled": raw_lines == rows,
            "duplicate_date_time_rows": rows - agg["distinct_date_time"], **agg,
            "schema": df.schema.simpleString(),
        })
        self.profile["output"] = self.write(self.with_provenance(df, src["member"]))
        df.unpersist()

    def nyc_tlc(self) -> None:
        months: dict[str, Any] = {}
        for f in self.manifest["files"]:
            if f["status"] == "failed" or not f["name"].endswith(".parquet"):
                continue
            month = f["extra"]["month"]
            d = self.spark.read.parquet(f"{self.raw}/{f['name']}")
            pickup = "tpep_pickup_datetime"
            stats = d.agg(
                F.count(F.lit(1)).alias("rows"),
                F.sum((F.date_format(pickup, "yyyy-MM") != month).cast("int")).alias("pickup_outside_file_month"),
                F.min(pickup).cast("string").alias("pickup_min"), F.max(pickup).cast("string").alias("pickup_max"),
                F.sum(F.col("passenger_count").isNull().cast("int")).alias("null_passenger_count"),
            ).first().asDict()
            months[month] = {"file": f["name"], "schema": d.schema.simpleString(),
                             "columns": [(x.name, x.dataType.simpleString()) for x in d.schema], **stats}
            out = self.write(self.with_provenance(d, f["name"]), extra=f"/source_month={month}")
            months[month]["output"] = out
        base = next(iter(months.values()))["columns"] if months else []
        self.profile["months"] = months
        self.profile["rows_total"] = sum(m["rows"] for m in months.values())
        self.profile["schema_differs_from_first_month"] = {
            m: sorted(set(map(tuple, v["columns"])) ^ set(map(tuple, base))) for m, v in months.items()
            if v["columns"] != base}
        zones = self.spark.read.option("header", True).csv(f"{self.raw}/taxi_zone_lookup.csv")
        self.profile["zone_lookup"] = {"rows": zones.count(), "columns": zones.columns,
                                       "output": self.write(self.with_provenance(zones, "taxi_zone_lookup.csv"), "zone_lookup")}

    def open_meteo(self) -> None:
        variables = self.sources["open_meteo"]["hourly"]
        j = (self.spark.read.option("multiLine", True).json(f"{self.raw}/open_meteo_*.json")
             .withColumn("_src", basename(F.input_file_name())))
        ex = j.select("_src", F.col("latitude").alias("grid_latitude"), F.col("longitude").alias("grid_longitude"),
                      "elevation", "timezone", F.col("hourly").alias("h"),
                      F.posexplode("hourly.time").alias("pos", "time"))
        for v in variables:
            ex = ex.withColumn(v, F.col(f"h.{v}")[F.col("pos")])
        df = ex.drop("h", "pos").withColumnRenamed("_src", "_source_file_tmp").cache()
        rows = df.count()
        ts = F.to_timestamp("time", "yyyy-MM-dd'T'HH:mm")
        agg = df.agg(F.countDistinct("time").alias("distinct_hours"), F.min("time").alias("time_min"),
                     F.max("time").alias("time_max"), F.sum(ts.isNull().cast("int")).alias("unparseable_time"),
                     F.countDistinct("grid_latitude", "grid_longitude").alias("distinct_grid_points"),
                     *[F.sum(F.col(v).isNull().cast("int")).alias(f"null__{v}") for v in variables]).first().asDict()
        cfg = self.sources["open_meteo"]
        start = dt.datetime.fromisoformat(cfg["start_date"])
        end = dt.datetime.fromisoformat(cfg["end_date"]) + dt.timedelta(hours=23)
        expected = int((end - start).total_seconds() // 3600) + 1
        per_file = {r["_source_file_tmp"]: r["n"] for r in df.groupBy("_source_file_tmp").agg(F.count("*").alias("n")).collect()}
        grid = df.select("grid_latitude", "grid_longitude", "elevation").distinct().collect()
        self.profile.update({"rows": rows, "expected_hours": expected,
                             "missing_hours": expected - agg["distinct_hours"],
                             "duplicate_hour_rows": rows - agg["distinct_hours"], **agg,
                             "rows_per_file": per_file, "grid_points": [r.asDict() for r in grid],
                             "requested_location": {k: self.manifest["params"][k] for k in ("latitude", "longitude", "location_from")},
                             "model": cfg["model"], "schema": df.schema.simpleString()})
        out = df.withColumnRenamed("_source_file_tmp", "_source_file").join(
            F.broadcast(self.provenance()), "_source_file", "left").withColumn("_source", F.lit(self.source)) \
            .withColumn("_ingested_at", F.to_timestamp("_ingested_at"))
        self.profile["output"] = self.write(out)
        df.unpersist()

    def kitti(self) -> None:
        label_zip = f"{self.raw}/data_object_label_2.zip"
        blob = self.spark.read.format("binaryFile").load(label_zip)
        n = len(KITTI_LABEL_FIELDS)

        def lines(row: Any):
            zf = zipfile.ZipFile(io.BytesIO(bytes(row.content)))
            for member in zf.namelist():
                m = KITTI_LABEL_RE.match(member)
                if not m:
                    continue
                for i, line in enumerate(zf.read(member).decode("utf-8").splitlines()):
                    parts = line.split()
                    ok = len(parts) == n
                    yield (m.group(1), member, i, line, ok, *(parts if ok else [None] * n))

        def files(row: Any):
            zf = zipfile.ZipFile(io.BytesIO(bytes(row.content)))
            for member in zf.namelist():
                if m := KITTI_LABEL_RE.match(member):
                    yield (m.group(1), member, zf.getinfo(member).file_size)

        lab_schema = T.StructType(
            [T.StructField("image_id", T.StringType()), T.StructField("label_member", T.StringType()),
             T.StructField("line_no", T.IntegerType()), T.StructField("raw_line", T.StringType()),
             T.StructField("well_formed", T.BooleanType())]
            + [T.StructField(f, T.StringType()) for f in KITTI_LABEL_FIELDS])
        labels = self.spark.createDataFrame(blob.rdd.flatMap(lines), lab_schema).cache()
        label_files = self.spark.createDataFrame(
            blob.rdd.flatMap(files), "image_id string, label_member string, label_bytes long").cache()

        def png_info(b: bytes):
            b = bytes(b)
            ok = b[:8] == b"\x89PNG\r\n\x1a\n" and b[12:16] == b"IHDR"
            w = int.from_bytes(b[16:20], "big") if ok else None
            h = int.from_bytes(b[20:24], "big") if ok else None
            return (ok, w, h, hashlib.sha256(b).hexdigest())

        info = F.udf(png_info, "png_ok boolean, width int, height int, sha256 string")
        images = (self.spark.read.format("binaryFile").load(f"{self.raw}/training/image_2/*.png")
                  .select(F.regexp_extract(basename("path"), r"^(\d{6})\.png$", 1).alias("image_id"),
                          F.col("path").alias("image_path"), F.col("length").alias("image_bytes"),
                          info("content").alias("i"))
                  .select("image_id", "image_path", "image_bytes", "i.*")).cache()
        inventory = (label_files.join(images, "image_id", "left")
                     .withColumn("is_available", F.col("image_path").isNotNull())
                     .withColumn("image_format", F.lit("png"))
                     .withColumn("split", F.lit(self.sources["kitti"]["split"]))
                     .withColumn("_source", F.lit(self.source))
                     .withColumn("_label_archive_sha256", F.lit(next(
                         f["sha256"] for f in self.manifest["files"] if f["name"] == "data_object_label_2.zip"))))
        types = {r["type"]: r["n"] for r in labels.filter("well_formed").groupBy("type").agg(F.count("*").alias("n")).collect()}
        img = images.agg(F.count("*").alias("n"), F.sum((~F.col("png_ok")).cast("int")).alias("bad_png"),
                         F.countDistinct("width", "height").alias("distinct_sizes"),
                         F.min("width").alias("w_min"), F.max("width").alias("w_max"),
                         F.min("height").alias("h_min"), F.max("height").alias("h_max")).first().asDict()
        lab_count = labels.count()
        self.profile.update({
            "label_files": label_files.count(),
            "label_lines": lab_count,
            "malformed_label_lines": labels.filter(~F.col("well_formed")).count(),
            "label_files_without_lines": label_files.join(labels.select("image_id").distinct(), "image_id", "left_anti").count(),
            "object_type_counts": types,
            "images_available": img["n"], "images_bad_png": img["bad_png"],
            "image_width_range": [img["w_min"], img["w_max"]], "image_height_range": [img["h_min"], img["h_max"]],
            "image_ids_without_label": images.join(label_files, "image_id", "left_anti").count(),
        })
        self.profile["output"] = self.write(self.with_provenance(labels, "data_object_label_2.zip"))
        self.profile["inventory_output"] = self.write(inventory, "image_inventory")
        for d in (labels, label_files, images):
            d.unpersist()


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--source", required=True, choices=["uci_metro", "nyc_tlc", "open_meteo", "kitti"])
    p.add_argument("--ingest-date", required=True)
    p.add_argument("--profile-out", required=True, help="path for the profile JSON (driver-local)")
    args = p.parse_args()
    spark = SparkSession.builder.appName(f"bdfp-m1-bronze-{args.source}").getOrCreate()
    job = Job(spark, args.source, args.ingest_date)
    job.verify_integrity()
    getattr(job, args.source)()
    integ = job.profile["integrity"]
    job.profile["ok"] = not (integ["sha256_mismatch"] or integ["missing_in_hdfs"] or job.profile["manifest_failures"])
    Path(args.profile_out).write_text(json.dumps(job.profile, default=str), encoding="utf-8")
    print(f"profile -> {args.profile_out}; ok={job.profile['ok']}", flush=True)
    spark.stop()
    return 0 if job.profile["ok"] else 1


if __name__ == "__main__":
    sys.exit(main())
