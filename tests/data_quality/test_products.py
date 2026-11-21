"""Data-quality tests for the T07 product transforms (Spark, synthetic inputs only)."""
import datetime as dt

from src.products import contracts
from src.products.build_products import (WINDOW_SCHEMA, conform, road_image_manifest, traffic_features)
from src.products.windows import window_rows

T0 = dt.datetime(2016, 12, 25, 0, 0)      # UTC; local = UTC-6 in December


def hourly_df(spark, volumes):
    """volumes: {hour offset from T0: volume}; missing offsets are gaps."""
    rows = []
    for h, v in sorted(volumes.items()):
        utc = T0 + dt.timedelta(hours=h)
        local = utc - dt.timedelta(hours=6)
        rows.append(("301", utc, local, local.date(), v, False))
    return spark.createDataFrame(rows, "station_id string, hour_start_utc timestamp, hour_start_local timestamp_ntz,"
                                       " local_date date, traffic_volume int, is_holiday_date boolean")


def feats(spark, volumes):
    return {r["hour_start_utc"]: r.asDict() for r in traffic_features(hourly_df(spark, volumes), "t").collect()}


def test_lags_are_exact_hours_and_null_across_gaps(spark):
    vols = {h: 1000 + h for h in range(0, 200) if h not in (100, 101)}   # gap at 100-101
    f = feats(spark, vols)
    at = lambda h: f[T0 + dt.timedelta(hours=h)]
    assert at(50)["lag_1h"] == 1049 and at(50)["lag_2h"] == 1048 and at(50)["lag_24h"] == 1026
    assert at(102)["lag_1h"] is None and at(102)["lag_2h"] is None and at(103)["lag_2h"] is None
    assert at(170)["lag_168h"] == 1002 and at(10)["lag_168h"] is None
    assert at(30)["rolling_count_prev_24h"] == 24
    assert at(30)["rolling_mean_prev_24h"] == sum(1000 + h for h in range(6, 30)) / 24
    assert at(0)["rolling_count_prev_24h"] == 0 and at(0)["rolling_mean_prev_24h"] is None
    assert at(170)["lags_complete"] is True and at(102)["lags_complete"] is False


def test_no_feature_uses_future_values(spark):
    base = {h: 1000 + h for h in range(0, 200)}
    changed = {h: (v if h <= 120 else 9999) for h, v in base.items()}   # change every hour after 120
    a, b = feats(spark, base), feats(spark, changed)
    feature_cols = ["lag_1h", "lag_2h", "lag_24h", "lag_168h", "rolling_mean_prev_24h", "rolling_count_prev_24h",
                    "hour_of_day", "day_of_week", "month", "is_weekend", "lags_complete", "split"]
    for h in range(0, 121):                                   # every row up to and incl. hour 120
        k = T0 + dt.timedelta(hours=h)
        assert {c: a[k][c] for c in feature_cols} == {c: b[k][c] for c in feature_cols}, h
    k = T0 + dt.timedelta(hours=121)                          # 121's lag_1h is hour 120: unchanged
    assert a[k]["lag_1h"] == b[k]["lag_1h"] == 1120


def test_calendar_and_split(spark):
    f = feats(spark, {h: 1000 for h in range(0, 24 * 10)})
    # 2016-12-26 is a Monday; local 2016-12-26 00:00 = UTC 06:00
    mon = f[dt.datetime(2016, 12, 26, 6, 0)]
    assert (mon["day_of_week"], mon["is_weekend"], mon["hour_of_day"], mon["month"]) == (1, False, 0, 12)
    sun = f[dt.datetime(2017, 1, 1, 6, 0)]
    assert (sun["day_of_week"], sun["is_weekend"]) == (7, True)
    assert f[dt.datetime(2017, 1, 1, 5, 0)]["split"] == "train"        # local 2016-12-31 23:00
    assert sun["split"] == "validation"                                 # local 2017-01-01 00:00


def test_features_conform_to_contract(spark):
    df = conform(traffic_features(hourly_df(spark, {h: 1 for h in range(48)}), "t"), contracts.load("traffic_features"))
    c = contracts.load("traffic_features")
    assert [(f.name, f.dataType.simpleString()) for f in df.schema.fields] == [(x["name"], x["type"]) for x in c["fields"]]


def test_image_manifest_counts_and_availability(spark):
    inv = spark.createDataFrame([
        ("000001", "training/label_2/000001.txt", 120, "hdfs://h/000001.png", 800, True, 1242, 375, "a" * 64, True,
         "png", "training", "kitti", "z" * 64),
        ("000002", "training/label_2/000002.txt", 60, None, None, None, None, None, None, False,
         "png", "training", "kitti", "z" * 64)],
        "image_id string, label_member string, label_bytes long, image_path string, image_bytes long, png_ok boolean,"
        " width int, height int, sha256 string, is_available boolean, image_format string, split string,"
        " _source string, _label_archive_sha256 string")
    lab = spark.createDataFrame([("000001", "Car", True), ("000001", "Car", True), ("000001", "DontCare", True),
                                 ("000001", "Pedestrian", True), ("000002", "Cyclist", True)],
                                "image_id string, type string, well_formed boolean")
    rows = {r["image_id"]: r for r in conform(road_image_manifest(inv, lab, "hdfs://raw", "t"),
                                              contracts.load("road_image_manifest")).collect()}
    a, b = rows["000001"], rows["000002"]
    assert (a["label_lines"], a["object_count"], a["dontcare_count"]) == (4, 3, 1)
    assert a["object_types"] == ["Car", "Pedestrian"] and a["file_available"] and a["width_px"] == 1242
    assert b["file_available"] is False and b["image_path"] is None and b["object_count"] == 1
    assert a["label_path"] == "hdfs://raw/data_object_label_2.zip!training/label_2/000001.txt"


def test_window_metrics_conform_to_contract(spark):
    r = {"station_id": "301", "window_start_utc": "2016-07-01T06:00:00Z", "window_end_utc": "2016-07-01T12:00:00Z",
         "window_size_hours": 6, "event_count": 6, "expected_events": 6, "completeness": 1.0, "volume_sum": 60,
         "volume_avg": 10.0, "volume_min": 5, "volume_max": 15, "watermark_at_emit": "w", "firing": "on_time",
         "update_seq": 0, "record_kind": "historical_event_replay_window", "source_topic": "t", "job_run_id": "j"}
    c = contracts.load("traffic_window_metrics")
    df = conform(spark.createDataFrame(window_rows([r], "t"), WINDOW_SCHEMA), c)
    assert [(f.name, f.dataType.simpleString()) for f in df.schema.fields] == [(x["name"], x["type"]) for x in c["fields"]]
    assert df.first()["window_start_utc"] == dt.datetime(2016, 7, 1, 6, 0)
