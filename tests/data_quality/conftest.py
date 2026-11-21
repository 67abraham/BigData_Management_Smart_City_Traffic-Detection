"""Spark fixture for data-quality tests. Runs inside the spark-master container:
    bash scripts/test_spark.sh tests/data_quality
Skipped where PySpark is not importable (e.g. the Mac), so `pytest tests/unit` stays Docker-free.
"""
import pytest

pyspark = pytest.importorskip("pyspark")


@pytest.fixture(scope="session")
def spark():
    from pyspark.sql import SparkSession
    s = (SparkSession.builder.master("local[2]").appName("bdfp-tests")
         .config("spark.sql.session.timeZone", "UTC")       # same as configs/spark/spark-defaults.conf
         .config("spark.sql.shuffle.partitions", "2")
         .config("spark.ui.enabled", "false")
         .config("spark.eventLog.enabled", "false")
         .getOrCreate())
    s.sparkContext.setLogLevel("ERROR")
    yield s
    s.stop()
