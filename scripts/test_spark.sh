#!/usr/bin/env bash
# Run pytest suites that need Spark inside the spark-master container (local SparkSession).
#   bash scripts/test_spark.sh                      # tests/data_quality
#   bash scripts/test_spark.sh tests/data_quality -k duplicate
set -euo pipefail
cd "$(dirname "$0")/.."
[ $# -eq 0 ] && set -- tests/data_quality
PY4J=$(docker compose exec -T spark-master bash -c 'ls /opt/spark/python/lib/py4j-*-src.zip' | tr -d '\r')
docker compose exec -T spark-master env \
  PYTHONPATH="/workspace:/opt/spark/python:${PY4J}" PYTHONDONTWRITEBYTECODE=1 \
  python3 -m pytest -q -p no:cacheprovider "$@"
