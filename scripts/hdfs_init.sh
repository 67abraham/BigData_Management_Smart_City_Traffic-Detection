#!/usr/bin/env bash
# Create the HDFS layer directories (idempotent). Spec T02: Bronze/Silver/Gold layers.
set -euo pipefail
cd "$(dirname "$0")/.."
dc() { docker compose exec -T namenode "$@"; }

dc hdfs dfsadmin -safemode wait
dc hdfs dfs -mkdir -p /datalake/bronze /datalake/silver /datalake/gold /datalake/_smoke /spark-events
dc hdfs dfs -chmod 755 /datalake /datalake/bronze /datalake/silver /datalake/gold
dc hdfs dfs -chmod 1777 /spark-events      # every job may append its event log (sticky bit)
dc hdfs dfs -ls -R /datalake
dc hdfs dfs -ls /spark-events >/dev/null && echo "[hdfs_init] /spark-events ready"
