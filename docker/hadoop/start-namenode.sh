#!/usr/bin/env bash
# Format the NameNode only on first start (empty volume), then run it in the foreground.
set -euo pipefail
NAME_DIR=/hadoop/dfs/name
if [ ! -f "$NAME_DIR/current/VERSION" ]; then
  echo "[start-namenode] formatting new namespace in $NAME_DIR"
  hdfs namenode -format -nonInteractive -force -clusterId "${HDFS_CLUSTER_ID:-bdfp-hdfs}"
fi
exec hdfs namenode
