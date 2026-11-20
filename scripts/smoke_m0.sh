#!/usr/bin/env bash
# M0 smoke test: proves every service is up and that Spark reads/writes HDFS (spec T02/AC02, T08).
# Run in the Codespace, from the project folder, after `docker compose up -d --build`.
# Writes all raw output to evidence/AC02/m0_<UTC timestamp>/ — nothing is edited by hand.
set -uo pipefail
cd "$(dirname "$0")/.."

RUN_ID="m0_$(date -u +%Y%m%dT%H%M%SZ)"
OUT="evidence/AC02/${RUN_ID}"
mkdir -p "$OUT"
SUMMARY="$OUT/summary.txt"
FAILS=0

step() {  # step <name> <logfile> <command...>
  local name=$1 log=$2; shift 2
  echo "=== $name ===" | tee -a "$SUMMARY"
  if "$@" >"$OUT/$log" 2>&1; then
    echo "PASS  $name  -> $log" | tee -a "$SUMMARY"
  else
    echo "FAIL  $name  -> $log (exit $?)" | tee -a "$SUMMARY"
    FAILS=$((FAILS + 1))
  fi
}
dc() { docker compose exec -T "$@"; }

environment() {
  echo "run_id: $RUN_ID"; date -u
  echo "--- host"; uname -a; nproc; free -h; df -h .
  echo "--- docker"; docker version --format '{{.Server.Version}}'; docker compose version
  echo "--- services"; docker compose ps
  echo "--- versions"
  dc namenode hadoop version | head -1
  dc spark-master spark-submit --version 2>&1 | grep -m1 -i "version"
  dc spark-master python3 --version
  dc kafka /opt/kafka/bin/kafka-topics.sh --version
  dc flink-jobmanager flink --version
  dc flink-jobmanager python -c "import pyflink, sys; print('pyflink ok, python', sys.version.split()[0])"
}

hdfs_write_read() {
  dc namenode hdfs dfs -put -f /opt/hadoop/etc/hadoop/hdfs-site.xml /datalake/_smoke/hdfs-site.xml
  dc namenode hdfs dfs -ls /datalake/_smoke
  dc namenode hdfs dfs -cat /datalake/_smoke/hdfs-site.xml | grep -A1 dfs.replication
}

kafka_roundtrip() {
  local topic=m0-smoke
  dc kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --create --if-not-exists \
     --topic "$topic" --partitions 3 --replication-factor 1
  dc kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --describe --topic "$topic"
  printf 'smoke-1\nsmoke-2\nsmoke-3\n' | dc kafka /opt/kafka/bin/kafka-console-producer.sh \
     --bootstrap-server kafka:9092 --topic "$topic"
  local got
  got=$(dc kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 \
        --topic "$topic" --from-beginning --max-messages 3 --timeout-ms 30000)
  echo "consumed:"; echo "$got"
  dc kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --delete --topic "$topic"
  [ "$(echo "$got" | grep -c '^smoke-')" -eq 3 ]
}

flink_cluster() {
  local ov
  ov=$(dc flink-jobmanager curl -sf http://localhost:8081/overview)
  echo "$ov"
  echo "$ov" | python3 -c "import json,sys; o=json.load(sys.stdin); sys.exit(0 if o['taskmanagers']>=1 else 1)"
}

step "environment & versions"       env.txt            environment
step "hdfs init (layers)"           hdfs_init.txt      bash scripts/hdfs_init.sh
step "hdfs dfsadmin -report"        dfsadmin_report.txt dc namenode hdfs dfsadmin -report
step "hdfs write/read"              hdfs_rw.txt        hdfs_write_read
step "spark on hdfs (parquet)"      spark_smoke.txt    dc spark-master spark-submit /workspace/src/batch/hdfs_smoke.py
step "hdfs fsck (blocks/replicas)"  fsck.txt           dc namenode hdfs fsck /datalake -files -blocks -locations
step "kafka produce/consume"        kafka.txt          kafka_roundtrip
step "flink cluster + pyflink"      flink.txt          flink_cluster

echo "---" | tee -a "$SUMMARY"
echo "failures: $FAILS   evidence: $OUT" | tee -a "$SUMMARY"
exit "$FAILS"
