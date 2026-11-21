#!/usr/bin/env bash
# Full pipeline, acquisition -> publication -> dashboard data (spec T08/AC08 run order).
# From a fresh Codespace: cp .env.example .env (add the KITTI links), then
#   bash scripts/run_all.sh            # ~1 h: stack, M0 smoke, M1 ingest, M2 Spark, M3 streaming, M4 products/benchmarks, M5 export
# Each stage is a milestone script with its own evidence folder; this script records the order, exit
# codes and durations in evidence/AC08/full_<UTC>/stages.tsv and stops at the first failing stage
# (re-run it: every stage is idempotent, so a re-run is also the recovery path).
set -uo pipefail
cd "$(dirname "$0")/.."
RUN_ID="full_$(date -u +%Y%m%dT%H%M%SZ)"
E="evidence/AC08/$RUN_ID"; mkdir -p "$E"
printf "stage\tcommand\texit\tseconds\tstarted_utc\n" >"$E/stages.tsv"

stage() {  # stage <name> <command...>
  local name=$1; shift
  local t0; t0=$(date +%s); local start; start=$(date -u +%FT%TZ)
  echo "=== $name: $*"
  "$@" >"$E/$name.log" 2>&1
  local rc=$?
  printf "%s\t%s\t%s\t%s\t%s\n" "$name" "$*" "$rc" "$(( $(date +%s) - t0 ))" "$start" >>"$E/stages.tsv"
  tail -n 3 "$E/$name.log"
  if [ $rc -ne 0 ]; then echo "STOP: $name failed (exit $rc), see $E/$name.log"; exit $rc; fi
}

wait_stack() {  # namenode and kafka healthy (spark-history restarts until the smoke test creates /spark-events,
               # so `docker compose up --wait` would report a failure on a fresh stack)
  docker compose up -d --build || return 1
  for i in $(seq 1 60); do
    h=$(docker compose ps --format '{{.Service}} {{.Health}}' | awk '$1=="namenode"||$1=="kafka"{print $2}' | sort -u | tr '\n' ' ')
    echo "t=$((i * 5))s health: $h"
    [ "$h" = "healthy " ] && { docker compose ps; return 0; }
    sleep 5
  done
  return 1
}

stage 00_stack        wait_stack
stage 01_smoke_m0     bash scripts/smoke_m0.sh
stage 02_ingest_m1    bash scripts/ingest_m1.sh
stage 03_spark_m2     bash scripts/run_m2.sh
stage 04_stream_m3    bash scripts/run_m3.sh replay edge recovery
stage 05_products_m4  bash scripts/run_m4.sh products analytics benchmark
stage 06_export_m5    bash scripts/run_m5.sh
echo "=== all stages passed: $E/stages.tsv"
column -t -s $'\t' "$E/stages.tsv"
