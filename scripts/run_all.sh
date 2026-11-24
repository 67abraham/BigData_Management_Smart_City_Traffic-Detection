#!/usr/bin/env bash
# Full pipeline, acquisition -> publication -> dashboard data (spec T08/AC08 run order).
# From a fresh Codespace: cp .env.example .env (add the KITTI links), then
#   bash scripts/run_all.sh            # about 1 h (see README): stack, M0 smoke, M1 ingest, M2 Spark, M3 streaming, M4 products/benchmarks,
#                                      # M5 export, then starts the dashboard and prints its link
# Each stage is a milestone script with its own evidence folder; this script records the order, exit
# codes and durations in evidence/AC08/full_<UTC>/stages.tsv and stops at the first failing stage
# (re-run it: every stage is idempotent, so a re-run is also the recovery path).
set -uo pipefail
cd "$(dirname "$0")/.."
RUN_ID="full_$(date -u +%Y%m%dT%H%M%SZ)"
E="evidence/AC08/$RUN_ID"; mkdir -p "$E"
printf "stage\tcommand\texit\tseconds\tstarted_utc\n" >"$E/stages.tsv"

T_ALL=$(date +%s)
N_STAGES=8
mmss() { printf "%dm%02ds" $(( $1 / 60 )) $(( $1 % 60 )); }

# Duration of each stage in the last complete run, so the teacher knows how long to expect (real data only).
PREV=$(ls -d evidence/AC08/full_*/ 2>/dev/null | sort | while read -r d; do
  [ "$d" = "$E/" ] && continue
  awk -F'\t' 'NR>1 && $3!=0 {bad=1} END {exit bad}' "$d/stages.tsv" 2>/dev/null &&
    [ "$(awk 'END{print NR}' "$d/stages.tsv")" -ge 8 ] && echo "$d"; done | tail -n 1)
prev_secs() { [ -n "$PREV" ] && awk -F'\t' -v n="$1" '$1==n {print $4}' "$PREV/stages.tsv"; }

show() {  # show <stage> <line>: print one log line live; the Docker build output of 00_stack is kept to its log
  case "$1" in
    00_stack) [[ "$2" =~ ^t=|rror|^STOP ]] && echo "    $2" ;;
    *) echo "    $2" ;;
  esac
}

stage() {  # stage <name> <description> <command...>
  local name=$1 desc=$2; shift 2
  local t0; t0=$(date +%s); local start; start=$(date -u +%FT%TZ)
  local i=$(( 10#${name%%_*} + 1 )) p; p=$(prev_secs "$name")
  echo
  echo "=== [$i/$N_STAGES] $name: $desc"
  echo "    command: $*   log: $E/$name.log${p:+   (last full run: $(mmss "$p"))}"
  "$@" >"$E/$name.log" 2>&1 &
  local pid=$! shown=0 quiet=0 n line cur
  while kill -0 "$pid" 2>/dev/null; do
    sleep 5
    n=$(wc -l <"$E/$name.log")
    if [ "$n" -gt "$shown" ]; then
      while IFS= read -r line; do show "$name" "$line"; done < <(sed -n "$((shown + 1)),${n}p" "$E/$name.log")
      shown=$n; quiet=0
    else
      quiet=$((quiet + 5))
      if [ "$quiet" -ge 30 ]; then
        # last progress line: "RUN x" = x is running; "PASS x" = x finished, the script is between steps
        cur=$(grep -E '^(RUN|PASS|===) ' "$E/$name.log" | tail -n 1 |
              sed -E 's/^(RUN|===) +(.*)/on: \2/; s/^PASS +(.*)/after: \1/; s/ ===$//; s/ +-> .*//')
        echo "    ... still working${cur:+ $cur} | stage $(mmss $(( $(date +%s) - t0 ))) | total $(mmss $(( $(date +%s) - T_ALL )))"
        quiet=0
      fi
    fi
  done
  wait "$pid"; local rc=$?
  while IFS= read -r line; do show "$name" "$line"; done < <(sed -n "$((shown + 1)),\$p" "$E/$name.log")
  local secs=$(( $(date +%s) - t0 ))
  printf "%s\t%s\t%s\t%s\t%s\n" "$name" "$*" "$rc" "$secs" "$start" >>"$E/stages.tsv"
  if [ $rc -ne 0 ]; then echo "STOP: $name failed (exit $rc) after $(mmss $secs), see $E/$name.log"; exit $rc; fi
  echo "    done: $name in $(mmss $secs) | total so far $(mmss $(( $(date +%s) - T_ALL )))"
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

echo "run $RUN_ID: 8 stages, about 1 hour in total. Progress is printed below; every stage's full log is in $E/"
[ -n "$PREV" ] && echo "last complete run for comparison: $PREV"
stage 00_stack       "start the Docker stack (HDFS, Spark, Kafka, Flink) and wait until it is healthy" wait_stack
stage 01_smoke_m0    "M0 smoke test: HDFS, Spark, Kafka, Flink each checked with a real read/write" bash scripts/smoke_m0.sh
stage 02_ingest_m1   "M1 ingestion: download the 4 sources, copy to HDFS Bronze, checksums and profiles" bash scripts/ingest_m1.sh
stage 03_spark_m2    "M2 Spark: Bronze -> Silver/Gold with quality rules and reconciliation" bash scripts/run_m2.sh
stage 04_stream_m3   "M3 Kafka + Flink: historical event replay, edge cases, failure/recovery (longest stage)" bash scripts/run_m3.sh replay edge recovery
stage 05_products_m4 "M4 data products + read-back, SQL analyses, benchmarks" bash scripts/run_m4.sh products analytics benchmark
stage 06_export_m5   "M5 export the dashboard data from the products" bash scripts/run_m5.sh
stage 07_dashboard   "start the Streamlit dashboard (last: it shows the outputs of all stages above)" bash scripts/start_dashboard.sh
echo
echo "=== all stages passed in $(mmss $(( $(date +%s) - T_ALL ))): $E/stages.tsv"
column -t -s $'\t' "$E/stages.tsv"
grep '^dashboard: ' "$E/07_dashboard.log"
