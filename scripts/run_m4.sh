#!/usr/bin/env bash
# M4 run: data products (spec T07/AC07) and, later, analytics/benchmarks (T06/AC06).
# Run in the Codespace from the project root with the stack up, after M1-M3.
#   bash scripts/run_m4.sh products     # build 4 products -> read-back test -> write contract release blocks
#   bash scripts/run_m4.sh analytics    # T06 SQL analyses (needs products)
#   bash scripts/run_m4.sh benchmark    # T06 strategy pairs on NYC trips (REPEATS=3 timed runs + 1 warm-up)
# WINDOW_RUN=<m3 run id> picks the replay run for traffic_window_metrics (default: newest replay run whose
# oracle check passed). Evidence: evidence/AC07/m4_<UTC>/.
set -uo pipefail
cd "$(dirname "$0")/.."

PARTS=("$@"); [ ${#PARTS[@]} -eq 0 ] && PARTS=(products)
RUN_ID="m4_$(date -u +%Y%m%dT%H%M%SZ)"
PY=${PYTHON:-python3}
SPARK="docker compose exec -T spark-master"
FAILS=0

step() {
  local name=$1 log=$2; shift 2
  if "$@" >"$log" 2>&1; then echo "PASS  $name -> $log" | tee -a "$SUMMARY"
  else echo "FAIL  $name -> $log (exit $?)" | tee -a "$SUMMARY"; FAILS=$((FAILS + 1)); return 1; fi
}

latest_good_replay() {   # newest evidence/AC04/m3_*/replay_verify.json with match = true and local results
  $PY - <<'EOF'
import json, pathlib
runs = sorted(pathlib.Path("evidence/AC04").glob("m3_*/replay_verify.json"), reverse=True)
for v in runs:
    run = v.parent.name
    if json.loads(v.read_text())["comparison"]["match"] and pathlib.Path(f"data/local/m3/{run}/replay_metrics.jsonl").exists():
        print(run); break
EOF
}

part_products() {
  local E="evidence/AC07/$RUN_ID"; mkdir -p "$E"; SUMMARY="$E/summary.txt"
  local wr=${WINDOW_RUN:-$(latest_good_replay)}
  echo "run_id=$RUN_ID part=products window_run=${wr:-none} commit=$(git rev-parse --short HEAD)" | tee "$SUMMARY"
  [ -n "$wr" ] || { echo "FAIL  no verified M3 replay run found (run scripts/run_m3.sh replay)" | tee -a "$SUMMARY"; FAILS=$((FAILS + 1)); return; }
  cp "evidence/AC04/$wr/replay_verify.json" "$E/window_source_replay_verify.json"
  step "unit tests" "$E/unit_tests.log" $PY -m pytest -q -p no:cacheprovider tests/unit
  step "product Spark tests" "$E/product_tests.log" bash scripts/test_spark.sh tests/data_quality/test_products.py
  step "build products (Spark)" "$E/build.log" $SPARK spark-submit /workspace/src/products/build_products.py \
    --product all --window-run "$wr" --run-id "$RUN_ID" --report-out "/workspace/$E/build_report.json" || return
  step "read-back test (Spark)" "$E/readback.log" $SPARK spark-submit /workspace/src/products/readback.py \
    --build-report "/workspace/$E/build_report.json" --report-out "/workspace/$E/readback_report.json" --run-id "$RUN_ID" || return
  step "contract release blocks" "$E/release.log" $PY -m src.products.release \
    --build "$E/build_report.json" --readback "$E/readback_report.json" --evidence "$E"
  step "hdfs product layout" "$E/hdfs_products.txt" bash -c \
    "docker compose exec -T namenode hdfs dfs -count -h '/datalake/gold/products/*/*' &&
     { docker compose exec -T namenode hdfs dfs -ls -R /datalake/gold/products | grep -v _SUCCESS | head -40 || true; }"
  cp contracts/*_v1.yaml "$E/" 2>/dev/null
  echo "--- failures: $FAILS   evidence: $E" | tee -a "$SUMMARY"
}

part_analytics() {   # T06 SQL analyses over products/Gold -> outputs/reports/analytics/<run>/
  local E="evidence/AC06/$RUN_ID" O="outputs/reports/analytics/$RUN_ID"; mkdir -p "$E" "$O"; SUMMARY="$E/summary.txt"
  echo "run_id=$RUN_ID part=analytics commit=$(git rev-parse --short HEAD)" | tee -a "$SUMMARY"
  step "SQL analyses (Spark)" "$E/analytics.log" $SPARK spark-submit /workspace/src/analytics/sql_analyses.py \
    --out-dir "/workspace/$O" --run-id "$RUN_ID"
  cp "$O/analyses.json" "$E/" 2>/dev/null
  echo "--- failures: $FAILS   evidence: $E  outputs: $O" | tee -a "$SUMMARY"
}

part_benchmark() {   # T06 strategy pairs, 1 warm-up + REPEATS timed runs each -> outputs/benchmarks/<run>/
  local E="evidence/AC06/$RUN_ID" O="outputs/benchmarks/$RUN_ID"; mkdir -p "$E" "$O"; SUMMARY="$E/summary.txt"
  echo "run_id=$RUN_ID part=benchmark repeats=${REPEATS:-3} commit=$(git rev-parse --short HEAD)" | tee -a "$SUMMARY"
  step "benchmark (Spark)" "$E/benchmark.log" $SPARK spark-submit /workspace/src/analytics/benchmark.py \
    --run-id "$RUN_ID" --out-dir "/workspace/$O" --repeats "${REPEATS:-3}"
  step "benchmark layouts on HDFS" "$E/hdfs_bench.txt" docker compose exec -T namenode hdfs dfs -count -h '/datalake/bench/nyc_trips/*'
  cp "$O/benchmark_results.json" "$E/" 2>/dev/null
  echo "--- failures: $FAILS   evidence: $E  outputs: $O" | tee -a "$SUMMARY"
}

for p in "${PARTS[@]}"; do "part_$p"; done
exit "$FAILS"
