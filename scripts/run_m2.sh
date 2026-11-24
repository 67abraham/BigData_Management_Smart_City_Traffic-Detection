#!/usr/bin/env bash
# M2 run: tests, then Spark Silver jobs with quality rules (spec T03/AC03, T05/AC05).
# Run in the Codespace from the project root, with the stack up and M1 Bronze present.
#   bash scripts/run_m2.sh               # all M2 datasets implemented so far
#   bash scripts/run_m2.sh uci_metro open_meteo   # Silver only
#   traffic_weather = Gold join of the two Silver datasets (run after both)
#   nyc_zone_hourly = Gold NYC aggregation (run after nyc_tlc)
# Evidence (same run id in both):
#   evidence/AC03/m2_<UTC>/  Spark logs, explain plan, before/after samples, reconciliation, HDFS layout
#   evidence/AC05/m2_<UTC>/  test logs, rule catalogue, quality JSON + Markdown report
set -uo pipefail
cd "$(dirname "$0")/.."

SOURCES=("$@"); [ ${#SOURCES[@]} -eq 0 ] && SOURCES=(uci_metro open_meteo traffic_weather nyc_tlc nyc_zone_hourly)
RUN_ID="m2_$(date -u +%Y%m%dT%H%M%SZ)"
A3="evidence/AC03/$RUN_ID"; A5="evidence/AC05/$RUN_ID"; mkdir -p "$A3" "$A5"
SUMMARY="$A5/summary.txt"; FAILS=0
PY=${PYTHON:-python3}

step() {  # step <name> <logfile> <command...>; PASS only if the command (a real check) succeeds
  local name=$1 log=$2; shift 2
  echo "RUN   $name"   # progress line for the terminal (run_all.sh shows it live); not in the summary
  if "$@" >"$log" 2>&1; then echo "PASS  $name -> $log" | tee -a "$SUMMARY"
  else echo "FAIL  $name -> $log (exit $?)" | tee -a "$SUMMARY"; FAILS=$((FAILS + 1)); return 1; fi
}

silver() {  # silver <source>: spark-submit, then split its stdout into evidence files
  local s=$1 log="$A3/silver_$1.log"
  docker compose exec -T spark-master spark-submit "/workspace/src/batch/silver_${s/_metro/}.py" \
    --run-id "$RUN_ID" --report-out "/workspace/$A5/quality_$s.json" \
    --explain-out "/workspace/$A3/explain_silver_$s.txt" >"$log" 2>&1
  local rc=$?
  [ -s "$A5/quality_$s.json" ] || { echo "job wrote no report ($A5/quality_$s.json)" >&2; return 1; }
  $PY - "$A5/quality_$s.json" "$A3" "$s" <<'EOF' || return 1
import json, sys
rep = json.load(open(sys.argv[1]))
out, s = sys.argv[2], sys.argv[3]
json.dump(rep.pop("samples"), open(f"{out}/samples_{s}.json", "w"), indent=2, default=str)
json.dump({k: rep[k] for k in ("run_id", "spark_app_id", "input", "output", "reconciliation", "outcomes")},
          open(f"{out}/reconciliation_{s}.json", "w"), indent=2)
json.dump(rep, open(sys.argv[1], "w"), indent=2, default=str)
checks = {c["name"]: c["ok"] for c in rep.get("checks", [])}
if "completeness" in rep:
    checks["all_expected_files_present"] = rep["completeness"]["all_expected_files_present"]
ok = rep["reconciliation"]["reconciled"] and all(checks.values())
print("reconciliation:", rep["reconciliation"], "checks:", checks)
sys.exit(0 if ok else 1)
EOF
  return $rc
}

echo "run_id=$RUN_ID sources=${SOURCES[*]}" | tee "$SUMMARY"
git rev-parse --short HEAD >>"$SUMMARY" 2>&1; git status --short src configs tests scripts >>"$SUMMARY" 2>&1

step "unit tests"         "$A5/unit_tests.log"         $PY -m pytest -q -p no:cacheprovider tests/unit
step "data-quality tests" "$A5/data_quality_tests.log" bash scripts/test_spark.sh tests/data_quality

gold() {  # gold <dataset>: Gold job; PASS needs the job's own reconciliation (exit code) and a report
  local d=$1
  docker compose exec -T spark-master spark-submit "/workspace/src/batch/gold_${d}.py" --run-id "$RUN_ID" \
    --report-out "/workspace/$A3/gold_$d.json" --explain-out "/workspace/$A3/explain_gold_$d.txt" \
    >"$A3/gold_$d.log" 2>&1
  local rc=$?
  [ -s "$A3/gold_$d.json" ] || { echo "job wrote no report" >&2; return 1; }
  $PY -c "import json,sys; r=json.load(open(sys.argv[1])); print(r['reconciliation']); [print(k, r[k]) for k in ('matches', 'alignment_check', 'coverage') if k in r]" "$A3/gold_$d.json"
  return $rc
}

for s in "${SOURCES[@]}"; do
  echo "=== $s" | tee -a "$SUMMARY"
  if [ "$s" = traffic_weather ] || [ "$s" = nyc_zone_hourly ]; then
    step "$s gold join (Spark)" "$A3/gold_$s.status" gold "$s"
    continue
  fi
  step "$s rule catalogue" "$A5/rules_$s.json" $PY -m "src.quality.${s/_metro/}_rules" || continue
  step "$s silver (Spark)" "$A3/silver_$s.status" silver "$s" || continue
  step "$s quality report" "$A5/quality_report_$s.md" $PY -m src.quality.report_md "$A5/quality_$s.json" "$A5/rules_$s.json"
done

step "hdfs silver/gold layout" "$A3/hdfs_silver_gold_layout.txt" bash -c \
  "docker compose exec -T namenode hdfs dfs -count -h '/datalake/silver/*/*/*' '/datalake/gold/*/*' &&
   { docker compose exec -T namenode hdfs dfs -ls -R /datalake/silver /datalake/gold | grep -v _SUCCESS | head -80 || true; }"  # listing informational
echo "--- failures: $FAILS   evidence: $A3 $A5" | tee -a "$SUMMARY"
cp "$SUMMARY" "$A3/summary.txt"
exit "$FAILS"
