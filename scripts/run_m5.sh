#!/usr/bin/env bash
# M5 run: dashboard data export from the products + dashboard tests (spec T09/AC09).
#   bash scripts/run_m5.sh
#   streamlit run src/visualisation/app.py      # then open port 8501
# Evidence: evidence/AC09/m5_<UTC>/; data: outputs/dashboard/m5_<UTC>/ (the dashboard shows the newest).
set -uo pipefail
cd "$(dirname "$0")/.."
RUN_ID="m5_$(date -u +%Y%m%dT%H%M%SZ)"
E="evidence/AC09/$RUN_ID"; O="outputs/dashboard/$RUN_ID"; mkdir -p "$E"
SUMMARY="$E/summary.txt"; FAILS=0
PY=${PYTHON:-python3}
step() {
  local name=$1 log=$2; shift 2
  echo "RUN   $name"   # progress line for the terminal (run_all.sh shows it live); not in the summary
  if "$@" >"$log" 2>&1; then echo "PASS  $name -> $log" | tee -a "$SUMMARY"
  else echo "FAIL  $name -> $log (exit $?)" | tee -a "$SUMMARY"; FAILS=$((FAILS + 1)); return 1; fi
}
echo "run_id=$RUN_ID commit=$(git rev-parse --short HEAD)" | tee "$SUMMARY"
step "export dashboard data (Spark)" "$E/export.log" docker compose exec -T spark-master spark-submit \
  /workspace/src/visualisation/export_dashboard_data.py --run-id "$RUN_ID" --out-dir "/workspace/$O" || exit 1
cp "$O/manifest.json" "$O/layer_counts.json" "$E/"
if $PY -c "import streamlit" 2>/dev/null; then
  step "dashboard renders (AppTest)" "$E/dashboard_tests.log" $PY -m pytest -q -p no:cacheprovider tests/integration/test_dashboard.py
else
  echo "SKIP  dashboard tests: streamlit not installed (pip install -r requirements-dashboard.txt)" | tee -a "$SUMMARY"
fi
echo "--- failures: $FAILS   evidence: $E  data: $O" | tee -a "$SUMMARY"
exit "$FAILS"
