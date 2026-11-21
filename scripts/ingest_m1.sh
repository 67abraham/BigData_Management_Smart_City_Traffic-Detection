#!/usr/bin/env bash
# M1 ingestion run (spec T01/AC01): fetch -> land in HDFS Bronze raw -> Spark Bronze records + profile.
# Run in the Codespace from the project root, with the stack up (docker compose up -d).
#   bash scripts/ingest_m1.sh                 # all sources
#   bash scripts/ingest_m1.sh uci_metro nyc_tlc
#   FORCE=1 bash scripts/ingest_m1.sh         # re-download everything (re-ingestion test)
# Evidence: evidence/AC01/m1_<UTC timestamp>/ (logs, manifests, profiles, ingestion_report.md).
set -uo pipefail
cd "$(dirname "$0")/.."

SOURCES=("$@"); [ ${#SOURCES[@]} -eq 0 ] && SOURCES=(uci_metro open_meteo nyc_tlc kitti)
D=$(date -u +%Y-%m-%d)
RUN_ID="m1_$(date -u +%Y%m%dT%H%M%SZ)"
OUT="evidence/AC01/${RUN_ID}"; mkdir -p "$OUT"
SUMMARY="$OUT/summary.txt"; FAILS=0
FORCE_FLAG=""; [ "${FORCE:-0}" = "1" ] && FORCE_FLAG="--force"
[ -f .env ] && { set -a; . ./.env; set +a; }     # KITTI_* download URLs live only in .env
PY=${PYTHON:-python3}

step() {  # step <name> <logfile> <command...>
  local name=$1 log=$2; shift 2
  if "$@" >"$OUT/$log" 2>&1; then echo "PASS  $name -> $log" | tee -a "$SUMMARY"
  else echo "FAIL  $name -> $log (exit $?)" | tee -a "$SUMMARY"; FAILS=$((FAILS + 1)); return 1; fi
}
nn() { docker compose exec -T namenode "$@"; }

land() {  # copy the landing partition into HDFS Bronze raw (replace = idempotent re-run)
  local s=$1 dst="/datalake/bronze/$1/raw"
  nn hdfs dfs -mkdir -p "$dst" &&
  nn hdfs dfs -rm -r -f -skipTrash "$dst/ingest_date=$D" &&
  nn hdfs dfs -put "/workspace/data/local/landing/$s/ingest_date=$D" "$dst/" &&
  nn hdfs dfs -count -h "$dst/ingest_date=$D" || return 1
  # listing is informational only: head closes the pipe early (SIGPIPE), which must not fail the step
  nn hdfs dfs -ls -R "$dst/ingest_date=$D" | head -40 || true
  # PASS only if every landed file reached HDFS
  local want got
  want=$(find "data/local/landing/$s/ingest_date=$D" -type f | wc -l)
  got=$(nn hdfs dfs -count "$dst/ingest_date=$D" | awk '{print $2}')
  echo "file count: local=$want hdfs=$got"
  [ -n "$got" ] && [ "$want" -eq "$got" ]
}

bronze() {  # Spark job; it writes its profile straight to profile_<source>.json (not via stdout)
  local s=$1 log="$OUT/bronze_$1.log"
  docker compose exec -T spark-master spark-submit /workspace/src/ingestion/bronze_load.py \
    --source "$s" --ingest-date "$D" --profile-out "/workspace/$OUT/profile_$s.json" >"$log" 2>&1
  local rc=$?
  $PY -c "import json,sys; json.load(open(sys.argv[1]))" "$OUT/profile_$s.json" || { echo "profile missing or not valid JSON" >&2; return 1; }
  return $rc
}

echo "run_id=$RUN_ID ingest_date=$D sources=${SOURCES[*]} force=${FORCE:-0}" | tee "$SUMMARY"
docker compose exec -T namenode hdfs dfsadmin -safemode get >>"$SUMMARY" 2>&1

for s in "${SOURCES[@]}"; do
  echo "=== $s" | tee -a "$SUMMARY"
  # (re-)locate unless a previous lookup already chose a unique station
  if [ "$s" = open_meteo ] && ! $PY -c 'import json,sys; sys.exit(0 if json.load(open("data/manifests/atr301_location.json")).get("chosen") else 1)' 2>/dev/null; then
    step "locate ATR 301 (MnDOT GIS)" locate_atr301.log $PY -m src.ingestion.locate_atr301 || continue
  fi
  case $s in
    uci_metro)  mod=fetch_uci ;;
    open_meteo) mod=fetch_open_meteo ;;
    nyc_tlc)    mod=fetch_nyc_tlc ;;
    kitti)      mod=fetch_kitti ;;
    *) echo "unknown source $s" | tee -a "$SUMMARY"; FAILS=$((FAILS + 1)); continue ;;
  esac
  step "$s fetch"        "fetch_$s.log" $PY -m "src.ingestion.$mod" --ingest-date "$D" $FORCE_FLAG
  # land even after a partial fetch failure: the manifest records what failed
  [ -f "data/local/landing/$s/ingest_date=$D/_manifest.json" ] || continue
  cp "data/local/landing/$s/ingest_date=$D/_manifest.json" "$OUT/manifest_$s.json"
  step "$s land in HDFS" "land_$s.log" land "$s" || continue
  step "$s bronze (Spark)" "bronze_$s.log.status" bronze "$s"
done

step "hdfs bronze layout" hdfs_bronze_layout.txt bash -c \
  "docker compose exec -T namenode hdfs dfs -du -h /datalake/bronze/*/*; docker compose exec -T namenode hdfs dfs -count -h /datalake/bronze/*/*"
step "ingestion report"   report.log $PY -m src.ingestion.report "$OUT"
echo "--- failures: $FAILS   evidence: $OUT" | tee -a "$SUMMARY"
exit "$FAILS"
