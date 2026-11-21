#!/usr/bin/env bash
# M3 run: Kafka historical event replay -> Flink event-time windows (spec T04/AC04, T08/AC08).
# Run in the Codespace from the project root with the stack up and M2 Silver UCI present.
#   bash scripts/run_m3.sh                 # replay + edge + recovery
#   bash scripts/run_m3.sh replay          # full replay, bounded Flink job, checked against the oracle
#   bash scripts/run_m3.sh edge            # late / duplicate / malformed / out-of-order test topics
#   bash scripts/run_m3.sh recovery        # kill the TaskManager mid-replay, restart, check results
# Topics of the scenario being run are deleted and re-created first, so every run starts clean.
# Evidence: evidence/AC04/m3_<UTC>/ (replay, edge) and evidence/AC08/m3_<UTC>/ (recovery).
# Large JSONL (produced events, collected records) stay in data/local/m3/<run>/ (git-ignored).
set -uo pipefail
cd "$(dirname "$0")/.."

PARTS=("$@"); [ ${#PARTS[@]} -eq 0 ] && PARTS=(replay edge recovery)
RUN_ID="m3_$(date -u +%Y%m%dT%H%M%SZ)"
A4="evidence/AC04/$RUN_ID"; A8="evidence/AC08/$RUN_ID"; L="data/local/m3/$RUN_ID"
mkdir -p "$A4" "$L"
SUMMARY="$A4/summary.txt"; FAILS=0
PY=${PYTHON:-python3}
FLINK_REST=${FLINK_REST:-http://localhost:8082}
TOPICS="docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092"
SPARK="docker compose exec -T spark-master"

step() {
  local name=$1 log=$2; shift 2
  if "$@" >"$log" 2>&1; then echo "PASS  $name -> $log" | tee -a "$SUMMARY"
  else echo "FAIL  $name -> $log (exit $?)" | tee -a "$SUMMARY"; FAILS=$((FAILS + 1)); return 1; fi
}

recreate_topics() {  # recreate_topics <topic>...  (1 partition each, see configs/streaming.yaml)
  for t in "$@"; do $TOPICS --delete --if-exists --topic "$t"; done
  sleep 3
  # retention.ms=-1: keep the (small) replay data; LogAppendTime: Kafka's record time is the append
  # time. Flink stamps sink records with their EVENT time (2012-2018), and with the default 7-day
  # retention Kafka deleted most window results at once (run m3_20261121T091418Z).
  for t in "$@"; do $TOPICS --create --topic "$t" --partitions 1 --replication-factor 1 \
      --config retention.ms=-1 --config message.timestamp.type=LogAppendTime || return 1; done
  $TOPICS --list
}

job_state() { curl -sf "$FLINK_REST/jobs/$1" | $PY -c "import json,sys; print(json.load(sys.stdin)['state'])"; }

wait_running() {  # wait_running <job_id> <timeout_s>: job RUNNING and all its tasks RUNNING
  local id=$1 t=$2 i=0
  while [ $i -lt "$t" ]; do
    if curl -sf "$FLINK_REST/jobs/$id" | $PY -c "
import json,sys; j=json.load(sys.stdin)
ok = j['state']=='RUNNING' and all(v['status']=='RUNNING' for v in j['vertices'])
sys.exit(0 if ok else 1)"; then echo "job $id RUNNING after ${i}s"; return 0; fi
    sleep 2; i=$((i + 2))
  done
  echo "job $id not RUNNING after ${t}s (state $(job_state "$id"))"; return 1
}

wait_restored() {  # wait_restored <job_id> <timeout_s>: job restarted FROM A CHECKPOINT and RUNNING again
  # (checking only state RUNNING right after the kill passes too early: the JobManager notices a lost
  # TaskManager only after its heartbeat timeout; run m3_20261121T092806Z)
  local id=$1 t=$2 i=0
  while [ $i -lt "$t" ]; do
    if $PY - "$FLINK_REST" "$id" <<'EOF2'
import json, sys, urllib.request
base, jid = sys.argv[1], sys.argv[2]
get = lambda p: json.load(urllib.request.urlopen(f"{base}/jobs/{jid}{p}"))
j, c = get(""), get("/checkpoints")
r = (c.get("latest") or {}).get("restored")
ok = j["state"] == "RUNNING" and all(v["status"] == "RUNNING" for v in j["vertices"]) and c["counts"]["restored"] >= 1
print(f"state={j['state']} restored={c['counts']['restored']} from={r and r.get('external_path')}")
sys.exit(0 if ok else 1)
EOF2
    then echo "restored and RUNNING after ${i}s"; return 0; fi
    sleep 3; i=$((i + 3))
  done
  return 1
}

submit_detached() {  # submit_detached <mode> <run_id> -> prints job id
  docker compose exec -T flink-jobmanager flink run -d -py /workspace/src/streaming/flink_window_job.py \
    --mode "$1" --run-id "$2" 2>&1 | tee -a "$3" | sed -n 's/.*JobID \([0-9a-f]\{32\}\).*/\1/p' | tail -1
}

wait_caught_up() {  # wait_caught_up <group> <timeout_s>: Flink's source has consumed everything
  local g=$1 t=$2 i=0 zero=0 lag
  while [ $i -lt "$t" ]; do
    lag=$(docker compose exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:9092 \
          --describe --group "$g" 2>/dev/null | awk '$6 ~ /^[0-9]+$/ {s += $6; n++} END {print (n ? s : "NA")}')
    echo "t=${i}s lag=$lag"
    if [ "$lag" = 0 ]; then zero=$((zero + 1)); [ $zero -ge 2 ] && { sleep 20; return 0; }; else zero=0; fi
    sleep 10; i=$((i + 10))
  done
  return 1
}

collect() {  # collect <topic> <out.jsonl> <idle_s>
  $SPARK python3 -m src.streaming.collect --topic "$1" --out "/workspace/$2" --idle "$3"
}

flink_evidence() {  # flink_evidence <job_id> <dir> <prefix>: job, checkpoint and exception records
  curl -sf "$FLINK_REST/jobs/$1" >"$2/$3_job.json"
  curl -sf "$FLINK_REST/jobs/$1/checkpoints" >"$2/$3_checkpoints.json"
  curl -sf "$FLINK_REST/jobs/$1/exceptions" >"$2/$3_exceptions.json"
  $PY - "$2/$3_job.json" "$2/$3_checkpoints.json" <<'EOF'
import json, sys
j, c = json.load(open(sys.argv[1])), json.load(open(sys.argv[2]))
print("job", j["jid"], j["name"], j["state"], "duration_ms", j["duration"])
for v in j["vertices"]:
    m = v["metrics"]
    print(f"  {v['name'][:60]:60s} par={v['parallelism']} in={m['read-records']} out={m['write-records']}")
print("checkpoints", c["counts"])
EOF
}

# ---------------- replay: full Silver UCI replay, bounded job ----------------
part_replay() {
  local id="${RUN_ID}_replay" log="$A4/replay_flink.log"
  step "replay: topics" "$A4/replay_topics.log" recreate_topics uci-traffic-replay-v1 uci-traffic-window-metrics-v1 uci-traffic-replay-dlq-v1 || return
  step "replay: produce Silver UCI events" "$A4/replay_producer.log" $SPARK spark-submit \
    /workspace/src/streaming/replay_producer.py --run-id "$id" \
    --events-out "/workspace/$L/replay_produced.jsonl" --summary-out "/workspace/$A4/replay_producer.json" || return
  step "replay: Flink bounded job" "$log" docker compose exec -T flink-jobmanager flink run \
    -py /workspace/src/streaming/flink_window_job.py --mode replay --run-id "$id" --bounded || return
  local jid; jid=$(sed -n 's/.*JobID \([0-9a-f]\{32\}\).*/\1/p' "$log" | tail -1)
  step "replay: Flink job record" "$A4/replay_flink_job.txt" flink_evidence "$jid" "$A4" replay
  step "replay: consume metrics" "$A4/replay_collect_metrics.log" collect uci-traffic-window-metrics-v1 "$L/replay_metrics.jsonl" 10
  step "replay: consume DLQ" "$A4/replay_collect_dlq.log" collect uci-traffic-replay-dlq-v1 "$L/replay_dlq.jsonl" 5
  head -n 5 "$L/replay_metrics.jsonl" >"$A4/replay_metrics_sample.jsonl"
  step "replay: results = oracle" "$A4/replay_verify.log" $PY -m src.streaming.verify \
    --events "$L/replay_produced.jsonl" --metrics "$L/replay_metrics.jsonl" --dlq "$L/replay_dlq.jsonl" \
    --out "$A4/replay_verify.json" --job-run-id "$id"
}

# ---------------- edge: late / duplicate / malformed / out-of-order ----------------
part_edge() {
  local id="${RUN_ID}_edge" log="$A4/edge_flink_submit.log"
  step "edge: test topics" "$A4/edge_topics.log" recreate_topics test-uci-traffic-replay-v1 test-uci-traffic-window-metrics-v1 test-uci-traffic-replay-dlq-v1 || return
  local jid; jid=$(submit_detached test "$id" "$log")
  [ -n "$jid" ] || { echo "FAIL  edge: submit (no job id) -> $log" | tee -a "$SUMMARY"; FAILS=$((FAILS + 1)); return; }
  step "edge: job running" "$A4/edge_wait.log" wait_running "$jid" 120 || return
  sleep 10   # let the Kafka source assign its partition before the first phase
  step "edge: produce scenario (paused phases)" "$A4/edge_producer.log" $SPARK python3 -m src.streaming.scenario \
    --run-id "$id" --events-out "/workspace/$L/edge_produced.jsonl"
  sleep 15   # Python bundles flush every 1 s; give the last firings time to reach Kafka
  step "edge: consume metrics" "$A4/edge_collect_metrics.log" collect test-uci-traffic-window-metrics-v1 "$L/edge_metrics.jsonl" 8
  step "edge: consume DLQ" "$A4/edge_collect_dlq.log" collect test-uci-traffic-replay-dlq-v1 "$L/edge_dlq.jsonl" 5
  cp "$L/edge_produced.jsonl" "$L/edge_metrics.jsonl" "$L/edge_dlq.jsonl" "$A4/"   # small: keep in evidence
  flink_evidence "$jid" "$A4" edge >"$A4/edge_flink_job.txt" 2>&1
  docker compose exec -T flink-jobmanager flink cancel "$jid" >"$A4/edge_cancel.log" 2>&1
  step "edge: expected results" "$A4/edge_check.log" $PY - "$A4" <<'EOF'
import json, sys
from src.streaming.scenario import check
d = sys.argv[1]
val = lambda f: [json.loads(json.loads(l)["value"]) for l in open(f"{d}/{f}") if l.strip()]
r = check(val("edge_metrics.jsonl"), val("edge_dlq.jsonl"))
json.dump(r, open(f"{d}/edge_check.json", "w"), indent=2)
print(json.dumps(r, indent=2))
sys.exit(0 if r["passed"] else 1)
EOF
}

# ---------------- recovery: kill the TaskManager mid-replay (AC08) ----------------
part_recovery() {
  mkdir -p "$A8"
  # Measured replay throughput is ~59 events/s (run m3_20261121T091418Z), so the recovery test replays
  # the first 10,000 events (spec §4 minimum) at 50/s and kills the TaskManager after 60 s.
  local id="${RUN_ID}_recovery" log="$A8/flink_submit.log" rate=${RECOVERY_RATE:-50} kill_after=${KILL_AFTER:-60}
  local limit=${RECOVERY_LIMIT:-10000}
  step "recovery: topics" "$A8/topics.log" recreate_topics uci-traffic-replay-v1 uci-traffic-window-metrics-v1 uci-traffic-replay-dlq-v1 || return
  local jid; jid=$(submit_detached replay "$id" "$log")
  [ -n "$jid" ] || { echo "FAIL  recovery: submit (no job id) -> $log" | tee -a "$SUMMARY"; FAILS=$((FAILS + 1)); return; }
  step "recovery: job running" "$A8/wait1.log" wait_running "$jid" 120 || return
  sleep 10
  echo "producing at $rate events/s in the background" | tee -a "$SUMMARY"
  $SPARK spark-submit /workspace/src/streaming/replay_producer.py --run-id "$id" --rate "$rate" --limit "$limit" --flush-marker \
    --events-out "/workspace/$L/recovery_produced.jsonl" --summary-out "/workspace/$A8/producer.json" \
    >"$A8/producer.log" 2>&1 &
  local ppid=$!
  sleep "$kill_after"
  curl -sf "$FLINK_REST/jobs/$jid/checkpoints" >"$A8/checkpoints_before_kill.json"
  date -u +%FT%TZ >"$A8/kill_time.txt"
  step "recovery: kill TaskManager" "$A8/kill.log" docker compose kill flink-taskmanager
  sleep 20
  step "recovery: start TaskManager" "$A8/start.log" docker compose start flink-taskmanager
  step "recovery: restored from checkpoint, running" "$A8/wait2.log" wait_restored "$jid" 240
  wait $ppid; echo "producer exit $?" >>"$A8/producer.log"
  # drain: the restarted job re-reads from the last checkpoint and must catch up (consumer lag 0)
  step "recovery: Flink caught up" "$A8/catch_up.log" wait_caught_up "bdfp-flink-replay-$id" 1200
  step "recovery: consume metrics" "$A8/collect_metrics.log" collect uci-traffic-window-metrics-v1 "$L/recovery_metrics.jsonl" 15
  step "recovery: consume DLQ" "$A8/collect_dlq.log" collect uci-traffic-replay-dlq-v1 "$L/recovery_dlq.jsonl" 5
  flink_evidence "$jid" "$A8" recovery >"$A8/flink_job.txt" 2>&1
  docker compose exec -T flink-jobmanager flink cancel "$jid" >"$A8/cancel.log" 2>&1
  step "recovery: results = oracle after restart" "$A8/verify.log" $PY -m src.streaming.verify \
    --events "$L/recovery_produced.jsonl" --metrics "$L/recovery_metrics.jsonl" --dlq "$L/recovery_dlq.jsonl" \
    --out "$A8/verify.json" --job-run-id "$id"
  cp "$SUMMARY" "$A8/summary.txt"
}

echo "run_id=$RUN_ID parts=${PARTS[*]} commit=$(git rev-parse --short HEAD)" | tee "$SUMMARY"
step "unit tests" "$A4/unit_tests.log" $PY -m pytest -q -p no:cacheprovider tests/unit
for p in "${PARTS[@]}"; do
  echo "=== $p" | tee -a "$SUMMARY"
  "part_$p"
done
echo "--- failures: $FAILS   evidence: $A4 ${A8}" | tee -a "$SUMMARY"
[ -d "$A8" ] && cp "$SUMMARY" "$A8/summary.txt"
exit "$FAILS"
