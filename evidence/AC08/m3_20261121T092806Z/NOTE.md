# Note on this run

Recovery worked (Flink REST records): TaskManager killed 09:42:38Z; JobManager detected the lost
TaskManager at 09:43:04.048Z (RESTARTING); RUNNING again at 09:43:14.059Z, restored from checkpoint
chk-7 (completed 09:42:36.666Z); results after restart = oracle (1,854 windows, 10,000 events, 22
re-emitted window results). But the step "recovery: job running again" passed after 0 s for the wrong
reason: it checked before the JobManager had noticed the failure and saw the old RUNNING state.
The check now requires checkpoints.counts.restored >= 1 and state RUNNING (scripts/run_m3.sh
wait_restored); recovery was re-run with it.
