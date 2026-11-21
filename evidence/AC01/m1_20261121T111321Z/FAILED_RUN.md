# Fresh-environment run, attempt 1 (kept as evidence)

Started after `docker compose down -v` (all HDFS/Kafka/Flink volumes removed) with data/local moved
aside, so ingestion really downloaded every source. Stages 00 (stack, 22 s), 01 (M0 smoke, 84 s) passed;
stage 02 (M1 ingestion) completed every fetch, HDFS landing and Spark Bronze step for all four sources
(evidence/AC01/m1_20261121T111321Z) but FAILED at the final report step: profile_kitti.json was invalid
JSON because a Spark INFO log line was written into the middle of the job's PROFILE_JSON stdout line.
The same bug had been fixed for the M2 jobs but not carried back to bronze_load.py. Fix: bronze_load.py
writes its profile to a file (--profile-out); ingest_m1.sh validates it. run_all.sh re-run follows.
