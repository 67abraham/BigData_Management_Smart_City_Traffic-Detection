# Interrupted run, kept as evidence

The Codespace stopped during the Open-Meteo Spark Bronze step (the log ends mid-job; no summary line
was written). The raw files had landed in HDFS; Bronze records were not written. Re-run after
`docker compose up -d` as m1_20261121T070714Z (PASS, 52,608 rows): the jobs are idempotent, so a re-run
is the recovery.
