# Screenshots 20261124T065644Z

Taken by `scripts/take_screenshots.py` (headless Chromium, Playwright 1.47) right after run(s) full_20261124T052158Z (m2_20261124T053015Z, m3_20261124T054041Z, m5_20261124T061039Z). The cluster UIs (HDFS, Spark History, Flink) show that run's jobs; the dashboard shows the runs pinned in `configs/dashboard.yaml` (each panel names its run).
Supporting evidence only: the numbers are in the runs' log/JSON files.
Check = the page contained the expected text when the shot was taken.

| File | Shows | URL | Check |
|---|---|---|---|
| `flink_recovery_checkpoints.png` | Flink UI: checkpoints of recovery job b8ea2dee07db4193710f559601117422, restored after the TaskManager kill | `http://localhost:8082/#/job/completed/b8ea2dee07db4193710f559601117422/checkpoints` | PASS |
