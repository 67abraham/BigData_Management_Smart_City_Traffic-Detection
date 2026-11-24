# Screenshots 20261124T065644Z

Taken by `scripts/take_screenshots.py` (headless Chromium, Playwright 1.47) right after run(s) full_20261124T052158Z (m2_20261124T053015Z, m3_20261124T054041Z, m5_20261124T061039Z). The cluster UIs (HDFS, Spark History, Flink) show that run's jobs; the dashboard shows the runs pinned in `configs/dashboard.yaml` (each panel names its run).
Supporting evidence only: the numbers are in the runs' log/JSON files.
Check = the page contained the expected text when the shot was taken.

| File | Shows | URL | Check |
|---|---|---|---|
| `dashboard_pipeline.png` | Dashboard: Pipeline tab (processing volume, runs shown; pinned in configs/dashboard.yaml) | `http://localhost:8501` | PASS |
| `dashboard_quality.png` | Dashboard: Quality tab | `http://localhost:8501` | PASS |
| `dashboard_streaming.png` | Dashboard: Streaming tab (window metrics) | `http://localhost:8501` | PASS |
| `dashboard_performance.png` | Dashboard: Performance tab (benchmark comparison) | `http://localhost:8501` | PASS |
| `dashboard_abnormal.png` | Dashboard: Abnormal cases tab (late/duplicate/malformed, failure + recovery) | `http://localhost:8501` | PASS |
