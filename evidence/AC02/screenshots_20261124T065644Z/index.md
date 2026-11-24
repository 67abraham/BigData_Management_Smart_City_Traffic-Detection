# Screenshots 20261124T065644Z

Taken by `scripts/take_screenshots.py` (headless Chromium, Playwright 1.47) right after run(s) full_20261124T052158Z (m2_20261124T053015Z, m3_20261124T054041Z, m5_20261124T061039Z). The cluster UIs (HDFS, Spark History, Flink) show that run's jobs; the dashboard shows the runs pinned in `configs/dashboard.yaml` (each panel names its run).
Supporting evidence only: the numbers are in the runs' log/JSON files.
Check = the page contained the expected text when the shot was taken.

| File | Shows | URL | Check |
|---|---|---|---|
| `hdfs_namenode_overview.png` | HDFS NameNode overview: capacity, live DataNodes, blocks | `http://localhost:9870/dfshealth.html#tab-overview` | PASS |
| `hdfs_datanodes.png` | HDFS DataNodes tab: the three live DataNodes | `http://localhost:9870/dfshealth.html#tab-datanode` | PASS |
| `hdfs_browse_datalake.png` | HDFS file browser: Bronze/Silver/Gold layers under /datalake | `http://localhost:9870/explorer.html#/datalake` | PASS |
