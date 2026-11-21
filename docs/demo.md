# Demonstration script (D10)

Order required by spec §9: ingestion → storage → Spark → Kafka/Flink → quality report → SQL/performance
→ product read-back → visualisation → one failure/recovery scenario. Each step has a live command (fast
enough to run in front of an audience) and the recorded evidence to show if time is short.

| # | Step | Live (≈ time) | Show |
|---|---|---|---|
| 1 | Ingestion | `bash scripts/ingest_m1.sh uci_metro` (≈1 min; every file `unchanged`) | `evidence/AC01/m1_20261121T072608Z/ingestion_report.md`; `docs/source_inventory.md` |
| 2 | Storage | `docker compose exec namenode hdfs dfsadmin -report`; `hdfs dfs -ls -R /datalake/silver` | NameNode UI :9870 (3 live DataNodes); `docs/storage_layout.md` |
| 3 | Spark | `bash scripts/run_m2.sh uci_metro open_meteo traffic_weather` (≈4 min) | Spark History :18080; `evidence/AC03/m2_20261121T084148Z/explain_gold_nyc_zone_hourly.txt` (Map–Shuffle–Reduce); `docs/spark_processing.md` |
| 4 | Kafka/Flink | `bash scripts/run_m3.sh edge` (≈2 min) | Flink UI :8082; `evidence/AC04/m3_20261121T091237Z/edge_check.json` (late update, too_late, duplicates, malformed) |
| 5 | Quality report | — | `evidence/AC05/m2_20261121T083320Z/quality_report_nyc_tlc.md`; dashboard tab *Quality* |
| 6 | SQL / performance | `bash scripts/run_m4.sh analytics` (≈1 min) | `docs/analytics.md`; `docs/performance.md`; dashboard tab *Performance* |
| 7 | Product read-back | `bash scripts/run_m4.sh products` (≈3 min) | `contracts/traffic_features_v1.yaml` release block; `evidence/AC07/m4_20261121T115736Z/readback_report.json` |
| 8 | Visualisation | `bash scripts/start_dashboard.sh` (prints the link) | all tabs; each panel names its source file and run |
| 9 | Failure / recovery | `bash scripts/run_m3.sh recovery` (≈6 min) | dashboard tab *Abnormal cases* (timeline: kill → RESTARTING → restored chk-7); `evidence/AC08/m3_20261121T094727Z/` |

Talking points: data is real and its provenance is checked (SHA-256 in HDFS); replay is labelled
historical, never live; no join without a defensible key; quality never drops records silently;
failed runs are kept and explained (dashboard tab *Abnormal cases*); delivery guarantee stated as
verified (at-least-once, effectively-once results with upsert).
