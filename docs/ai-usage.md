# Generative-AI usage declaration (spec §10)

| Date | Tool | What it produced | How it was verified |
|---|---|---|---|
| 2026-11-19 | Claude Code (Anthropic) | M0 environment: `compose.yaml`, Dockerfiles, Hadoop/Spark configs, `scripts/hdfs_init.sh`, `scripts/smoke_m0.sh`, `src/common/config.py`, `src/batch/hdfs_smoke.py`, unit tests | Local: `py_compile`, `bash -n`, pytest. Cluster: `scripts/smoke_m0.sh` in Codespace, 8/8 PASS (runs `m0_20261121T013641Z`, `m0_20261121T014208Z` with stricter result checks), output in `evidence/AC02/` |
| 2026-11-21 | Claude Code (Anthropic) | M1 ingestion: `src/ingestion/*`, `configs/sources.yaml`, `scripts/ingest_m1.sh`, `docs/source_inventory.md`, `docs/storage_layout.md`, unit tests | Local: pytest (fetch/skip/force logic, range-zip reader, sampling); range reader tested against the real KITTI archive listing. Cluster: pending `scripts/ingest_m1.sh` run |

All numbers in reports come from recorded runs, not from the AI tool.
