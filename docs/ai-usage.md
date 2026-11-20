# Generative-AI usage declaration (spec §10)

| Date | Tool | What it produced | How it was verified |
|---|---|---|---|
| 2026-11-19 | Claude Code (Anthropic) | M0 environment: `compose.yaml`, Dockerfiles, Hadoop/Spark configs, `scripts/hdfs_init.sh`, `scripts/smoke_m0.sh`, `src/common/config.py`, `src/batch/hdfs_smoke.py`, unit tests | Local: `py_compile`, `bash -n`, pytest. Cluster: `scripts/smoke_m0.sh` in Codespace, output in `evidence/AC02/` |

All numbers in reports come from recorded runs, not from the AI tool.
