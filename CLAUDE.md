# CLAUDE.md — Smart City Traffic Data Platform (Big Data final project)

Student ID: 241091038. All project work lives in `04-projects/241091038/`. Never edit other students' folders, the course materials, or `04-projects/README.md`.

## Source of truth: the spec
`docs/spec/final-project-task-specification.md` is authoritative. This file is only a summary and checklist. It is a byte-identical copy of the course file `04-projects/final-project-task-specification.md` (copied 2026-11-21), kept in the project so the Codespace has it too. If the course publishes a new version, re-copy it on the Mac (`cp ../final-project-task-specification.md docs/spec/`) and note the date here.
- **Before any technical decision, read the relevant spec section first** and cite it in your reasoning (e.g. "spec §5 T04"). That includes: tool or version choice, schemas, storage layout and partitioning, quality rules, window/watermark design, contract fields, benchmark design, and acceptance evidence.
- If this file and the spec disagree, the spec wins. Then fix this file.
- Section map: §3 scope and technology boundaries · §4 data sources and minimum volumes · §5 tasks T01–T09 · §6 extensions · §7 product interface and R0/R1 · §8 acceptance AC01–AC09 · §9 deliverables and directory layout · §10 rubric and integrity. Architecture diagram: `docs/spec/assets/BD-FP-Architecture-v1.0.en.png` (and `.svg`).

## What we are building
A big-data **engineering** platform, not an ML model and not a web app:
real sources → HDFS Bronze/Silver/Gold → Spark batch → Kafka + Flink event-time streaming (historical replay) → quality and governance → 4 versioned data products with contracts → light Streamlit dashboard.
Marks are 75% data engineering, 5% visualisation, so pipelines and evidence come before UI polish.

## Environment
- **The stack runs only in GitHub Codespaces (16 GB, Docker).** The local Mac has 4 GB RAM, almost no free disk (~150 MB) and no Docker, so don't start Docker, Spark, Kafka, Flink or big downloads locally. Local work is limited to editing code and light Python checks.
- Git remotes: `origin` = Gitee fork (where you submit). The GitHub repo used for Codespaces holds **only this student folder at its root**, so every path must be relative to the project root (never `04-projects/241091038/...`), and `.devcontainer/` is picked up by Codespaces there.
- Stack: HDFS 3.3 (1 NameNode + 3 DataNodes, replication = 2), PySpark 3.5, Kafka 3.x (KRaft), PyFlink 1.18, pytest, Streamlit. Define everything in `compose.yaml` and `.devcontainer/`.

## Directory layout
```
compose.yaml  .env.example  .devcontainer/  configs/  contracts/  docs/
src/{ingestion,batch,streaming,quality,products,analytics,visualisation}/
tests/{unit,integration,data_quality,streaming}/
scripts/  notebooks/  data/manifests/  data/local/ (git-ignored)
outputs/{reports,benchmarks}/  evidence/AC01..AC09/
```

## Data sources and rules
| Source | Role | Notes |
|---|---|---|
| UCI Metro Interstate Traffic Volume | hourly traffic, Kafka replay source | I-94 Minneapolis–St Paul, 2012–2018, ~48K rows; keep the timezone interpretation explicit |
| Open-Meteo Historical API | API ingestion and weather join | Query the same lat/lon and period as UCI; label it as reanalysis data, not station observations |
| NYC TLC Trip Records | 1M+ row Spark batch and benchmarks | A different city, so publish it as a separate product and **never join it to UCI** |
| KITTI Object Detection | `road_image_manifest_v1` | Manifest only; never commit or redistribute images |

**Minimum volumes:** ≥10K records for the functional test · ≥1M **distinct** source records for the batch test (NYC TLC; repeated copies don't count) · ≥10K event-time records for the replay test. If a threshold can't be met, get instructor approval first and document it.

## Data products (T07)
| Product | Required content | Consumer |
|---|---|---|
| `traffic_hourly_v1` | hour, location/direction, volume, units, source, timezone explanation | analytics, ML |
| `traffic_features_v1` | time features; lags computed only from data available at prediction time; target definition; temporal split description | ML features |
| `road_image_manifest_v1` | image_id, controlled path, size/format, label path, origin, split, file availability | CV loading |
| `traffic_window_metrics_v1` | event-time window, metric, granularity, late-event handling, origin | streaming, dashboard |

Each product needs a YAML contract in `contracts/` covering: name/version, fields, types, units, granularity, keys/uniqueness, time semantics, source and **licence**, temporal/spatial coverage, quality rules and **quality summary**, row count/size, location, reproducible production method, reading instructions, and **known limitations**. Each product also needs a read-back test. Structured products are stored as Parquet; images get a manifest only. Training an ML/CV model is NOT required.
Cross-course: R0 is the baseline snapshot (supplied or approved by the course), and R1 is our verified output. ML/CV work must never be blocked by our pipeline.

## Hard rules (integrity: breaking these loses marks)
- Never invent numbers. Every throughput, latency, row count, quality score or benchmark figure must come from a recorded run, with logs saved under `evidence/ACxx/`.
- No fabricated joins: if there's no defensible spatial or temporal key, publish the datasets separately.
- Label replayed events as **historical event replay**, never as live data.
- Keep injected anomalies clearly separate from real records (separate files, or a flag column).
- Don't claim end-to-end exactly-once; state the scope that was actually verified (normally at-least-once).
- Spark is the batch backbone. Don't replace it with Pandas (Pandas is fine for small reports and plots).
- Benchmarks: keep logic and data constant, run at least 3 times, and record the environment, volume, plans, and cache/warm-up effects.
- Never commit secrets, `.env`, raw datasets, model weights, venvs or caches. Only `.env.example` uses placeholder values.
- Don't hide failed tests; record them and list them as limitations.

## Conventions
- Python 3.10+ with type hints. Configuration comes from `configs/*.yaml` plus environment variables, never hard-coded hosts or paths.
- Each job is idempotent and re-runnable, writes to a versioned path, and logs input and output row counts.
- HDFS paths: Bronze raw files `/datalake/bronze/<source>/raw/ingest_date=YYYY-MM-DD/`, Bronze Parquet `/datalake/bronze/<source>/records/ingest_date=YYYY-MM-DD/` (separate trees so partition discovery works; see `docs/storage_layout.md`). Silver/Gold follow `/datalake/{silver,gold}/<dataset>/...`.
- Bronze records carry provenance: `_source`, `_ingested_at`, `_source_version`, `_sha256`.
- Quality outcomes are split into `accepted` / `quarantine` / `rejected`, and every rule has a test.
- **Every job is tested before it counts as done.** That covers each Spark, Kafka/Flink, quality and product job, and each fetch or check script. Three levels:
  1. **Unit tests** (`tests/unit/`) for the job's pure logic: parsing, matching, sampling, rule functions. Use small fake inputs, and include one failure case. These must run on the Mac and in the Codespace without Docker.
  2. **A run on the stack** in the Codespace. The job's check must verify the *result* (row counts reconciled, checksums, expected schema), not just exit code 0. Integration, data-quality and streaming tests go in `tests/integration/`, `tests/data_quality/` and `tests/streaming/`.
  3. **Recorded evidence:** that run's logs and summary are saved in `evidence/ACxx/<run>/` and cited by run ID.
  Add or update the tests in the same change as the job, and run `pytest` before saying a job works. If a test fails, report it with its output; don't skip or delete it.
- After each milestone, update `README.md` run steps and the matching `evidence/ACxx/` folder.

## Acceptance checklist (minimum requirement per task; evidence goes in `evidence/ACxx/`)
- **T01/AC01 Ingestion:** source inventory (origin, licence, URL/API, fields, time/space range, update behaviour) in `docs/`. At least 2 ingestion patterns: file batch + API. Raw data kept with timestamp, origin, version and checksum. Re-ingestion works; report row/file counts, schemas, and failed/anomalous records.
- **T02/AC02 Storage:** Bronze/Silver/Gold in HDFS; CSV/JSON → Parquet; date/region partitions; a storage catalogue and access rules. Show the cluster config, reads/writes, file stats (`hdfs dfsadmin -report`, `fsck`), an explanation of replication, and Spark reading from HDFS.
- **T03/AC03 Spark:** schema standardisation, timestamps and units, duplicates and outliers, valid joins, time/region aggregation, written to Parquet. Repeatable jobs, input/output reconciliation, before/after samples, and the Spark UI or event log. Explain one Map–Shuffle–Reduce step using `explain()` or a stage record.
- **T04/AC04 Streaming:** a Kafka producer replays events with their original event time. Flink event-time windows with watermarks. Tests for **late, duplicate, malformed and restart/recovery**. Explain the watermark and result-update semantics, and state the verified delivery guarantee.
- **T05/AC05 Quality:** ≥6 executable rules (type, timestamp, business key, missing values, numeric range, uniqueness, source completeness). ≥5 test categories (valid, missing, duplicate, invalid numeric, abnormal timestamp). Output is accepted/quarantine/rejected, plus a quality report and raw→published lineage.
- **T06/AC06 Analytics/performance:** ≥3 SQL analyses (e.g. hourly pattern, weekday vs holiday, weather impact, NYC zones). ≥2 strategy pairs (CSV vs Parquet, partitioned vs not, pushdown/pruning), each run ≥3 times. Record environment, volume, elapsed time, plans and conclusions.
- **T07/AC07 Products:** the 4 contracts, read-back tests and version notes.
- **T08/AC08 Deployment:** run from a fresh Codespace using only the README. Provide a run order from acquisition to publication, job logs, failure handling, and one demonstrated failure + restart recovery.
- **T09/AC09 Visualisation:** show source coverage, processing volume, quality indicators, batch results, stream window metrics and ≥1 performance comparison, all from real outputs with traceable generation. Show one full pipeline run and one abnormal case.

## Out of scope (don't build)
Standalone React/Vue frontend, login/registration, authorisation system, complex CRUD, traffic-prediction or detector model training, production-grade high availability. Optional extras (X01 NoSQL/graph, X02 vector discovery, X03 REST/web UI) only after all mandatory tasks are done; they add no marks beyond 100.

## Deliverables and demo
D01 README · D02 architecture/design (`docs/`) · D03 source code · D04 environment (compose, versions, `.env.example`) · D05 data catalogue · D06 products + contracts + quality summaries · D07 evidence for AC01–AC09 · D08 performance report · D09 project report (design, risks, limitations, references) · D10 demo + short presentation.
Demo order: ingestion → storage → Spark → Kafka/Flink → quality report → SQL/performance → product read-back → visualisation → one failure/recovery scenario.
Rubric: ingestion 8 · storage 12 · Spark + performance 18 · streaming 15 · quality 12 · products 10 · deployment 10 · reproducibility 5 · visualisation 5 · docs 5.

## Workflow (Claude Code in the Codespace, or on the Mac)
**Find out where you are first.** If the working directory is `/workspaces/BigData_Management_Smart_City_Traffic-Detection` and `docker version` works, you are in the **Codespace**. If the path contains `Desktop/BD/course-bigdata-analytics/04-projects/241091038`, you are on the **Mac**.

**In the Codespace (preferred from M1 on):**
- The repo root is the project root. Run the real things yourself: `docker compose ps/up -d`, `pytest tests/unit`, `bash scripts/smoke_m0.sh`, `bash scripts/ingest_m1.sh …`, then read the logs in `evidence/ACxx/<run>/` and fix failures directly.
- Only numbers from runs you actually executed go into docs and reports, and you cite the run ID (e.g. `m1_20261121T…Z`). A failed run stays in `evidence/` and is listed as a limitation; never delete or hide it.
- Check that the stack is up before running jobs. After the Codespace restarts, `docker compose up -d` is enough. `--build` is only needed when `docker/*/Dockerfile` or `docker/spark/requirements.txt` changes.
- Long jobs (full ingestion, benchmarks) run in the background with their output going to `evidence/`. Don't start a second copy while one is running.
- Never print, log or commit `.env`. It holds the user's personal KITTI download links. Check `git status` before every commit: `.env`, `data/local/` and images must not be listed.
- Git: **commit only when the user asks. Don't push and don't open PRs unless the user asks in that conversation** (the user's rule from 2026-11-21). The branch is `codeSpaceInitial`.

**On the Mac:**
- 4 GB RAM and almost no free disk (about 150 MB on 2026-11-21). Edit code and docs only, check with `python -m py_compile` and `.venv/bin/python -m pytest -q tests/unit`. No Docker, no downloads.
- Nothing counts as "working" until it has run in the Codespace.

**Syncing:** the GitHub repo (`67abraham/BigData_Management_Smart_City_Traffic-Detection`, project folder at the root) is the working copy. The graded submission is the Gitee fork of the course repo at `04-projects/241091038/`, which the user syncs from GitHub before submitting. This `CLAUDE.md` and the spec copy in `docs/spec/` are tracked in git (since 2026-11-21), so they reach the Codespace with `git pull`. When you change `CLAUDE.md`, tell the user so they commit it.

## Lessons from earlier sessions (don't repeat these)
- **Explain plainly.** The user asked "what are you actually trying to do". Before a multi-step change, say in 1–2 plain sentences what you're doing and why. Keep answers short.
- **Devcontainer:** `python` image + `docker-in-docker` feature put the Codespace into recovery mode ("docker: command not found"). Fixed by using `mcr.microsoft.com/devcontainers/universal:2`, which already has Docker. Don't switch back.
- **Kafka KRaft `CLUSTER_ID`** must be a 22-character base64 UUID (current one: `5L6g3nShT-eMCtK--X86sw`). A readable string makes the broker exit.
- **Flink memory:** `jobmanager.memory.process.size` / `taskmanager.memory.process.size` must fit under the containers' `mem_limit`, or the containers get OOM-killed.
- **Spark `input_file_name()`** returns `''` on cached or RDD-built DataFrames. Attach provenance from the manifest instead (see `bronze_load.py`).
- **smoke_m0.sh:** PASS must mean the *result* is right (3 live DataNodes, repl=2, read-back count), not just exit code 0. Apply the same rule to every new check script.
- **MnDOT GIS** (`webgis.dot.state.mn.us`) timed out from outside the US **and from the Codespace** (run `m1_20261121T060408Z`). `locate_atr301.py` falls back to the ArcGIS Hub CSV export of the same official item. Match on `DOT_ID` only: `OBJECTID` 301 is a different site. Never hard-code ATR 301 coordinates; they come from `data/manifests/atr301_location.json`.
- **`cmd | head` under `set -o pipefail`** can fail the step with SIGPIPE even when the work succeeded (run `m1_20261121T071308Z`). Add `|| true` to informational listings and make the step's PASS depend on a real check (here, the HDFS file count equal to the local count).
- **Codespace stops** interrupt long jobs (run `m1_20261121T061620Z`). After `docker compose up -d`, re-run the step. Idempotent jobs make that safe.
- **KITTI image zip** (12.6 GB) supports HTTP range requests. `range_zip.py` reads only the 500 sampled images. Tested against the real archive listing: training/image_2 has 7,481 PNGs and training/label_2 has 7,481 TXT files.
- **When a bug class is fixed in one job, fix it in every job at once** (grep for the pattern). The stdout-report fix of M2 was not carried back to `bronze_load.py`, and the fresh-environment run failed on it (`full_20261121T111135Z`).
- **Spark jobs must write reports to files, not stdout** (run `m2_20261121T080458Z`): Spark INFO log lines interleave with long stdout lines and corrupt the JSON. Jobs take `--report-out` / `--explain-out` paths under `/workspace/evidence/…`.
- **`first()` on string columns turns HashAggregate into SortAggregate** (run `m2_20261121T083320Z`). Group by attributes that depend on the key instead.
- **Spark tests** run in the spark-master container (`bash scripts/test_spark.sh`), which has pytest and Spark's bundled `pyspark.zip`; the Codespace host has no pyspark. Keep rule logic in pure modules (`src/quality/*_rules.py`) so `pytest tests/unit` runs without Spark.
- **Check sources before writing a fact.** The Open-Meteo and UCI pages and the TLC dictionary were read in M2; two earlier claims were wrong (UCI weather "OpenWeatherMap-derived"; WMO list missing 97). Quote the page, or say "not stated".
- **PyFlink watermarks:** generate them with parallelism 1 per ordered source partition. PyFlink's internal Extract-Timestamp operator ignores `set_parallelism` on `assign_timestamps_and_watermarks`, so set the job default parallelism to 1 and raise it on the keyed stages. Python bundles must be size 1 or watermarks lag by a whole burst (runs `m3_20261121T090639Z`, `…090823Z`, `…091035Z`). Cost: ~57–59 events/s.
- **PyFlink `on_timer` must be a generator** (`yield from ()`), even if it emits nothing.
- **Kafka topics for replayed data need `retention.ms=-1` and `LogAppendTime`:** Flink stamps sink records with their event time (2012–2018) and the default 7-day retention deletes them at once (run `m3_20261121T091418Z`).
- **A restart check must prove a restore** (`checkpoints.counts.restored >= 1`), not just state RUNNING: right after a TaskManager kill the JobManager still reports RUNNING until its heartbeat times out (~25 s).
- **Write CSV with the `csv` module**, never by joining strings: values like KITTI's `origin` contain commas (run `m4_20261121T102633Z`).
- **Verify every number in a doc against the run's files before writing it**, including "small" ones (check counts, how many runs were slowest). Three draft statements in M4 were wrong until checked.
- **Predicate pushdown needs clustered data:** on randomly ordered Parquet it skipped nothing (run `m4_20261121T103358Z`); on data sorted by the filter column it read 10.9x fewer bytes.
- **NYC TLC monthly files do differ in schema** (confirmed in M1). 2023-01 has `airport_fee` and `bigint` IDs. 2023-02 and 2023-03 have `Airport_fee` and `int` IDs, and the type of `passenger_count`/`RatecodeID` differs between the months. Bronze keeps one `source_month=` sub-partition per file; Silver must unify the names and types (details in `docs/source_inventory.md`).

## Milestones
M0 environment (AC02) → M1 ingestion (AC01) → M2 Spark and quality (AC03, AC05) → M3 Kafka/Flink and recovery (AC04, AC08) → M4 products and benchmarks (AC06, AC07) → M5 dashboard and docs (AC09, D01–D10).

**Progress:** M0 done 2026-11-21 (smoke_m0.sh 8/8 PASS, run m0_20261121T014208Z). M1 done 2026-11-21: all 4 sources in Bronze, and the second full run `m1_20261121T072608Z` has 0 failures, every file `unchanged` and the same record counts. Results and anomalies are in `docs/source_inventory.md`, evidence in `evidence/AC01/`. M2 done 2026-11-21: Silver `uci_traffic_hourly`, `open_meteo_hourly`, `nyc_yellow_trips` and Gold `uci_traffic_weather_hourly`, `nyc_zone_hourly_demand`, all reconciled to Bronze (runs `m2_20261121T081847Z`, `m2_20261121T083320Z`, `m2_20261121T084148Z`); 52 unit + 73 Spark tests; docs `docs/quality.md`, `docs/spark_processing.md`, `docs/storage_layout.md`. M3 done 2026-11-21: replay of 40,558 UCI hours → 7,334 windows = oracle (`m3_20261121T092806Z`), edge tests (`m3_20261121T091237Z`), TaskManager kill + restore from checkpoint (`m3_20261121T094727Z`); docs `docs/streaming.md`. M4 done 2026-11-21: products released (`m4_20261121T102858Z`, read-back all passed), SQL analyses (`m4_20261121T103306Z`), benchmark (`m4_20261121T104058Z`); docs `docs/data_products.md`, `docs/analytics.md`, `docs/performance.md`. M5 done 2026-11-21: dashboard (`src/visualisation/app.py`), `scripts/run_all.sh`, docs D02/D09/D10. AC08 fresh environment done 2026-11-21: `full_20261121T112047Z` all 7 stages passed in 47 min after `docker compose down -v` (first attempt `full_20261121T111135Z` failed at the M1 report, fixed). The pre-run local cache backup was deleted afterwards (user's request). **Still open:** instructor question on single-host HDFS; revert personal notes in `../README.md` (course repo).

## Open decisions / TODO
- [x] UCI station coordinates: DONE 2026-11-21. `DOT_ID` 301 was found in MnDOT's official layer (via the ArcGIS Hub export): 44.9516° N, 93.1369° W, site status Retired. The ERA5 cell used is 45.00° N, 93.25° W. Documented, with its limitations, in `docs/source_inventory.md`.
- [ ] Confirm on a map that ATR 301 lies on I-94 westbound (the layer has no route field). The link and steps are in `docs/source_inventory.md`; record the result and date there.
- [x] NYC TLC: DECIDED 2026-11-21 — Yellow taxi, 3 consecutive months (e.g. 2023-01..03); monthly partitions feed the T06 partitioned-vs-unpartitioned benchmark. Row counts come from the actual ingest, not estimates.
- [x] KITTI: DECIDED 2026-11-21 — user's KITTI account is approved (2026-11-21). Download in the Codespace only: full 2D labels (small) + a subset of training images; the manifest lists every image ID with a file-availability flag; commit only the manifest. Never put KITTI download links or credentials in git.
- [ ] Ask the instructor whether single-host multi-container HDFS is acceptable as "distributed"
- [x] GitHub repo for Codespaces (project folder only, at repo root)
- [ ] Revert the user's personal notes in `../README.md` before the first commit

## Disclosure
The spec requires declaring generative-AI use. Keep `docs/ai-usage.md` up to date, and verify generated code by running it.
