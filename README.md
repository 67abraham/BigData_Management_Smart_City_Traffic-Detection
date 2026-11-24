# Smart City Traffic Data Platform — Big Data final project

Student ID: **241091038** · Spec: [`docs/spec/final-project-task-specification.md`](docs/spec/final-project-task-specification.md)

A big-data engineering platform: real traffic/weather/trip/image sources → HDFS Bronze/Silver/Gold →
Spark batch → Kafka + Flink event-time processing (historical event replay) → quality and governance →
four versioned data products with contracts → light dashboard.

## Quick start

`scripts/run_all.sh` is the starter for the whole project: it brings up the Docker stack
(`docker compose up -d --build`, then waits until HDFS and Kafka are healthy), runs every milestone in
order, from data acquisition to the published products, and finally starts the dashboard and prints
its link.

```bash
# in a GitHub Codespace (4 cores / 16 GB) opened on this repository
cp .env.example .env                    # then add your two KITTI download links (KITTI account e-mail)
bash scripts/run_all.sh                 # about 50 min (measured 49m56s, run full_20261124T052158Z) + image build on a new Codespace
                                        # ends with the dashboard running on port 8501 and its link
```

> **Note for the instructor — the full run takes about 50–55 minutes.** From starting the Docker stack to
> the dashboard being up, the last fresh run took **49m56s** (`full_20261124T052158Z`, after
> `docker compose down -v` and with an empty `data/local/`; Docker images already built). On a brand-new
> machine the Docker image build comes on top of that. The longest stage is M3 Kafka/Flink (about 20 min).
> The terminal is not blank while you wait: each stage prints a header (`=== [5/8] 04_stream_m3: ...`) with
> its duration in the last full run, then a `RUN` line when a step starts, a `PASS`/`FAIL` line when it ends,
> and a `... still working on: <step> | stage 7m16s | total 25m59s` line every 30 s during long steps.
> To watch a run started in the background: `tail -f <file the output was redirected to>`.

| Stage (run `full_20261124T052158Z`) | Duration |
|---|---|
| 00 stack: start Docker, wait for HDFS/Kafka healthy | 0m26s |
| 01 M0 smoke test | 1m30s |
| 02 M1 ingestion (downloads all 4 sources) | 6m20s |
| 03 M2 Spark Silver/Gold + quality | 10m27s |
| 04 M3 Kafka/Flink replay, edge cases, failure/recovery | 20m07s |
| 05 M4 products, SQL analyses, benchmarks | 9m51s |
| 06 M5 dashboard data export | 1m10s |
| 07 dashboard start | 0m05s |
| **Total** | **49m56s** |

`run_all.sh` stages: `00_stack` (Docker) → `01_smoke_m0` (HDFS/Spark/Kafka/Flink checks) → `02_ingest_m1`
(download + Bronze) → `03_spark_m2` (Silver/Gold + quality) → `04_stream_m3` (Kafka replay → Flink windows,
edge cases, failure/recovery) → `05_products_m4` (4 data products + read-back, SQL analyses, benchmark) →
`06_export_m5` (dashboard data) → `07_dashboard` (starts Streamlit; last, because it shows the outputs of
all earlier stages). Each stage's exit code and duration go to
`evidence/AC08/full_<run>/stages.tsv`; it stops at the first failing stage and names its log. Every stage is
idempotent, so after fixing a problem just run `bash scripts/run_all.sh` again.

After a Codespace restart, `docker compose up -d` brings the stack back with its data and
`bash scripts/start_dashboard.sh` the dashboard. To redo one part, run its milestone script (sections
below), e.g. `bash scripts/run_m3.sh edge`.

**Status** (all from recorded runs in a GitHub Codespace):
- M0 environment: `scripts/smoke_m0.sh` 8/8 PASS, run `m0_20261121T014208Z` (`evidence/AC02/`).
- M1 ingestion: all 4 sources in Bronze; re-run `m1_20261121T072608Z` 0 failures, every file
  `unchanged`, same record counts (`evidence/AC01/`, `docs/source_inventory.md`).
- M2 Spark + quality: Silver UCI / Open-Meteo / NYC TLC and Gold UCI×ERA5 join and NYC zone-hourly
  aggregation, all reconciled (runs `m2_20261121T081847Z`, `m2_20261121T083320Z`,
  `m2_20261121T084148Z`; `evidence/AC03/`, `evidence/AC05/`, `docs/quality.md`, `docs/spark_processing.md`).
- M3 Kafka/Flink: historical event replay of 40,558 UCI hours → 7,334 six-hour windows equal to an
  independent oracle (`m3_20261121T092806Z`); late / duplicate / malformed / out-of-order tests
  (`m3_20261121T091237Z`); TaskManager kill → restore from checkpoint → results still equal the oracle
  (`m3_20261121T094727Z`). `evidence/AC04/`, `evidence/AC08/`, `docs/streaming.md`.
- M4 products + analytics: 4 data products with contracts, all read-back tests passed
  (`m4_20261121T102858Z`, `docs/data_products.md`, `contracts/`); 5 SQL analyses (`m4_20261121T103306Z`,
  `docs/analytics.md`); benchmark of 3 strategy pairs × 3 timed runs (`m4_20261121T104058Z`,
  `docs/performance.md`). `evidence/AC06/`, `evidence/AC07/`.
- M5 dashboard: Streamlit app reading only recorded outputs, every panel names its source
  (`m5_20261121T105711Z`, `evidence/AC09/`); architecture, project report and demo script in `docs/`.
- AC08 fresh environment: after `docker compose down -v` and with the local cache moved away,
  `scripts/run_all.sh` ran every stage with 0 failures in 47 min (`full_20261121T112047Z`:
  stack 3 s, M0 62 s, M1 308 s, M2 624 s, M3 1,212 s, M4 602 s, M5 52 s) and reproduced every record
  count, reconciliation, oracle match and read-back result. A first attempt (`full_20261121T111135Z`)
  stopped at the M1 report step (bug fixed, kept as evidence). Docker images were cached; a brand-new
  Codespace also builds them first. Repeated from scratch on 2026-11-24 with all 8 stages (including the
  dashboard): `full_20261124T052158Z`, 0 failures in 49m56s, replay windows again equal to the oracle
  (7,334/7,334) and all product read-backs passed.

## Stack (D04)

| Component | Version | Containers | UI / port |
|---|---|---|---|
| HDFS | 3.3.6 (`apache/hadoop`) | namenode, datanode1–3, replication = 2 | NameNode `:9870` |
| Spark (standalone) | 3.5.3, Java 17, Python 3.10 | spark-master, spark-worker-1/2, spark-history | master `:8080`, workers `:8081`/`:8083`, running job `:4040`, history `:18080` |
| Kafka (KRaft) | 3.7.1 (`apache/kafka`) | kafka | `kafka:9092` in-network, `localhost:29092` from host |
| Flink + PyFlink | 1.18.1, Java 11, Kafka connector 3.1.0-1.18 | flink-jobmanager, flink-taskmanager | `:8082` |

Container memory limits are caps, not reservations; they sum to about 13 GB.

## Hardware requirements
A **4-core / 16 GB / 32 GB disk** GitHub Codespace (the minimum declared in `.devcontainer/devcontainer.json`),
or any Linux host with Docker Engine + Compose v2 and the same resources. The first build downloads several
GB of images (exact size to be recorded in `evidence/AC02/`).

> **Note for the next user:** this project was developed and run in a **GitHub Codespace**
> (4 cores / 16 GB RAM / 32 GB disk), because the author's own PC did not have enough resources
> (RAM and disk) for the Docker stack. All recorded runs and timings in this README and in `evidence/`
> come from that Codespace. Any Linux host with the resources above and Docker works the same way.

## Codespace setup
`.devcontainer/devcontainer.json` uses GitHub's default Codespaces image (`devcontainers/universal:2`),
which already includes Docker, Python and Java, and installs `requirements-dev.txt`. Check Docker before anything else:

```bash
docker version && docker compose version
```

If you see `docker: command not found`, the Codespace did not build from the devcontainer (it is in
recovery mode or predates it): run **Codespaces: View Creation Log** to find the error, then
**Codespaces: Full Rebuild Container**.

## Run (M0)

Run from the project root: `04-projects/241091038/` in the course fork, or the repository root of the
GitHub mirror used for Codespaces (which contains only this folder).

```bash
cp .env.example .env            # optional; defaults are built into compose.yaml
docker compose up -d --build    # first build takes several minutes (PyFlink image)
docker compose ps               # namenode and kafka should become "healthy"; spark-history
                                #   restarts until the smoke test creates hdfs:///spark-events
bash scripts/smoke_m0.sh        # writes evidence/AC02/m0_<timestamp>/
```

`smoke_m0.sh` checks: versions and host resources, HDFS layer dirs, `dfsadmin -report`, an HDFS write/read,
a Spark job on the cluster writing and reading Parquet on HDFS (synthetic `spark.range` rows — an
infrastructure check, not project data), `fsck` block locations, a Kafka produce/consume round trip,
and the Flink cluster with PyFlink. It exits non-zero if any step fails.

Stop: `docker compose down` (keeps volumes) · full reset: `docker compose down -v`.

## Run (M1 — ingestion, spec T01/AC01)

Needs the M0 stack running. KITTI needs your personal download links in `.env`
(`KITTI_LABEL_URL`, `KITTI_IMAGE_URL`; see `.env.example`). The links are never committed.

```bash
pip install -r requirements-dev.txt          # fetchers run on the Codespace host
bash scripts/ingest_m1.sh                    # all four sources -> evidence/AC01/m1_<timestamp>/
bash scripts/ingest_m1.sh                    # re-run: unchanged files are skipped ("unchanged")
FORCE=1 bash scripts/ingest_m1.sh uci_metro  # forced re-download: expect "redownloaded_identical"
```

Per source, the script: fetches into `data/local/landing/` (with SHA-256, ETag and version in
`_manifest.json`); copies the files byte-identical into HDFS `bronze/<source>/raw/`; runs a Spark job
that recomputes every checksum on the HDFS copy, profiles the data (row counts, schemas, malformed,
duplicate and out-of-range records) and writes Bronze Parquet with provenance columns. The run ends
with a generated `ingestion_report.md`. Open-Meteo first looks up MnDOT ATR station 301 in MnDOT's
GIS layer (`data/manifests/atr301_location.json`). Sources and layout: `docs/source_inventory.md`,
`docs/storage_layout.md`.

## Run (M2 — Spark Silver/Gold and quality, spec T03/AC03, T05/AC05)

Needs the stack running and M1 Bronze in HDFS.

```bash
bash scripts/run_m2.sh                                   # everything below, in order
bash scripts/run_m2.sh uci_metro open_meteo traffic_weather   # UCI/weather part only
bash scripts/run_m2.sh nyc_tlc nyc_zone_hourly                # NYC part only
```

Steps: unit tests → Spark data-quality tests → per dataset the rule catalogue, the Silver Spark job
and a Markdown quality report → Gold jobs → HDFS listing. A step only PASSes if its result checks
pass (Silver: Bronze rows = Silver source rows and all dataset checks; Gold: no fan-out / trip sums
equal accepted rows). Evidence: `evidence/AC03/m2_<timestamp>/` (Spark logs, `explain_*.txt` plans,
before/after samples, reconciliation, HDFS layout) and `evidence/AC05/m2_<timestamp>/` (test logs,
rules, `quality_report_*.md`). The NYC Silver job takes about 5 minutes in a 4-core Codespace
(278 s recorded in `m2_20261121T083320Z`). Outputs and access rules: `docs/storage_layout.md`.

## Run (M3 — Kafka/Flink historical event replay, spec T04/AC04, T08/AC08)

Needs the stack running and M2 Silver UCI in HDFS. These are **replayed historical events**
(2012–2018), never live data.

```bash
bash scripts/run_m3.sh              # replay + edge + recovery (~25 min)
bash scripts/run_m3.sh replay       # 40,558 events -> bounded Flink job -> results checked against an oracle (~12 min)
bash scripts/run_m3.sh edge         # late, duplicate, malformed, out-of-order events on separate test topics (~2 min)
bash scripts/run_m3.sh recovery     # kill the Flink TaskManager mid-replay, restart, check results (~6 min)
```

Each part deletes and re-creates its Kafka topics first. Flink UI: port 8082. A step only PASSes if
its result is right (window results equal the oracle; the edge-case expectations hold; after the
kill the job restored from a checkpoint). Evidence in `evidence/AC04/m3_<timestamp>/` and
`evidence/AC08/m3_<timestamp>/`; large JSONL files stay in `data/local/m3/`. Design, watermark and
update semantics, and the verified delivery guarantee: `docs/streaming.md`.

## Run (M4 — data products, analytics, benchmarks; spec T06/AC06, T07/AC07)

Needs M1–M3 done (Gold in HDFS and a verified M3 replay run for the window metrics).

```bash
bash scripts/run_m4.sh products     # build 4 products -> read-back test -> contract release blocks (~3 min)
bash scripts/run_m4.sh analytics    # 5 Spark SQL analyses -> outputs/reports/analytics/<run>/ (~1 min)
bash scripts/run_m4.sh benchmark    # CSV vs Parquet, partitioned vs flat, pushdown off/on (~8 min)
```

Products are written to `/datalake/gold/products/<name>/v1/`; each contract in `contracts/` ends with a
generated `release` block (run, row count, size, quality summary, read-back result). The benchmark
writes its four data layouts once to `/datalake/bench/nyc_trips/` (about 1.7 GB, untimed), then runs
1 warm-up + 3 timed runs per configuration and fails if any query result differs between layouts.

## Run (M5 — dashboard, spec T09/AC09)

```bash
bash scripts/run_m5.sh                          # export small tables from the products + dashboard tests
bash scripts/start_dashboard.sh                 # installs requirements-dashboard.txt if needed, starts port 8501, prints the link
```

Tabs: Pipeline (processing volume, the runs shown), Sources, Quality, Batch results, Streaming,
Performance, Abnormal cases (late/duplicate/malformed test, TaskManager failure and recovery, every
failed run kept as evidence). The runs shown are pinned in `configs/dashboard.yaml`.

## Full pipeline in one go (spec T08)

`bash scripts/run_all.sh` — see **Quick start** at the top. The milestone scripts above are what it runs,
in that order.

## Deliverables (spec §9)

| | Where |
|---|---|
| D01 instructions | this README |
| D02 architecture and design | `docs/architecture.md` |
| D03 source code | `src/` (ingestion, batch, quality, streaming, products, analytics, visualisation), `scripts/` |
| D04 environment | `compose.yaml`, `.devcontainer/`, `docker/`, `.env.example`, `requirements-*.txt` |
| D05 data catalogue | `docs/source_inventory.md`, `docs/storage_layout.md`, `docs/data_products.md` |
| D06 data products | `contracts/` (4 contracts + `CHANGELOG.md`), `data/manifests/road_image_manifest_v1.csv` |
| D07 test evidence | `evidence/AC01..AC09/`, `tests/`; screenshots of the HDFS, Spark History, Flink and dashboard UIs in `evidence/AC02,AC03,AC04,AC08,AC09/screenshots_20261124T065644Z/` (each with an `index.md`; made by `scripts/take_screenshots.py`, optional, needs Playwright) |
| D08 performance report | `docs/performance.md` (+ `outputs/benchmarks/`) |
| D09 project report | `docs/project_report.md` |
| D10 demonstration | `docs/demo.md`, the dashboard |

## Tests
```bash
pip install -r requirements-dev.txt
pytest tests/unit                       # pure Python, no Docker (also on a laptop)
pytest tests/integration                # dashboard renders (needs requirements-dashboard.txt)
bash scripts/test_spark.sh              # tests/data_quality with a local SparkSession in the spark-master container
```

## Layout
`configs/` cluster and platform config · `docker/` image builds · `src/` jobs · `tests/` · `scripts/` run
scripts · `contracts/` data-product contracts · `docs/` design and catalogue · `evidence/AC01..AC09/` run
evidence · `data/local/` ignored local cache.

## Known limitations
- All containers run on one host. HDFS has three real DataNode processes with separate volumes and
  replication = 2, but they share one machine's disk and failure domain.
- Kafka is a single broker (replication factor 1). That is enough for event replay and the recovery tests,
  but it is not fault-tolerant.

## AI usage
See [`docs/ai-usage.md`](docs/ai-usage.md).
