# Smart City Traffic Data Platform — Big Data final project

Student ID: **241091038** · Spec: [`docs/spec/final-project-task-specification.md`](docs/spec/final-project-task-specification.md)

A big-data engineering platform: real traffic/weather/trip/image sources → HDFS Bronze/Silver/Gold →
Spark batch → Kafka + Flink event-time processing (historical event replay) → quality and governance →
four versioned data products with contracts → light dashboard.

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
- Next: M5 dashboard (T09) and remaining docs (D01–D10).

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

## Tests
```bash
pip install -r requirements-dev.txt
pytest tests/unit                       # pure Python, no Docker (also on a laptop)
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
