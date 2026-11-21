# HDFS storage layout and catalogue (spec T02, D05)

All data lives in HDFS under `/datalake` (NameNode `hdfs://namenode:8020`, 3 DataNodes, replication 2;
see `configs/hadoop/hdfs-site.xml`). Three layers, as required by spec T02:

| Layer | Meaning | Format | Who writes it |
|---|---|---|---|
| **Bronze** (raw) | landed files byte-identical to upstream + one Parquet row per source record with provenance; nothing cleaned | as delivered + Parquet | `scripts/ingest_m1.sh`, `src/ingestion/bronze_load.py` |
| **Silver** (validated) | typed, standardised, quality-checked; every record has an `outcome` (accepted / quarantine / rejected) and the rules it broke | Parquet | `src/batch/silver_*.py` |
| **Gold** (curated) | joins and aggregations built **only from accepted Silver records** | Parquet | `src/batch/gold_*.py` |

Sizes below are `hdfs dfs -count -h` from run `m2_20261121T084148Z`
(`evidence/AC03/m2_20261121T084148Z/hdfs_silver_gold_layout.txt`) and the M1 runs (`evidence/AC01/`).
They are the logical size; with replication 2 the raw disk use is twice that.

## Bronze

| Path | Content | Partitions |
|---|---|---|
| `/datalake/bronze/<source>/raw/ingest_date=YYYY-MM-DD/` | landed files + `_manifest.json` (URL, ETag, version, SHA-256, status) | `ingest_date` |
| `/datalake/bronze/<source>/records/ingest_date=YYYY-MM-DD/` | one row per source record, values as delivered, + `_source, _source_file, _ingested_at, _source_version, _sha256` | `ingest_date` |
| `/datalake/bronze/nyc_tlc/records/ingest_date=…/source_month=YYYY-MM/` | NYC trips, one sub-partition per monthly file (the month schemas differ) | `ingest_date`, `source_month` |
| `/datalake/bronze/nyc_tlc/zone_lookup/ingest_date=…/` | TLC taxi zone lookup (265 zones) | `ingest_date` |
| `/datalake/bronze/kitti/image_inventory/ingest_date=…/` | every KITTI training image ID: label path, availability, size, PNG dimensions, SHA-256 | `ingest_date` |

`<source>` ∈ `uci_metro`, `open_meteo`, `nyc_tlc`, `kitti`. Raw and records are separate trees so that
Spark partition discovery on `records/` sees only `ingest_date=` partitions. Re-running on the same UTC
day replaces that day's partitions (idempotent); a run on a new day adds a partition, so earlier
deliveries stay available for lineage. Silver jobs read the **latest** `ingest_date` and record which one
(`_bronze_ingest_date`).

## Silver (M2)

| Dataset | Path | Partitions | Grain / key | Size | Records (run) |
|---|---|---|---|---|---|
| `uci_traffic_hourly` v1 | `/datalake/silver/uci_traffic_hourly/v1/` | `outcome`, `year` (local) | one record per (station, local hour); rejected rows one per source row | accepted 1.8 M, quarantine 72.6 K | 40,558 / 17 / 0 (`m2_20261121T081847Z`) |
| `open_meteo_hourly` v1 | `/datalake/silver/open_meteo_hourly/v1/` | `outcome`, `year` (UTC) | one record per (grid point, UTC hour) | accepted 1.4 M | 52,608 / 0 / 0 (`m2_20261121T081847Z`) |
| `nyc_yellow_trips` v1 | `/datalake/silver/nyc_yellow_trips/v1/` | `outcome`, `pickup_date` (local) | one record per source trip row (no natural trip id; duplicates detected on a 10-column key) | accepted 243.8 M (90 files), quarantine 4.6 M, rejected 850.2 K | 9,300,058 / 83,793 / 636 (`m2_20261121T083320Z`) |

Records = accepted / quarantine / rejected. Every Silver record carries `_violations`
(array of `{rule, category, severity}`), `outcome`, the Bronze provenance columns, `_bronze_ingest_date`,
`_silver_run_id` and `_silver_version`. UCI and Open-Meteo Silver also keep `_raw_rows`, the Bronze
values each record was built from. Rules and results: `docs/quality.md`.

**Partitioning choices.** NYC trips are partitioned by `pickup_date` (90 accepted partitions, one file
each) so date filters prune whole directories; this is the layout the T06 partitioned-vs-unpartitioned
benchmark uses. UCI/Open-Meteo are small (≈2 MB), so a yearly partition is enough; finer partitions
would only create small files. Each write repartitions by its partition columns first, so every
partition gets one file.

## Gold (M2)

| Dataset | Path | Partitions | Grain / key | Built from | Size | Rows (run) |
|---|---|---|---|---|---|---|
| `uci_traffic_weather_hourly` v1 | `/datalake/gold/uci_traffic_weather_hourly/v1/` | `year` | one row per accepted UCI hour (`hour_start_utc` unique) | Silver UCI accepted ⟕ Silver Open-Meteo accepted on UTC hour | 1.8 M | 40,558 (`m2_20261121T081847Z`) |
| `nyc_zone_hourly_demand` v1 | `/datalake/gold/nyc_zone_hourly_demand/v1/` | `pickup_date` | one row per (pickup date, local hour, pickup LocationID) | Silver NYC accepted | 7.5 M | 212,041; sum of `trips` = 9,300,058 (`m2_20261121T084148Z`) |

There is deliberately **no** Gold dataset combining NYC with UCI: different city, no defensible key
(spec §4). The four T07 data products (M4) will be published from Gold.

## Versioning
- The **schema version** is part of the path (`…/v1/`). A change that breaks readers (renamed/removed
  column, changed meaning or unit) goes to `v2/`; `v1/` stays until consumers have moved.
- A re-run of the same version **overwrites** that version (jobs are idempotent); the run that produced
  the current data is recorded in every record (`_silver_run_id` / `_gold_run_id`) and in
  `evidence/AC03|AC05/<run>/`.
- Bronze keeps every delivery by `ingest_date`; Silver/Gold record which Bronze delivery they used.

## Access rules
The cluster uses Hadoop **simple authentication**; all jobs run as `root` (`HADOOP_USER_NAME=root` in
`.env.example`) and the layer directories are `root:supergroup 755`. There is no Kerberos and no
per-user authorisation (out of scope, spec §3.2). The rules below are therefore **conventions the jobs
follow**, not permissions HDFS enforces:

| Layer / data | Written by | Read by |
|---|---|---|
| Bronze `raw/`, `records/` | ingestion only (`ingest_m1.sh`, `bronze_load.py`) | Silver jobs; audits |
| Silver `outcome=accepted` | Silver jobs only | Gold jobs, analytics |
| Silver `outcome=quarantine` / `outcome=rejected` | Silver jobs only | quality review only; **never** used for Gold or products |
| Gold | Gold jobs only | analytics, data products, dashboard |
| KITTI images | ingestion only | stay in HDFS / `data/local/`; never committed or redistributed (licence CC BY-NC-SA 3.0) |

Known limitation: with one superuser, nothing technically stops a job from writing to the wrong layer;
code review and the per-job output paths are the control.
