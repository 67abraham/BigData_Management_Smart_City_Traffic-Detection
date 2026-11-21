# Project report (D09) — Smart City Traffic Data Platform

Student 241091038. Spec: `docs/spec/final-project-task-specification.md`. Every figure below comes from a
recorded run named next to it; evidence is in `evidence/AC01..AC09/`.

## 1. Goal and scope
A data-engineering platform (not a model, not a web app) that ingests four real sources, keeps traceable
raw records, runs Spark batch and Kafka/Flink event-time processing, enforces quality rules and publishes
four versioned data products for ML and CV (spec §1–3). Architecture and design choices: `docs/architecture.md`.

## 2. Implementation summary per task

| Task | What was built | Key recorded result |
|---|---|---|
| T01 Ingestion | file batch (UCI, NYC, KITTI labels), REST API (Open-Meteo), HTTP range reads (500 KITTI images from a 12.6 GB zip); SHA-256, ETag, manifests; `docs/source_inventory.md` | re-ingestion `m1_20261121T072608Z`: every file `unchanged`, record counts identical (48,204 / 52,608 / 9,384,487 / 51,865 label lines) |
| T02 Storage | HDFS Bronze/Silver/Gold/products, Parquet, date/region partitions, catalogue and access rules; `docs/storage_layout.md` | 3 DataNodes, replication 2 (`m0_20261121T014208Z`); all Silver/Gold paths listed in `evidence/AC03/` |
| T03 Spark | Silver for UCI, Open-Meteo, NYC (schema unification over 3 monthly schemas); Gold UCI×ERA5 join and NYC zone-hour aggregation; `docs/spark_processing.md` | Bronze = Silver reconciled for all three (`m2_20261121T081847Z`, `m2_20261121T083320Z`); aggregation 9,300,058 trips → 212,041 rows, plan HashAggregate → Exchange → HashAggregate (`m2_20261121T084148Z`) |
| T04 Streaming | Kafka replay labelled historical; PyFlink watermarks, dedup, 6 h windows with lateness, DLQ; `docs/streaming.md` | 40,558 events → 7,334 windows = oracle (`m3_20261121T092806Z`); late / too-late / duplicate / malformed / out-of-order as designed (`m3_20261121T091237Z`) |
| T05 Quality | 55 rules over 3 datasets (type, timestamp, key, missing, range, uniqueness + completeness checks); accepted / quarantine / rejected; lineage; `docs/quality.md` | NYC 9,300,058 / 83,793 / 636; UCI 40,558 / 17 / 0; 52 unit + 73 Spark quality test cases in M2 |
| T06 Analytics | 5 SQL analyses; 3 strategy pairs × (1 warm-up + 3 runs); `docs/analytics.md`, `docs/performance.md` | CSV→Parquet 4.6–6.5×; partition pruning 3.9×; pushdown 1.8× only on sorted data (`m4_20261121T104058Z`) |
| T07 Products | 4 contracts, build from contract, independent read-back, generated release blocks; `docs/data_products.md` | all read-back checks passed incl. lag-leakage recomputation (`m4_20261121T102858Z`) |
| T08 Deployment | `compose.yaml`, devcontainer, `.env.example`, one script per milestone + `scripts/run_all.sh`; failure/restart | TaskManager kill → restored from checkpoint → results = oracle (`m3_20261121T094727Z`); see §5 for the fresh-environment run |
| T09 Visualisation | Streamlit app from recorded outputs, every panel names its source; `src/visualisation/` | renders all 7 tabs without errors (AppTest, `m5_20261121T105711Z`) |

## 3. What went wrong and how it was found
Failed runs are kept in `evidence/` with their cause (dashboard tab *Abnormal cases*). The important ones:
- **MnDOT GIS timed out** from the Codespace (`m1_20261121T060408Z`): fallback to ArcGIS Hub export of the
  same official layer.
- **A listing with `head` failed a step under `pipefail`** although the data had landed
  (`m1_20261121T071308Z`): the step now checks the HDFS file count.
- **Spark log lines corrupted a stdout report** (`m2_20261121T080458Z`): jobs write reports to files.
- **Flink watermarks lagged** three times (`m3_20261121T090639Z`, `…090823Z`, `…091035Z`): two watermark
  generators fed round-robin, PyFlink bundles, and an internal operator ignoring `set_parallelism`.
- **Kafka deleted results** because Flink stamps them with 2012–2018 event times and the default 7-day
  retention applied (`m3_20261121T091418Z`).
- **A recovery check passed for the wrong reason** (`m3_20261121T092806Z`): it now requires a restored
  checkpoint.
- **Wrong facts in drafts**: "UCI weather is OpenWeatherMap-derived" (UCI names no source), a WMO code list
  missing code 97, a CSV written without quoting, three wrong numbers in M4 drafts — all found by checking
  the official page or the run files before publishing.

## 4. Risks and limitations
- **Single host.** HDFS (3 DataNodes, replication 2), Spark (2 workers) and Kafka run on one 4-core
  Codespace: distributed processes, one failure domain. Instructor confirmation that this counts as
  "distributed" is still open.
- **Kafka**: one broker, one partition per topic, replication 1. **Flink**: PyFlink with bundle size 1 runs
  ≈57–59 events/s; fine for 40,558 events, far below a Java/SQL job.
- **Semantics assumptions**: UCI `date_time` read as the start of the hour (not stated; correlation check
  inconclusive) and as America/Chicago with DST although UCI says "CST"; ATR 301 located by ID in MnDOT's
  current layer (site now Retired); ERA5 is reanalysis on a ~25 km grid.
- **Coverage**: one traffic station; NYC 3 months of Yellow taxis; 500 of 7,481 KITTI images.
- **Governance**: Hadoop simple auth, one superuser — layer access rules are conventions, not enforced.
- **Benchmarks**: 3 timed runs per configuration on a shared host; sub-second medians vary by ±0.3 s.

## 5. Reproducibility (T08/AC08)
`README.md` gives the full run order; `scripts/run_all.sh` runs it end to end and records each stage's
exit code and duration in `evidence/AC08/full_<run>/stages.tsv`. All jobs are idempotent (versioned
output paths overwritten per version, Bronze per `ingest_date`), so re-running a failed stage is the
recovery path; the Codespace stopping mid-job was recovered this way (`m1_20261121T061620Z` →
`m1_20261121T070714Z`). Status of the fresh-environment run from the README: see `README.md` → Status.

## 6. Conclusions
The platform runs every mandatory path on real data with result checks rather than exit codes: every
Silver table reconciles to Bronze, the streaming results equal an independent oracle before and after a
failure, and every data product passes a read-back test against its contract. The storage experiments
give the expected direction with measured causes (bytes and rows read). The main lessons were about
event time (watermarks in PyFlink, Kafka retention on event-time timestamps) and about checking every
claim against its source before writing it down.

## 7. References
- Hogue, J. *Metro Interstate Traffic Volume*. UCI Machine Learning Repository, 2019. DOI 10.24432/C5X60B (CC BY 4.0).
- Zippenfenig, P. *Open-Meteo.com Weather API*. Zenodo, 2023. DOI 10.5281/ZENODO.7970649 (data CC BY 4.0).
- Hersbach, H. et al. *ERA5 hourly data on single levels from 1940 to present*. Copernicus Climate Change
  Service (C3S) Climate Data Store, 2023. DOI 10.24381/cds.adbb2d47.
- NYC Taxi & Limousine Commission. *TLC Trip Record Data* and *Data Dictionary – Yellow Taxi Trip Records*
  (March 18, 2025). https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
- Minnesota DOT. *Continuous Collection Sites* (ArcGIS item 4e85cd73d09949f5ac2a9d08b266d74e).
- Geiger, A., Lenz, P., Urtasun, R. *Are we ready for autonomous driving? The KITTI vision benchmark suite*.
  CVPR 2012 (data CC BY-NC-SA 3.0).
- Apache Hadoop 3.3.6, Apache Spark 3.5.3, Apache Kafka 3.7.1, Apache Flink 1.18.1 (Apache License 2.0);
  Streamlit 1.39 (Apache 2.0).
- Generative-AI use: `docs/ai-usage.md`.
