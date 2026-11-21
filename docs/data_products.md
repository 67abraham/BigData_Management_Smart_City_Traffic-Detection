# Data products (spec T07/AC07, §7, D06)

Four versioned products, Parquet on HDFS under `/datalake/gold/products/<name>/v1/`. The full contract
of each (fields, types, units, key, grain, time semantics, coverage, sources and licences, quality rules,
production method, reading instructions, known limitations) is in `contracts/<name>_v1.yaml`. Its
generated `release` block records the run, row count, size, coverage, quality summary and read-back
result. Versioning and R0/R1: `contracts/CHANGELOG.md`.

Current release: build + read-back run **`m4_20261121T102858Z`** (`evidence/AC07/m4_20261121T102858Z/`).

| Product | Consumers | Grain / key | Rows | Size | Read-back |
|---|---|---|---:|---:|---|
| `traffic_hourly_v1` | analytics, ML | (station_id, hour_start_utc) | 40,558 | 1.14 MB | 10/10 checks passed |
| `traffic_features_v1` | ML | (station_id, hour_start_utc) | 40,558 | 1.37 MB | 11/11, incl. leakage check |
| `road_image_manifest_v1` | CV | image_id | 7,481 | 0.19 MB | 8/8 |
| `traffic_window_metrics_v1` | streaming analytics, dashboard | (station_id, window_start_utc) | 7,334 | 0.27 MB | 9/9 |

(Check counts are from `readback_report.json`; the sizes are HDFS logical sizes.)

## How they are produced
`bash scripts/run_m4.sh products` runs, in order: unit tests → Spark product tests → **build**
(`src/products/build_products.py`: selects and casts exactly the contract's fields) → **read-back**
(`src/products/readback.py`, a separate Spark application) → **release** (`src/products/release.py`
writes each contract's `release` block from the two reports and refuses a product whose read-back
failed). Inputs: Gold `uci_traffic_weather_hourly` v1 (M2), Bronze KITTI (M1), and the collected Flink
window results of replay run `m3_20261121T092806Z` (M3), whose results matched the oracle.

## What the read-back test checks (every product)
1. schema = contract (names and types), 2. row count = build report, 3. primary key unique,
4. non-nullable fields have no NULLs, 5. every contract quality rule holds on every row
(`NOT rule OR rule IS NULL` must match 0 rows). `traffic_features_v1` additionally: `lag_1h` and `lag_24h`
recomputed from `traffic_hourly_v1` for the hour 1 h / 24 h earlier — 0 mismatches in the current release.

## Product notes
- **traffic_hourly_v1** — accepted UCI hours only, UTC key plus local time and the zone used
  (America/Chicago; UCI says "local CST time" but the data has DST); UCI weather (`uci_*`) and ERA5
  reanalysis (`era5_*`) side by side, never mixed. 2012-10-02 09:00 to 2018-09-30 23:00 local,
  1,860 local dates.
- **traffic_features_v1** — target = the hour's volume; features only from information available at
  the hour start (calendar; lags 1/2/24/168 h; mean of the previous 24 h). No weather features (not
  available at prediction time). Split by local date: train 25,313 / validation 8,712 / test 6,533
  rows; 34,679 rows have all lags.
- **road_image_manifest_v1** — all 7,481 KITTI training ids, 500 with `file_available = true` (stored
  in HDFS); label path, counts (40,570 objects + 11,295 DontCare = all 51,865 label lines) and object
  types. No images are redistributed; a CSV copy of the manifest is committed in
  `data/manifests/road_image_manifest_v1.csv`.
- **traffic_window_metrics_v1** — the final row per 6 h event-time window (highest `update_seq`) from
  the historical event replay; 7,334 windows containing all 40,558 replayed hours; 5,760 windows are
  complete (6 of 6 hours).

## Cross-course delivery (spec §7)
These are **R1** releases (this platform's verified output). The course has not supplied an R0 baseline
for these datasets; ML/CV work can start from these files or from the raw public sources without
waiting for this pipeline.
