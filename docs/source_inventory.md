# Source inventory (spec T01, D05)

Two kinds of facts appear here, and each one says which it is:

- **Source facts** are quoted from the official source page (linked) or were observed directly on
  2026-11-21 (HTTP headers, archive listings, API responses).
- **Recorded results** come from an M1 ingestion run and cite its run ID. The full logs, manifests and
  profiles are in `evidence/AC01/<run>/`. Unless stated otherwise, figures are from the second full
  run **`m1_20261121T072608Z`** (all 4 sources, 0 failures), which reproduced the first runs exactly
  (see [Re-ingestion](#re-ingestion-check)).

Ingestion patterns used (spec T01 asks for at least 2): **file batch** (UCI, NYC TLC, KITTI labels),
**REST API** (Open-Meteo) and **partial file batch by HTTP range reads** (KITTI image subset).

## Summary table

| | UCI Metro Interstate Traffic Volume | Open-Meteo Historical Weather | NYC TLC Yellow Taxi trips | KITTI 2D Object Detection |
|---|---|---|---|---|
| Role | hourly road volume; Kafka replay source | weather for the same place and period as UCI | ≥1M-record Spark batch and benchmarks | road-scene images for `road_image_manifest_v1` |
| Origin | UCI ML Repository, dataset 492 | Open-Meteo archive API (ERA5 reanalysis) | NYC Taxi & Limousine Commission | KIT & Toyota Technological Institute (KITTI Vision Benchmark) |
| Page | <https://archive.ics.uci.edu/dataset/492/metro+interstate+traffic+volume> | <https://open-meteo.com/en/docs/historical-weather-api> | <https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page> | <https://www.cvlibs.net/datasets/kitti/eval_object.php> |
| Access | file: `https://archive.ics.uci.edu/static/public/492/metro+interstate+traffic+volume.zip` | API: `https://archive-api.open-meteo.com/v1/archive` | files: `https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_YYYY-MM.parquet` + `misc/taxi_zone_lookup.csv` | registered account; personal links in `.env` only (logged as `kitti://<file>`) |
| Ingestion pattern | file batch | API (one call per calendar year, 7 calls) | file batch (one file per month) | file batch, partial (labels in full, 500-image subset by HTTP range reads) |
| Licence | CC BY 4.0 (DOI 10.24432/C5X60B) | CC BY 4.0; ERA5 contains modified Copernicus Climate Change Service information | NYC Open Data Terms of Use | CC BY-NC-SA 3.0 (non-commercial); images not redistributed in this repository |
| Format delivered | zip → one `Metro_Interstate_Traffic_Volume.csv.gz` | JSON (`hourly` column arrays) | Parquet (monthly) + CSV zone lookup | zip of PNG images; zip of label TXT files |
| Fields | `holiday, temp, rain_1h, snow_1h, clouds_all, weather_main, weather_description, date_time, traffic_volume` (file header; matched expected header in the run) | `temperature_2m` °C, `precipitation` mm, `rain` mm, `snowfall` cm, `cloud_cover` %, `weather_code` WMO code, `wind_speed_10m` km/h (units as returned by the API) | TLC yellow schema, 19 columns; **types and one column name differ between months** (see below) | label line: type, truncated, occluded, alpha, 2D bbox (4), 3D dimensions (3), location (3), rotation_y |
| Time coverage | 2012-10-02 09:00 .. 2018-09-30 23:00 (recorded min/max); hourly **with gaps** | 2012-10-01 00:00 .. 2018-10-01 23:00 UTC, hourly, complete (recorded) | 2023-01 .. 2023-03 (chosen); pickups outside those months exist (see below) | not time-indexed |
| Time semantics | local time of Minneapolis–St Paul as given by UCI; no offset in the data; DST handling checked in M2 | `timezone=GMT` requested, so raw times are UTC | pickup/drop-off as local NYC wall-clock time (`timestamp_ntz`, no zone) | — |
| Spatial coverage | one counter: I-94 **westbound**, MnDOT ATR station 301, "roughly midway between Minneapolis and St Paul" (UCI) | one ERA5 grid cell: 45.00° N, 93.25° W, elevation 267 m (returned by the API) | New York City, 265 TLC taxi zones | Karlsruhe, Germany (unrelated to the other sources) |
| Size observed | zip 405,547 B | 7 JSON files, 2,563,969 B in HDFS | 2023-01: 47,673,370 B · 2023-02: 47,748,012 B · 2023-03: 56,127,762 B (HTTP Content-Length) | labels zip 5,601,213 B; images zip 12,569,945,557 B (only ~526 MB read) |
| Update behaviour | static (donated dataset) | reanalysis; past values can be reprocessed upstream. A request already made is **not re-sent** unless `--force` (see limitations) | files published monthly and republished upstream at times (Last-Modified differs per month); ETag + checksum detect changes | static benchmark release |
| Joins | UCI ↔ Open-Meteo on hour only (same place and period) | — | **never joined** to UCI (different city) | **never joined** to the other sources |

## Recorded ingestion results (M1)

| Source | Files landed | Records in Bronze | Checks recorded in the run profile |
|---|---|---|---|
| UCI Metro | 2 (zip + extracted CSV.gz), 810,920 B | **48,204** rows | header as expected; 48,204 data lines = 48,204 parsed rows; 0 corrupt records; 0 unparseable `date_time`; 0 non-integer `traffic_volume`; 0 empty values in any column |
| Open-Meteo | 7 JSON (one per year) | **52,608** hourly rows | 52,608 expected hours, 0 missing, 0 duplicate hours, 0 nulls in all 7 variables, 1 grid point |
| NYC TLC | 4 (3 monthly Parquet + zone lookup), 151,561,475 B | **9,384,487** trips (2023-01: 3,066,766 · 2023-02: 2,913,955 · 2023-03: 3,403,766) + 265 zones | all 4 files' SHA-256 match the copies in HDFS |
| KITTI | 501 in the manifest (labels zip + 500 images); 503 files in HDFS including `_manifest.json` and `subset_ids.txt` | **7,481** label files, **51,865** label lines; **500** images | 0 malformed label lines; 0 label files without lines; 0 bad PNGs; every sampled image has a label; images 1224–1242 × 370–376 px |

In every run, Bronze Parquet is written to `/datalake/bronze/<source>/records/ingest_date=2026-11-21/`
next to the raw files in `/datalake/bronze/<source>/raw/ingest_date=2026-11-21/` (layout:
`docs/storage_layout.md`).

### Anomalies found during ingestion
These are recorded as found. Bronze does not fix or drop anything. The quality rules in M2 (T05)
decide whether each record is accepted, quarantined or rejected.

- **UCI duplicate hours:** 5,445 hours appear more than once, which gives 7,629 extra rows (40,575
  distinct hours in 48,204 rows). `traffic_volume` is identical within every one of the 5,445 groups.
  The rows differ in `weather_description` (5,386 groups) and `weather_main` (5,349), and a few also
  differ in `temp` (78), `clouds_all` (32) or `rain_1h` (9). So the file repeats an hour once per
  reported weather condition. (Checked on the landed CSV, ingest_date 2026-11-21.)
- **UCI gaps:** the span from first to last row covers 52,551 hours, but only 40,575 distinct hours are
  present, so **11,976 hours are absent**. (Derived from the recorded min, max and distinct count.)
- **UCI types:** Bronze keeps every column as `string`. Typing happens in Silver.
- **NYC TLC schema drift between months:** January has `airport_fee`, `bigint` `VendorID` /
  `PULocationID` / `DOLocationID`, and `double` `passenger_count` / `RatecodeID`. February and March have
  `Airport_fee` (capital A), `int` IDs, and `bigint` `passenger_count` / `RatecodeID`. Bronze keeps one
  `source_month=` partition per file with its own schema. Silver must unify them.
- **NYC TLC pickups outside the file's month:** 48 (Jan), 55 (Feb) and 189 (Mar) trips. The earliest
  pickup is 2001-01-01 (March file) and the latest is 2023-04-05.
- **NYC TLC missing `passenger_count`:** 71,743 (Jan), 76,817 (Feb) and 87,619 (Mar) rows.
- **KITTI `DontCare`:** 11,295 of the 51,865 label lines are `DontCare` regions, which are not objects.
  The object counts are Car 28,742 · Pedestrian 4,487 · Van 2,914 · Cyclist 1,627 · Truck 1,094 · Misc
  973 · Tram 511 · Person_sitting 222.

### Re-ingestion check
The second full run `m1_20261121T072608Z` downloaded nothing new and gave identical results:

| Source | Download status | Records, first run → second run |
|---|---|---|
| UCI Metro | zip `unchanged`; CSV extracted again, SHA-256 identical | 48,204 → 48,204 (first: `m1_20261121T060214Z`) |
| Open-Meteo | 7/7 `unchanged` | 52,608 → 52,608 (first: `m1_20261121T070714Z`) |
| NYC TLC | 4/4 `unchanged` | 9,384,487 → 9,384,487 (first: `m1_20261121T070826Z`) |
| KITTI | 501/501 `unchanged` | 51,865 lines / 500 images → same (first: `m1_20261121T071555Z`) |

### Failed and interrupted runs (kept in `evidence/AC01/`)
| Run | What happened |
|---|---|
| `m1_20261121T060408Z` | ATR 301 lookup failed (exit 1): MnDOT's FeatureServer timed out (`ConnectTimeout`). Fixed by adding the ArcGIS Hub CSV export of the same layer as a fallback. |
| `m1_20261121T061317Z` | ATR 301 lookup exit 2: the FeatureServer timed out again, and the Hub fallback found 1 candidate with `DOT_ID = 301`, but the lookup did not choose it (`chosen=False`). In the next run (`m1_20261121T061620Z`) the unique `DOT_ID` match was accepted. |
| `m1_20261121T061604Z` | Open-Meteo fetch failed (exit 1): `atr301_location.json` had no unique ATR 301 match yet, so there were no coordinates to request. Passed in the next run, after the Hub fallback had located the station. |
| `m1_20261121T061620Z` | Open-Meteo Spark Bronze step interrupted because the Codespace stopped. Re-run as `m1_20261121T070714Z` (PASS). |
| `m1_20261121T071308Z` | KITTI land step reported FAIL although all 503 files had reached HDFS. The step's informational `hdfs dfs -ls -R \| head -40` ended with SIGPIPE under `pipefail`. Fixed in `scripts/ingest_m1.sh`, which now passes the step only when the HDFS file count equals the local count. Re-run as `m1_20261121T071555Z` (PASS). |

## Location of ATR station 301
UCI gives no coordinates. `src/ingestion/locate_atr301.py` looks the station up in MnDOT's official
**CONTINUOUS_COLLECTION_SITES** layer (ArcGIS item `4e85cd73d09949f5ac2a9d08b266d74e`) and saves the
full answer to `data/manifests/atr301_location.json`. The Open-Meteo request uses those coordinates.

**Recorded result (retrieved 2026-11-21 06:18 UTC):**
- **Route:** the FeatureServer
  (<https://webgis.dot.state.mn.us/65agsf1/rest/services/sdw_infra/CONTINUOUS_COLLECTION_SITES/FeatureServer>)
  timed out from the Codespace too. The lookup used the ArcGIS Hub CSV export of the same item and
  layer (SHA-256 `17fb3fd1…`, 377 features).
- **Match:** exactly one record has `DOT_ID = 301` (OBJECTID 235, LOCATION_ID 9834, collection type
  Volume/Speed/Class, **site status Retired**).
- **Coordinates:** 44.9516214° N, 93.1368848° W (WGS 84).
- **ERA5 grid cell used:** 45.00° N, 93.25° W, about 10.4 km from the station (great-circle distance
  computed from the two points).

**Not yet confirmed:** the layer's attributes contain no route or direction field
(`mentions_i94: false`), so the station is matched by its ID only. Cross-checking that this point lies
on I-94 westbound is still open, against a map or a MnDOT station page.

**Plausibility check (not a confirmation):** the point lies between the two city centres, east of
downtown Minneapolis (≈93.27° W) and west of downtown St Paul (≈93.09° W), which fits UCI's
"roughly midway between Minneapolis and St Paul". To confirm, open
`https://www.openstreetmap.org/?mlat=44.95162&mlon=-93.13688#map=17/44.95162/-93.13688` and check
that the point is on I-94, then record the result and date here.

**About "Retired":** the status is MnDOT's current record (retrieved 2026-11-21), not the status in
2012–2018, when UCI's data was collected. The layer gives no retirement date. UCI's data ending on
2018-09-30 is *consistent* with a later retirement, but nothing in the source shows when the station
was retired, so this document does not claim one.

## Provenance kept for every landed file
`_manifest.json` next to the raw files (also copied to `evidence/AC01/<run>/manifest_<source>.json`)
records the URL (KITTI: file name only), HTTP ETag and Last-Modified, `source_version`, download time
(UTC), size, SHA-256, and a status: `downloaded`, `unchanged`, `extracted`, `redownloaded_identical`,
`redownloaded_changed` or `failed`. Bronze Parquet rows carry `_source`, `_source_file`, `_ingested_at`,
`_source_version` and `_sha256`. Each Spark Bronze job also checks that every manifest file exists in
HDFS with the same SHA-256 (`integrity` in the run profile).

## Known limitations
- **Weather is reanalysis, not observation.** ERA5 is a ~0.25° (~25 km) grid, and the cell used is
  ~10 km from the counter. UCI's own weather columns (OpenWeatherMap-derived, per UCI) are kept as
  delivered and are a different source from Open-Meteo.
- **ATR 301 is matched by ID only** (see above), and the site is listed as Retired today. The
  coordinates are MnDOT's current record of the site, not necessarily its location in 2012–2018.
- **Open-Meteo changes upstream go unnoticed on a normal re-run.** A request that was already made
  successfully is not sent again unless `FORCE=1`, so reprocessed ERA5 values are only picked up by a
  forced re-ingest. Open-Meteo returns no ETag or Last-Modified header.
- **KITTI is a 500-image sample** (seed 241091038) of the 7,481 training images. Labels are complete.
  The testing split has no labels and is not ingested.
- **NYC TLC covers 3 months of Yellow taxi trips only** (Green, FHV and other months are not ingested).
- **UCI timezone:** the data gives local times with no offset. How DST transitions appear (repeated or
  missing hours) is checked in M2 before the hour-level join with UTC weather.
