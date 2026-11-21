# Source inventory (spec T01, D05)

Facts below are either quoted from the official source page (linked) or were observed directly on
2026-11-21 (HTTP headers, archive listings, the API response). Row counts, schemas and anomalies of
the ingested data are **not** written here by hand: they come from each run's
`evidence/AC01/m1_*/ingestion_report.md`.

| | UCI Metro Interstate Traffic Volume | Open-Meteo Historical Weather | NYC TLC Yellow Taxi trips | KITTI 2D Object Detection |
|---|---|---|---|---|
| Role | hourly road volume; Kafka replay source | weather for the same place and period as UCI | ≥1M-record Spark batch and benchmarks | road-scene images for `road_image_manifest_v1` |
| Origin | UCI ML Repository, dataset 492 | Open-Meteo archive API (ERA5 reanalysis) | NYC Taxi & Limousine Commission | KIT & Toyota Technological Institute (KITTI Vision Benchmark) |
| Page | <https://archive.ics.uci.edu/dataset/492/metro+interstate+traffic+volume> | <https://open-meteo.com/en/docs/historical-weather-api> | <https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page> | <https://www.cvlibs.net/datasets/kitti/eval_object.php> |
| Access | file: `https://archive.ics.uci.edu/static/public/492/metro+interstate+traffic+volume.zip` | API: `https://archive-api.open-meteo.com/v1/archive` | files: `https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_YYYY-MM.parquet` + `misc/taxi_zone_lookup.csv` | registered account; personal links in `.env` only |
| Ingestion pattern | file batch | API (one call per year) | file batch | file batch, partial (labels in full, image subset by HTTP range reads) |
| Licence | CC BY 4.0 (DOI 10.24432/C5X60B) | CC BY 4.0; ERA5 contains modified Copernicus Climate Change Service information | NYC Open Data Terms of Use | CC BY-NC-SA 3.0; images not redistributed in this repository |
| Format delivered | zip → one `Metro_Interstate_Traffic_Volume.csv.gz` | JSON (`hourly` column arrays) | Parquet (monthly) + CSV | zip of PNG images; zip of label TXT files |
| Fields | `holiday, temp, rain_1h, snow_1h, clouds_all, weather_main, weather_description, date_time, traffic_volume` (header of the file) | requested: `temperature_2m` °C, `precipitation` mm, `rain` mm, `snowfall` cm, `cloud_cover` %, `weather_code` WMO code, `wind_speed_10m` km/h (units as returned by the API) | TLC yellow schema (per-month schema recorded in the run profile) | label line: type, truncated, occluded, alpha, 2D bbox (4), 3D dimensions (3), location (3), rotation_y |
| Time coverage | first row 2012-10-02 09:00, last row 2018-09-30 23:00 (observed in the file); hourly with gaps | requested 2012-10-01 .. 2018-10-01, hourly, UTC | 2023-01 .. 2023-03 (chosen) | not time-indexed |
| Time semantics | local time of Minneapolis–St Paul as given by UCI; DST handling to be checked in M2 | `timezone=GMT` requested, so raw times are UTC | pickup/drop-off local NYC times | — |
| Spatial coverage | one counter: I-94 **westbound**, MnDOT ATR station 301, "roughly midway between Minneapolis and St Paul" (UCI) | one ERA5 grid cell (0.25°) nearest to ATR 301; the API snaps coordinates to the grid and returns the grid point used | New York City, TLC taxi zones | Karlsruhe, Germany (unrelated to the other sources) |
| Size observed | zip 405,547 bytes | small JSON per year | 2023-01: 47,673,370 B · 2023-02: 47,748,012 B · 2023-03: 56,127,762 B (HTTP Content-Length) | labels zip 5,601,213 B; images zip 12,569,945,557 B; training/image_2 has 7,481 PNGs, testing/image_2 7,518; training/label_2 7,481 TXT (archive listings) |
| Update behaviour | static (donated dataset) | reanalysis; values for past dates can be reprocessed upstream — checksum per request detects a change on re-ingest | files are published monthly and have been republished upstream (Last-Modified differs per month); ETag + checksum detect changes | static benchmark release |
| Joins | UCI ↔ Open-Meteo on hour only (same place and period) | — | **never joined** to UCI (different city) | **never joined** to the other sources |

## Location of ATR station 301
UCI gives no coordinates. `src/ingestion/locate_atr301.py` looks the station up in MnDOT's official
**CONTINUOUS_COLLECTION_SITES** layer
(<https://webgis.dot.state.mn.us/65agsf1/rest/services/sdw_infra/CONTINUOUS_COLLECTION_SITES/FeatureServer>,
ArcGIS item `4e85cd73d09949f5ac2a9d08b266d74e`) and saves the full answer to
`data/manifests/atr301_location.json`. The Open-Meteo request uses those coordinates.

**Known limitation:** ERA5 is reanalysis on a ~0.25° (~25 km) grid, not a station observation at the
road. The exact point inside the metro area changes the grid cell at most, which is recorded in the
profile (`grid_points`). UCI's own weather columns (OpenWeatherMap-derived, per UCI) are kept as
delivered and are a different source from Open-Meteo.

## Provenance kept for every landed file
`_manifest.json` next to the raw files (also copied to `evidence/AC01/<run>/manifest_<source>.json`):
URL (KITTI: file name only), HTTP ETag and Last-Modified, `source_version`, download time (UTC),
size, SHA-256, and status (`downloaded`, `unchanged`, `redownloaded_identical`, `redownloaded_changed`,
`failed`). Bronze Parquet rows carry `_source`, `_source_file`, `_ingested_at`, `_source_version`, `_sha256`.
