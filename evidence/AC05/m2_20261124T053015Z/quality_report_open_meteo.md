# Quality report — `open_meteo_hourly` v1

- Run: `m2_20261124T053015Z` · Spark app `app-20261124053332-0006`
- Input: `hdfs://namenode:8020/datalake/bronze/open_meteo/records/ingest_date=2026-11-24` (Bronze ingest_date 2026-11-24)
- Output: `hdfs://namenode:8020/datalake/silver/open_meteo_hourly/v1`

## Reconciliation

| Bronze rows | Source rows in Silver (all outcomes) | Reconciled |
|---:|---:|---|
| 52,608 | 52,608 | yes |

## Outcomes

| Outcome | Silver records | Source rows |
|---|---:|---:|
| accepted | 52,608 | 52,608 |
| quarantine | 0 | 0 |
| rejected | 0 | 0 |

## Rules

Source rows = Bronze rows that broke the rule before collapsing; records = Silver records carrying it.

| Rule | Category | Severity | Source rows | Records | Meaning |
|---|---|---|---:|---:|---|
| `key_missing` | business_key | reject | 0 | 0 | business key (grid point, hour) incomplete |
| `ts_unparseable` | timestamp | reject | 0 | 0 | time does not match yyyy-MM-dd'T'HH:mm on the hour |
| `ts_outside_request` | timestamp | quarantine | 0 | 0 | hour outside the requested period 2012-10-01 00:00:00 .. 2018-10-01 23:00:00 UTC |
| `missing_variable` | missing | quarantine | 0 | 0 | one of the requested hourly variables is NULL |
| `type_non_finite` | type | quarantine | 0 | 0 | a variable is NaN |
| `type_integer_codes` | type | quarantine | 0 | 0 | weather_code or cloud_cover is not an integer |
| `range_temperature_2m` | range | quarantine | 0 | 0 | temperature_2m outside -60.0 .. 60.0 °C |
| `range_precipitation` | range | quarantine | 0 | 0 | precipitation or rain outside 0 .. 305.0 mm |
| `range_snowfall` | range | quarantine | 0 | 0 | snowfall outside 0 .. 100.0 cm |
| `range_cloud_cover` | range | quarantine | 0 | 0 | cloud_cover outside 0 .. 100 % |
| `range_wind_speed_10m` | range | quarantine | 0 | 0 | wind_speed_10m outside 0 .. 250.0 km/h |
| `range_weather_code_unknown` | range | quarantine | 0 | 0 | weather_code is not one of the documented WMO codes |
| `range_rain_above_precipitation` | range | quarantine | 0 | 0 | rain exceeds total precipitation (rain is part of precipitation) |
| `unique_key_conflicting` | uniqueness | quarantine | — | 0 | several rows for one (grid point, hour) disagree; values set to NULL |
| `unique_key_duplicate` | uniqueness | flag | — | 0 | identical rows for one (grid point, hour); collapsed to one |

## Dataset-level checks

- **Duplicates:** 0 keys had several source rows; 0 extra rows collapsed
- **Hourly coverage** (accepted + quarantine): 52,608 distinct hours from 2012-10-01 00:00:00 to 2018-10-01 23:00:00 (UTC); 52,608 hours in that span, so **0 hours absent**
- **units_timezone_match_api_response:** PASS — 7 raw files checked; not matching: []
- **complete_requested_period:** PASS — 52,608 of 52,608 requested hours present (2012-10-01 00:00:00 .. 2018-10-01 23:00:00 UTC)
- **single_grid_point:** PASS — grid points: [[45.0, -93.25]]
- **station_inside_era5_cell:** PASS — ATR 301 is 10.4 km from grid point [45.0, -93.25]; inside the ±0.125° cell: True

## Labels

- model `era5`, data kind `reanalysis`
- valid time per variable: {'temperature_2m': 'instant', 'cloud_cover': 'instant', 'weather_code': 'instant', 'wind_speed_10m': 'instant', 'precipitation': 'preceding_hour_sum', 'rain': 'preceding_hour_sum', 'snowfall': 'preceding_hour_sum'}
