# Quality report — `uci_traffic_hourly` v1

- Run: `m2_20261121T112700Z` · Spark app `app-20261121112914-0010`
- Input: `hdfs://namenode:8020/datalake/bronze/uci_metro/records/ingest_date=2026-11-21` (Bronze ingest_date 2026-11-21)
- Output: `hdfs://namenode:8020/datalake/silver/uci_traffic_hourly/v1`

## Reconciliation

| Bronze rows | Source rows in Silver (all outcomes) | Reconciled |
|---:|---:|---|
| 48,204 | 48,204 | yes |

## Outcomes

| Outcome | Silver records | Source rows |
|---|---:|---:|
| accepted | 40,558 | 48,181 |
| quarantine | 17 | 23 |
| rejected | 0 | 0 |

## Rules

Source rows = Bronze rows that broke the rule before collapsing; records = Silver records carrying it.

| Rule | Category | Severity | Source rows | Records | Meaning |
|---|---|---|---:|---:|---|
| `type_malformed_line` | type | reject | 0 | 0 | the CSV line could not be parsed into the 9 UCI columns (Bronze _corrupt_record) |
| `key_date_time_missing` | business_key | reject | 0 | 0 | date_time (part of the business key station+hour) is missing |
| `missing_traffic_volume` | missing | reject | 0 | 0 | traffic_volume, the measured value, is missing |
| `missing_weather_field` | missing | quarantine | 0 | 0 | one of the UCI weather fields is missing |
| `type_traffic_volume` | type | reject | 0 | 0 | traffic_volume is not an integer |
| `type_weather_numeric` | type | quarantine | 0 | 0 | temp, rain_1h, snow_1h or clouds_all is not numeric |
| `ts_unparseable` | timestamp | reject | 0 | 0 | date_time does not match yyyy-MM-dd HH:mm:ss on the hour |
| `ts_dst_nonexistent` | timestamp | reject | 0 | 0 | local time does not exist in America/Chicago (spring-forward gap) |
| `ts_dst_ambiguous` | timestamp | quarantine | 10 | 4 | local time occurs twice in America/Chicago (fall-back hour); UTC hour is uncertain |
| `ts_outside_coverage` | timestamp | quarantine | 0 | 0 | timestamp outside the documented source period 2012-01-01 00:00:00 .. 2018-12-31 23:00:00 |
| `range_volume_negative` | range | reject | 0 | 0 | traffic_volume is negative |
| `range_volume_zero` | range | quarantine | 2 | 2 | traffic_volume is 0 vehicles/hour (possible outage or closure) |
| `range_volume_high` | range | quarantine | 0 | 0 | traffic_volume above the sanity bound 10000 |
| `range_temp_k` | range | quarantine | 10 | 10 | temp outside 223.15 .. 333.15 K |
| `range_rain_1h` | range | quarantine | 1 | 1 | rain_1h outside 0 .. 305.0 mm |
| `range_snow_1h` | range | quarantine | 0 | 0 | snow_1h outside 0 .. 100.0 mm |
| `range_clouds_all` | range | quarantine | 0 | 0 | clouds_all outside 0 .. 100 percent |
| `unique_hour_conflicting_volume` | uniqueness | quarantine | — | 0 | several source rows for one hour disagree on traffic_volume |
| `unique_hour_conflicting_weather` | uniqueness | flag | — | 79 | several source rows for one hour disagree on a numeric weather field; that field is set to NULL |

## Dataset-level checks

- **Source completeness:** expected files ['Metro_Interstate_Traffic_Volume.csv.gz']; rows per file in Bronze {'Metro_Interstate_Traffic_Volume.csv.gz': 48204}; all present: **yes**
- **Duplicates:** 5,445 keys had several source rows; 7,629 extra rows collapsed
- **Hourly coverage** (accepted + quarantine): 40,575 distinct hours from 2012-10-02 09:00:00 to 2018-09-30 23:00:00 (local); 52,551 hours in that span, so **11,976 hours absent**
- **DST:** 4 ambiguous fall-back hours in the data; UTC offsets {'-6': 13533, '-5': 27038, 'None': 4}
- **Holidays:** 53 dates, 1,203 hours marked
