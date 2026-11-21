# Quality report — `nyc_yellow_trips` v1

- Run: `m2_20261121T083320Z` · Spark app `app-20261121083529-0013`
- Input: `hdfs://namenode:8020/datalake/bronze/nyc_tlc/records/ingest_date=2026-11-21` (Bronze ingest_date 2026-11-21)
- Output: `hdfs://namenode:8020/datalake/silver/nyc_yellow_trips/v1`

## Reconciliation

| Bronze rows | Source rows in Silver (all outcomes) | Reconciled |
|---:|---:|---|
| 9,384,487 | 9,384,487 | yes |

## Outcomes

| Outcome | Silver records | Source rows |
|---|---:|---:|
| accepted | 9,300,058 | 9,300,058 |
| quarantine | 83,793 | 83,793 |
| rejected | 636 | 636 |

## Rules

Source rows = Bronze rows that broke the rule before collapsing; records = Silver records carrying it.

| Rule | Category | Severity | Source rows | Records | Meaning |
|---|---|---|---:|---:|---|
| `key_missing` | business_key | reject | 0 | 0 | pickup/drop-off time or zone missing |
| `zone_not_in_lookup` | business_key | quarantine | 0 | 0 | pickup or drop-off LocationID not in the TLC taxi zone lookup |
| `zone_unknown_or_outside` | business_key | flag | 165,102 | 165,102 | zone 264 (Unknown) or 265 (Outside of NYC) |
| `ts_dropoff_before_pickup` | timestamp | reject | 636 | 636 | drop-off earlier than pickup |
| `ts_pickup_outside_file_month` | timestamp | quarantine | 292 | 292 | pickup not in the month of the file it was published in |
| `ts_zero_duration` | timestamp | quarantine | 3,203 | 3,203 | pickup and drop-off at the same second |
| `range_duration_over_max` | range | quarantine | 68 | 68 | trip longer than 1440 minutes |
| `type_non_integer_code` | type | quarantine | 0 | 0 | passenger_count or RatecodeID is not a whole number before the int cast |
| `range_distance_negative` | range | reject | 0 | 0 | negative trip distance |
| `range_distance_over_max` | range | quarantine | 297 | 297 | trip distance above 100.0 miles |
| `flag_distance_zero` | range | flag | 135,488 | 135,488 | trip distance 0 miles |
| `range_amount_negative` | range | quarantine | 79,954 | 79,954 | negative fare or total (refund/dispute) |
| `range_total_over_max` | range | quarantine | 3 | 3 | total amount above 1000.0 USD |
| `flag_passenger_count_zero` | range | flag | 156,806 | 156,806 | driver-reported passenger count is 0 |
| `range_vendor_unknown` | range | quarantine | 0 | 0 | VendorID not in the data dictionary |
| `range_ratecode_unknown` | range | quarantine | 0 | 0 | RatecodeID not in the data dictionary |
| `range_payment_type_unknown` | range | quarantine | 0 | 0 | payment_type not in the data dictionary |
| `range_store_fwd_unknown` | range | quarantine | 0 | 0 | store_and_fwd_flag not Y/N |
| `missing_fields_flex_fare` | missing | flag | 236,179 | 236,179 | Flex Fare trip (payment_type 0) without passenger count / rate code / surcharges |
| `missing_fields_non_flex` | missing | quarantine | 0 | 0 | passenger count / rate code / surcharges missing on a non-Flex-Fare trip |
| `unique_trip_duplicate` | uniqueness | quarantine | 0 | 0 | several rows share the duplicate key (all copies quarantined) |

## Dataset-level checks

- **rows_reconciled_per_month:** PASS — bronze {'2023-01': 3066766, '2023-02': 2913955, '2023-03': 3403766} vs silver {'2023-01': 3066766, '2023-02': 2913955, '2023-03': 3403766}
- **all_expected_months_present:** PASS — expected ['2023-01', '2023-02', '2023-03']
- **zone_lookup_complete:** PASS — 265 zones (expected 265)
- **no_unexpected_columns:** PASS — extra columns per month: {'2023-01': [], '2023-02': [], '2023-03': []}

## Labels

- model `TLC TPEP trip records (provider-submitted)`, data kind `trip records; not road-counter traffic volume (spec §4)`
- valid time per variable: local wall-clock, America/New_York assumed (not stated by TLC)
