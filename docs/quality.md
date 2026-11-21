# Data quality and lineage (spec T05, AC05)

Every figure here comes from a recorded run; the run ID is given next to it. Full outputs per run:
`evidence/AC05/<run>/` (rule catalogue `rules_<source>.json`, `quality_<source>.json`,
`quality_report_<source>.md`, test logs) and `evidence/AC03/<run>/` (Spark logs, plans, samples).

## Outcome model
Each rule has a **category** (the T05 list: type, timestamp, business key, missing, range, uniqueness,
completeness) and a **severity**. A record's outcome is its worst severity:

| Severity | Outcome | Meaning | Used downstream? |
|---|---|---|---|
| `reject` | `rejected` | structurally unusable: no key, unparseable or impossible value | no |
| `quarantine` | `quarantine` | parseable but suspicious; kept for review | no |
| `flag` | `accepted` | valid but noteworthy; the rule name stays in `_violations` | yes |
| — | `accepted` | no rule broken | yes |

Nothing is dropped silently: rejected and quarantined records are written to their own Silver
partitions with the rules they broke, and every Silver job reconciles its output back to Bronze.
Thresholds are in `configs/quality.yaml`; rule logic in `src/quality/*_rules.py` (pure Python, unit
tested) and evaluated by `src/quality/spark_rules.py`. Completeness (whole files, whole periods) is
checked per dataset in each job's report, not per record.

## Rules per dataset

| Dataset | Rules | Categories covered | Catalogue |
|---|---:|---|---|
| UCI `uci_traffic_hourly` v1 | 19 (17 per source row, 2 per hour) | type, timestamp, business key, missing, range, uniqueness + completeness checks | `evidence/AC05/m2_20261121T081847Z/rules_uci_metro.json` |
| Open-Meteo `open_meteo_hourly` v1 | 15 (13 per row, 2 per key) | same | `…/rules_open_meteo.json` |
| NYC `nyc_yellow_trips` v1 | 21 | same | `evidence/AC05/m2_20261121T083320Z/rules_nyc_tlc.json` |

Where the thresholds and code lists come from:
- **UCI**: units from the UCI page (temp in K, rain/snow in mm, clouds in %); timezone `America/Chicago`
  because the spring-forward 02:00 hour is absent in every year (M1 profile), although UCI's page says
  "local CST time"; sanity bounds chosen above the values seen in M1 (e.g. max volume 7,280).
- **Open-Meteo**: expected units are compared with the `hourly_units` of every raw API response; the
  29 WMO weather codes and the "valid time" of each variable (instant vs preceding-hour sum) are from the
  Open-Meteo historical-weather docs.
- **NYC TLC**: VendorID, RatecodeID, payment_type and store_and_fwd_flag code lists from the TLC
  *Data Dictionary – Yellow Taxi Trip Records* (March 18, 2025); bounds from a profile of the 9.4M real
  rows (e.g. max trip distance 335,004 mi, ~25k negative fares per month).

## Results

### UCI Metro — run `m2_20261121T081847Z` (identical to `m2_20261121T080753Z`)
48,204 Bronze rows → 48,204 source rows in Silver (**reconciled**). 40,558 accepted hours (48,181 source
rows), 17 quarantined hours (23 source rows), 0 rejected.

| Rule (broken at least once) | Severity | Source rows | Silver records |
|---|---|---:|---:|
| `ts_dst_ambiguous` (fall-back 01:00 hour) | quarantine | 10 | 4 |
| `range_temp_k` (0 K) | quarantine | 10 | 10 |
| `range_volume_zero` | quarantine | 2 | 2 |
| `range_rain_1h` (9,831.3 mm) | quarantine | 1 | 1 |
| `unique_hour_conflicting_weather` | flag | — | 79 |

Dataset checks: the one expected file present; 5,445 hours had several source rows (7,629 extra rows
collapsed: same `traffic_volume`, one row per reported weather condition); 40,575 distinct hours between
2012-10-02 09:00 and 2018-09-30 23:00 local, **11,976 hours absent**; UTC offsets −5 (27,038 h) and −6
(13,533 h); 53 holiday dates.

### Open-Meteo — run `m2_20261121T081847Z`
52,608 Bronze rows → 52,608 (**reconciled**), all accepted; no rule broken. Checks: units and timezone
match all 7 raw API responses; 52,608 of 52,608 requested hours present; one grid point (45.0 N,
93.25 W); ATR 301 lies **inside** that 0.25° ERA5 cell, 10.4 km from its centre.

### NYC TLC Yellow — run `m2_20261121T083320Z`
9,384,487 Bronze rows → 9,384,487 Silver rows (**reconciled**, also per month: 3,066,766 / 2,913,955 /
3,403,766). 9,300,058 accepted, 83,793 quarantined, 636 rejected.

| Rule (broken at least once) | Severity | Records |
|---|---|---:|
| `missing_fields_flex_fare` (payment_type 0 without passenger count, rate code, surcharges) | flag | 236,179 |
| `zone_unknown_or_outside` (LocationID 264/265) | flag | 165,102 |
| `flag_passenger_count_zero` | flag | 156,806 |
| `flag_distance_zero` | flag | 135,488 |
| `range_amount_negative` | quarantine | 79,954 |
| `ts_zero_duration` | quarantine | 3,203 |
| `ts_dropoff_before_pickup` | reject | 636 |
| `range_distance_over_max` (> 100 mi) | quarantine | 297 |
| `ts_pickup_outside_file_month` | quarantine | 292 |
| `range_duration_over_max` (> 24 h) | quarantine | 68 |
| `range_total_over_max` (> 1,000 USD) | quarantine | 3 |

`missing_fields_non_flex` = **0** and `unique_trip_duplicate` = **0**: the NULL fields occur only on
Flex Fare trips (checked row by row), and no two rows share the 10-column duplicate key. Checks: all
three months present, 265 zones in the lookup, no unexpected columns; the per-month schema changes
(e.g. `Airport_fee` → `airport_fee_usd`, `bigint`/`double` → `int`) are listed in
`quality_nyc_tlc.json` → `schema_unification`.

## Test categories (spec T05: at least five)

Synthetic records are built inside the tests only and never written to the data lake (spec §8).
Recorded result, run `m2_20261121T084148Z`: **52 unit tests passed** (`unit_tests.log`, no Spark) and
**73 Spark data-quality test cases passed** (`data_quality_tests.log`, run in the Spark container by
`scripts/test_spark.sh`).

| Category | UCI (`test_uci_silver.py`) | Open-Meteo (`test_open_meteo_silver.py`) | NYC (`test_nyc_tlc_silver.py`) |
|---|---|---|---|
| valid | `test_valid_summer_row_…`, `test_valid_winter_row_…` | `test_valid_row_accepted_…` | `test_valid_trips_from_both_schemas_…` |
| missing | `test_missing_values` (5 cases) | `test_missing_variable_…`, `test_missing_key_rejected` | `test_flex_fare_nulls_…`, `test_same_nulls_on_card_trip_…`, `test_missing_pickup_time_…` |
| duplicate | 4 tests (same / exact / conflicting volume / conflicting weather) | `test_identical_duplicates_…`, `test_conflicting_duplicates_…` | `test_duplicate_trips_are_all_quarantined` |
| invalid numeric | `test_invalid_numeric` (9 cases) | `test_invalid_numeric_quarantined` (8 cases) | `test_invalid_numeric_and_codes` (13 cases) |
| abnormal timestamp | `test_abnormal_timestamp` (6 cases), `test_ambiguous_hour_has_no_utc_time` | `test_abnormal_timestamp` (4 cases) | `test_abnormal_timestamp` (4 cases) |
| also | malformed line, holiday spreading, reconciliation | reconciliation; Gold join alignment and left-join | Gold aggregation reconciliation |

## Lineage: raw → published

```
upstream URL/API
  └─ landing file + _manifest.json (URL, ETag, Last-Modified, SHA-256, status)      ingest_m1.sh
     └─ Bronze raw/ (byte-identical; SHA-256 recomputed on the HDFS copy)            bronze_load.py
        └─ Bronze records/ (_source, _source_file, _sha256, _source_version, _ingested_at)
           └─ Silver (+ _violations, outcome, _bronze_ingest_date, _silver_run_id, _silver_version;
              │        UCI/Open-Meteo also _raw_rows = the Bronze values of the record)
              └─ Gold (accepted Silver only; + _gold_run_id, _gold_version; UCI-based Gold keeps the
                        UCI provenance columns)
                 └─ data products (M4)
```

To trace a Gold or Silver value back: `_silver_run_id` / `_gold_run_id` → `evidence/AC03|AC05/<run>/`
(inputs, outputs, Spark app id) → `_bronze_ingest_date` + `_sha256` → the Bronze manifest in
`evidence/AC01/<run>/manifest_<source>.json` → the upstream URL. Before/after examples (Bronze rows and
the Silver record they became) are in `evidence/AC03/<run>/samples_<source>.json`.

## Known limitations
- Thresholds are sanity bounds, not domain truth; flagged records (e.g. 135,488 zero-distance NYC
  trips) stay in `accepted` and consumers must decide whether to use them.
- UCI quarantine is per hour: one bad source row (e.g. 0 K) quarantines the whole hour's volume.
- UCI's `date_time` is assumed to be the **start** of the hour (not stated by UCI); see
  `docs/spark_processing.md` for the evidence checked.
- NYC times are kept as local wall clock; TLC does not state a time zone.
