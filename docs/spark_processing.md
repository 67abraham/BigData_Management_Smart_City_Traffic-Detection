# Spark batch processing (spec T03, AC03)

All batch work runs as PySpark jobs submitted to the Spark standalone cluster (`spark-master`, 2 workers,
1 core / 1 GB executor each; `configs/spark/spark-defaults.conf`), reading and writing HDFS. Pandas is not
used for any pipeline step. Every job writes its plan and a JSON report to `evidence/`, and every run is
in the Spark History Server (event log on `hdfs:///spark-events`, UI port 18080).

Run order: `bash scripts/run_m2.sh` (tests → Silver UCI → Silver Open-Meteo → Gold join → Silver NYC →
Gold NYC). Layout of the outputs: `docs/storage_layout.md`. Quality rules: `docs/quality.md`.

## Jobs and recorded runs

| Job | What it does (T03 item) | Input → output (records) | Run · Spark app |
|---|---|---|---|
| `src/batch/silver_uci.py` | schema standardisation (strings → typed), local time → UTC with DST handling, units (K → °C kept side by side), duplicate hours collapsed, outliers quarantined, holiday spread to the whole date | 48,204 Bronze rows → 40,575 Silver records (40,558 accepted, 17 quarantine) | `m2_20261121T081847Z` · `app-20261121082036-0009` |
| `src/batch/silver_open_meteo.py` | typing, UTC hour key, units verified against the API response, reanalysis labels, duplicate keys collapsed | 52,608 → 52,608 | `m2_20261121T081847Z` · `app-20261121082138-0010` |
| `src/batch/gold_traffic_weather.py` | **valid join** UCI ⟕ ERA5 on UTC hour with valid-time alignment | 40,558 UCI hours → 40,558 rows, all matched | `m2_20261121T081847Z` · `app-20261121082230-0011` |
| `src/batch/silver_nyc_tlc.py` | per-month schema unification, zone lookup join, duplicate detection, outliers | 9,384,487 → 9,384,487 (9,300,058 accepted) | `m2_20261121T083320Z` · `app-20261121083529-0013` (278 s) |
| `src/batch/gold_nyc_zone_hourly.py` | **temporal + regional aggregation**: trips per pickup date × hour × zone | 9,300,058 trips → 212,041 rows (sum of trips 9,300,058) | `m2_20261121T084148Z` · `app-20261121084359-0015` (47 s) |

**Repeatable:** every job overwrites only its own versioned output path, so a re-run replaces the
result. Two UCI Silver runs (`m2_20261121T080753Z`, `m2_20261121T081847Z`) produced identical
reconciliation, outcomes, rule counts, duplicate and coverage figures.

**Input–output reconciliation** (in each job's report; a job exits non-zero if it fails):
- Silver: `sum(source_rows)` over all outcomes = Bronze rows (UCI 48,204; Open-Meteo 52,608; NYC
  9,384,487, also per month).
- Gold join: output rows = accepted UCI hours = distinct `hour_start_utc` (40,558) — no fan-out.
- Gold aggregation: `sum(trips)` = accepted Silver trips (9,300,058) and the group key is unique.

**Before/after samples:** `evidence/AC03/<run>/samples_<source>.json` shows Bronze rows next to the
Silver record they became, e.g. UCI 2012-10-14 09:00: three Bronze rows (Drizzle, Haze, Mist; volume
2,685) → one Silver record with `weather_main = [Drizzle, Haze, Mist]`, `source_rows = 3`,
`hour_start_utc = 14:00`, `temp_c = 9.28`.

## Map–Shuffle–Reduce: the NYC zone-hourly aggregation

Query (`gold_nyc_zone_hourly.py`): group 9.3M accepted trips by `(pickup_date, pickup_hour_local,
pu_location_id)` and compute counts, sums and averages. Physical plan
(`evidence/AC03/m2_20261121T084148Z/explain_gold_nyc_zone_hourly.txt`):

```
AdaptiveSparkPlan
+- HashAggregate (4)        <- reduce: count(1), sum(...), avg(...)  (final)
   +- Exchange (3)          <- shuffle: hashpartitioning(pickup_date, pickup_hour_local, pu_location_id, pu_borough, pu_zone, 8)
      +- HashAggregate (2)  <- map: partial_count(1), partial_sum(...), partial_avg(...)
         +- Scan parquet (1)   hdfs://…/silver/nyc_yellow_trips/v1/outcome=accepted
```

Stage record from the History Server REST API
(`evidence/AC03/m2_20261121T084148Z/stages_gold_nyc_zone_hourly.json`):

| Stage | Role | Tasks | Records in | Shuffle write | Shuffle read | Executor time |
|---|---|---:|---:|---:|---:|---:|
| 2 | **map**: scan + partial aggregate per input split | 5 | 9,300,058 (89.5 MB) | 212,041 records (15.8 MB) | — | 19.8 s |
| 4 | **reduce**: fetch partial results by key, final aggregate | 3 | — | (212,041 for the following repartition-by-date write) | 212,041 records (15.8 MB) | 3.5 s |
| 7 | write Parquet partitioned by `pickup_date` | 3 | — | — | 212,041 | 11.4 s |

What happens:
1. **Map.** Each of the 5 map tasks reads its share of the Parquet files and keeps a hash table keyed by
   the group key, holding *partial* aggregates (`partial_count`, `partial_sum`, and sum + count for each
   `avg`). Only the partial rows leave the task: 9,300,058 trips become 212,041 shuffle records, about
   44× fewer.
2. **Shuffle.** The `Exchange` hash-partitions those records on the group key into 8 shuffle partitions
   (`spark.sql.shuffle.partitions = 8`), so all partial rows of one key land in the same reduce task. AQE
   coalesced them to 3 reduce tasks.
3. **Reduce.** Each reduce task merges the partials per key (`count` adds the counts, `avg` divides the
   merged sum by the merged count) and emits one row per key: 212,041 rows.

Here the map side already produced one partial row per final group (212,041 = final rows) because the
input is partitioned by `pickup_date` and the date is part of the key, so no group spans two map tasks.
The partial aggregation did the heavy reduction; the shuffle moved only 15.8 MB.

A first version used `first()` on the zone-name strings, which forced `SortAggregate` (sort before each
aggregate). Grouping by those columns instead (they depend only on `pu_location_id`, so no groups are
added; same 212,041 rows) gives the HashAggregate plan above (run `m2_20261121T083320Z` kept, with a
note).

## Other shuffle and join decisions seen in the plans
- **UCI duplicate collapse** (`explain_silver_uci_metro.txt`): `ObjectHashAggregate` → `Exchange` →
  `ObjectHashAggregate` (object variant because of `collect_set`/`collect_list`), then a `Window` per
  local date for the holiday spread.
- **NYC duplicate detection** (`explain_silver_nyc_tlc.txt`): `Exchange hashpartitioning` on the
  10-column duplicate key → `Sort` → `Window count(*)`. This is the one full shuffle of 9.4M rows.
- **Zone lookup join** (NYC Silver) and **UCI ⟕ ERA5 join** (Gold): `BroadcastHashJoin LeftOuter
  BuildRight`. The right sides are small (265 zones; 52,608 weather hours), so Spark ships them to every
  executor and the large side is not shuffled for the join.

## The UCI ↔ ERA5 join: why it is valid, and what is assumed
- **Same place:** ATR 301 (MnDOT coordinates) lies inside the single ERA5 cell used (checked by the
  Open-Meteo Silver job; 10.4 km from the cell centre).
- **Same time base:** UCI local time converted to UTC (`America/Chicago`); Open-Meteo requested in GMT.
- **Same interval:** a UCI hour [U, U+1h) gets instant ERA5 values at U and preceding-hour sums from
  U+1h (`src/quality/alignment.py`, unit tested).
- **Assumption:** UCI does not say whether `date_time` is the start or the end of the hour; *start* is
  assumed. Correlations of UCI's own weather columns with ERA5 at three offsets
  (`evidence/AC03/m2_20261121T081847Z/gold_traffic_weather.json`):

  | ERA5 time − UCI hour | corr(temp) | corr(rain, precipitation) |
  |---:|---:|---:|
  | −1 h | 0.9819 | 0.1854 |
  | 0 h | **0.9832** | **0.2221** |
  | +1 h | 0.9802 | 0.2065 |

  Temperature fits best at 0 h, which is consistent with *start*. Rain also fits best at 0 h, whereas
  *start* predicts the matching sum at +1 h. The differences are small and compare UCI's weather
  columns (source not named by UCI), not the traffic counts, so this is **inconclusive** and the
  assumption stays documented as a limitation.
- NYC TLC is **never** joined to UCI or ERA5 (different city; spec §4).
