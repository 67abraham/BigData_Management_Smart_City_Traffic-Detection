# Performance report (spec T06/AC06, D08)

All figures come from recorded runs: **`m4_20261121T104058Z`** (main, below) and
`m4_20261121T103358Z` (first run, same pairs without the sorted layout; consistent, see the end).
Raw results: `outputs/benchmarks/<run>/benchmark_results.json` (every run of every configuration) and
`outputs/benchmarks/<run>/plans/` (one physical plan per configuration); copies in `evidence/AC06/<run>/`.
Job: `src/analytics/benchmark.py`, run with `bash scripts/run_m4.sh benchmark`.

## Method
- **Data held constant.** The 9,300,058 accepted NYC Yellow trips of Silver `nyc_yellow_trips` v1
  (2023-01..03), the same 14 columns, written **once** before timing in four layouts on HDFS:

  | Layout | Files | Size on HDFS (logical) |
  |---|---:|---:|
  | `csv_flat` (header, no partitioning) | 17 | 1,076.8 MB |
  | `parquet_flat` (snappy, rows in random order) | 17 | 243.6 MB |
  | `parquet_by_date` (partitioned by `pickup_date`, 90 days) | 91 | 212.2 MB |
  | `parquet_sorted` (rows sorted by pickup time, range-partitioned) | 17 | 188.5 MB |

  Every layout has exactly 9,300,058 rows (checked by the job).
- **Logic held constant.** Three SQL queries, identical text for every layout (only the table name
  changes). Money columns are aggregated as `DECIMAL`, so results are exact; the job **fails** if any
  run's result differs from the first layout's. All runs returned identical results.
  - Q1 `full_agg`: trips and average fare per pickup borough over all rows (2 columns)
  - Q2 `one_week`: trips per local pickup hour, 2023-02-06..12 (737,447 matching trips)
  - Q3 `one_column`: sum of `total_amount_usd`
- **Repeats.** Per configuration 1 warm-up run (reported separately) and 3 timed runs; the table shows
  the median and the three values. Each run also records the bytes and records the scan stages read
  (Spark REST API, stages of the run's job group).
- **Environment.** GitHub Codespace, 4 CPUs, 16 GB; Spark 3.5.3 standalone, 2 executors × 1 core × 1 GB,
  `spark.sql.shuffle.partitions = 8`, AQE on; HDFS 3.3.6, 3 DataNodes on the same host, replication 2.
  No DataFrame caching; the OS page cache is **not** cleared between runs (see warm-up).

## Results (run `m4_20261121T104058Z`)

| Pair | Query | Configuration | Warm-up | Timed runs (s) | **Median** | Bytes read | Rows read |
|---|---|---|---:|---|---:|---:|---:|
| A | Q1 full_agg | csv_flat | 17.46 | 14.66, 14.81, 13.86 | **14.66 s** | 1,076.8 MB | 9,300,058 |
| A | Q1 full_agg | parquet_flat | 4.35 | 3.18, 3.14, 3.43 | **3.18 s** | 15.9 MB | 9,300,058 |
| A | Q3 one_column | csv_flat | 12.60 | 12.49, 12.38, 12.31 | **12.38 s** | 1,076.8 MB | 9,300,058 |
| A | Q3 one_column | parquet_flat | 1.98 | 1.81, 1.98, 1.90 | **1.90 s** | 16.3 MB | 9,300,058 |
| B | Q2 one_week | parquet_flat | 1.71 | 1.16, 1.40, 1.53 | **1.40 s** | 73.6 MB | 9,300,058 |
| B | Q2 one_week | parquet_by_date | 0.57 | 0.39, 0.36, 0.32 | **0.36 s** | 4.3 MB | 737,447 |
| C | Q2 one_week | parquet_flat, pushdown off | 0.84 | 1.10, 1.29, 0.69 | **1.10 s** | 73.6 MB | 9,300,058 |
| C | Q2 one_week | parquet_flat, pushdown on | 0.81 | 0.88, 0.82, 1.18 | **0.88 s** | 73.6 MB | 9,300,058 |
| C | Q2 one_week | parquet_sorted, pushdown off | 1.37 | 0.93, 0.77, 0.67 | **0.77 s** | 39.7 MB | 9,300,058 |
| C | Q2 one_week | parquet_sorted, pushdown on | 0.42 | 0.31, 0.56, 0.42 | **0.42 s** | 3.6 MB | 743,427 |

## Conclusions

**A — CSV vs Parquet (column pruning, binary columnar format).** Parquet was **4.6× faster** for Q1
(14.66 → 3.18 s) and **6.5× faster** for Q3 (12.38 → 1.90 s). The cause is visible in the bytes read:
CSV has to read and parse every byte of every row (1,076.8 MB) even though the queries need one or two
columns; Parquet reads only those column chunks (15.9–16.3 MB, **about 66× less**). The same data is also
4.4× smaller on disk as Parquet (243.6 MB vs 1,076.8 MB). Both still touch all 9,300,058 rows: the gain
is from columns, not from skipping rows.

**B — partitioned vs unpartitioned (partition pruning).** With `pickup_date` partitions, the one-week
query reads only the 7 matching directories: 737,447 rows and 4.3 MB instead of all 9,300,058 rows and
73.6 MB, and runs **3.9× faster** (1.40 → 0.36 s). The plan shows the filter as `PartitionFilters` on
`pickup_date`, so the skipped directories are never listed or opened. The cost: 91 files instead of 17
(small files for low-volume days); for 90 days that is acceptable, for years of data the partition grain
should be coarser.

**C — predicate pushdown off vs on.** Pushdown only helps when the Parquet min/max statistics can rule
row groups out:
- The physical plan lists the date filter as `PushedFilters` in **both** settings;
  `spark.sql.parquet.filterPushdown` decides whether the Parquet reader actually uses it to skip row
  groups by their min/max statistics. The rows and bytes read show whether it did.
- On `parquet_flat`, rows are in random order (round-robin `repartition(16)`), so every row group spans
  the whole quarter and nothing can be skipped: both settings read the same 73.6 MB and all 9,300,058
  rows. The median
  difference (1.10 vs 0.88 s) is within the run-to-run spread (0.69–1.29 s) and had the opposite sign in
  the first run (0.77 vs 1.01 s): **no measurable effect**.
- On `parquet_sorted`, each row group covers a narrow time range. With pushdown on, Spark skips the row
  groups outside the week: 743,427 rows and 3.6 MB read instead of 9,300,058 rows and 39.7 MB, **1.8×
  faster** (0.77 → 0.42 s). (743,427 > 737,447 because the first and last row groups straddle the week.)
- So pushdown is a property of **data layout plus filter**, not a switch that helps by itself. Sorting
  also made the file 23% smaller (188.5 vs 243.6 MB) through better compression.

## Warm-up and cache effects
- The warm-up (first) run of a configuration was the slowest in 6 of 10 configurations, e.g. CSV Q1
  17.46 s vs 14.66 s median (+19%), Parquet Q1 4.35 vs 3.18 s (+37%), sorted pushdown-off 1.37 vs 0.77 s.
  The first run pays for query planning, Parquet footer/metadata reads, JVM JIT compilation and cold OS
  page cache; later runs reuse them. That is why the warm-up is reported separately and the median of
  the 3 timed runs is compared.
- The OS page cache was not cleared, so timed runs read partly from memory. This favours all layouts
  equally per run, but it means absolute times are lower than on a cold cluster. Bytes/rows read are not
  affected by caching, which is why they are reported next to the times.
- Sub-second timings vary by up to ±0.3 s between runs (e.g. 0.69–1.29 s); only differences that are
  larger than that spread **and** come with a change in bytes/rows read are treated as real.

## Consistency with the first run (`m4_20261121T103358Z`)
Same environment and data; pairs A, B and C (flat only). Medians: CSV/Parquet Q1 15.38 / 3.06 s (5.0×),
Q3 12.92 / 1.78 s (7.3×); flat/partitioned Q2 0.97 / 0.49 s (2.0×); pushdown off/on (flat) 0.77 / 1.01 s.
Bytes and rows read were identical to the main run. Speed-up factors differ between the runs (e.g.
partition pruning 2.0× vs 3.9×) because sub-second runs are noisy; the direction and the bytes-read
reductions are the same.

## Limitations
- One host: three DataNodes and two executors share 4 CPUs and one disk, so I/O parallelism is lower than
  on a real cluster, and absolute times do not transfer.
- 3 timed runs per configuration (the spec minimum); medians of sub-second queries are noisy.
- Only one data set (NYC, 9.3M rows) and three queries.
