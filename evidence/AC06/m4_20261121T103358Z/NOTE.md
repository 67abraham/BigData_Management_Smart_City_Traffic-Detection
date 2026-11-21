# Note on this run

Valid run (all layouts 9,300,058 rows, all results identical). Pair C (filter pushdown off vs on on
parquet_flat) showed no gain: both read 73.6 MB / 9,300,058 records, medians 0.77 s vs 1.01 s, within
run-to-run noise. Reason: parquet_flat was written with a round-robin repartition(16), so every row group
spans all dates and its min/max statistics can never exclude it. The next run adds a layout sorted by
pickup time (narrow row-group statistics) to show when pushdown helps.
