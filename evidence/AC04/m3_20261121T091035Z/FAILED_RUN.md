# Failed run, kept as evidence

With Python bundle size 1, everything except the last window was correct: W0 on time (5 events,
sum 510), W1 on time (5, 541) then one late_update (6, 651), W2 (6, 687), too_late 1, duplicate 2 and
all five malformed reasons. Window 2016-07-01T18:00Z never fired. `edge_flink_job.txt` shows why:
`Extract-Timestamp -> Timestamps/Watermarks par=2`. PyFlink's internal timestamp-extraction operator
ignored the set_parallelism(1) on assign_timestamps_and_watermarks, so two watermark generators were
fed round-robin and the flush marker reached only one. Fix: job default parallelism 1
(configs/streaming.yaml `parallelism.default`), keyed stages explicitly at 2. Re-run follows.
