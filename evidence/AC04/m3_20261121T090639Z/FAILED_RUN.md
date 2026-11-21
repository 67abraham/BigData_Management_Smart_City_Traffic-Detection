# Failed run, kept as evidence

Late update, too-late, duplicate and malformed handling were all correct here (see `edge_check.json`),
but window 2016-07-01T18:00Z never fired. Cause: the timestamp/watermark operator ran with
parallelism 2 behind a parallelism-1 parse operator, so events were spread round-robin over two
watermark generators. Flink's watermark is the minimum over its inputs; the final flush marker reached
only one generator, so the combined watermark stayed near 2016-07-01T21:59 and never passed the end of
the last window (also visible as lagging `watermark_at_emit` values).
Fix: `assign_timestamps_and_watermarks(...).set_parallelism(<source partitions>)` in
`src/streaming/flink_window_job.py`. Re-run follows.
