# Streaming: Kafka historical event replay → Flink event-time windows (spec T04/AC04, T08/AC08)

**This is historical event replay, not live data.** Every event is a UCI hour from 2012–2018 re-sent to
Kafka in 2026; each message says so (`record_kind: historical_event_replay`) and carries the time it
was replayed (`produced_at`) separately from the time it describes (`event_time`).

Run: `bash scripts/run_m3.sh [replay] [edge] [recovery]`. Config: `configs/streaming.yaml`. Code:
`src/streaming/` (`events.py` format/parse/oracle, `replay_producer.py`, `flink_window_job.py`,
`scenario.py`, `collect.py`, `verify.py`). Evidence: `evidence/AC04/<run>/`, `evidence/AC08/<run>/`.

## Data flow

```
Silver uci_traffic_hourly v1 (accepted, 40,558 hours)
  └─ replay_producer.py (Spark read, ordered by event time) ── JSON ──▶ Kafka uci-traffic-replay-v1 (1 partition)
       └─ Flink job (PyFlink DataStream, flink_window_job.py)
            source → parse ──malformed──────────────────────────────▶ uci-traffic-replay-dlq-v1
                  → event time + watermark (1 generator)
                  → dedup  (keyed by event_id, parallelism 2) ──duplicate──▶ DLQ
                  → 6 h tumbling window (keyed by station, parallelism 2) ──too_late──▶ DLQ
                  → results ─────────────────────────────────────▶ uci-traffic-window-metrics-v1
```

Message fields: `event_id` (`<station>|<event_time>`, deterministic), `record_kind`, `station_id`, `road`,
`direction`, `event_time` (UCI hour start, UTC), `traffic_volume` (vehicles/hour), `source` (Silver dataset,
version and run), `replay_run_id`, `produced_at`. Window result fields: `station_id`, `window_start_utc`,
`window_end_utc`, `window_size_hours`, `event_count`, `expected_events`, `completeness`, `volume_sum`,
`volume_avg`, `volume_min`, `volume_max`, `firing`, `update_seq`, `watermark_at_emit`,
`late_event_handling`, `record_kind`, `source_topic`, `job_run_id`, `emitted_at`.

## Event time and watermarks
- **Event time** = the UCI hour start in UTC (`hour_start_utc` from Silver), not the time Kafka or
  Flink saw the record.
- **Watermark** = largest event time seen − 60 min − 1 ms (Flink `BoundedOutOfOrderness`,
  `max_out_of_orderness_minutes: 60`), emitted every 200 ms of processing time. It means "no event
  older than this is expected any more"; an event up to 60 min older than the newest one is still on
  time.
- **One watermark generator.** The topic has one partition (one station, so Kafka keeps the replay in
  order) and the watermark is generated with parallelism 1. With two generators fed round-robin the
  combined watermark is the minimum of the two, it lagged, and the last window never fired (failed
  runs `m3_20261121T090639Z`, `m3_20261121T091035Z`).
- **PyFlink bundles.** Python operators hand results back to Java per "bundle"; with the default
  bundle a burst of events only came out when the next burst arrived, so watermarks lagged by a whole
  burst (failed run `m3_20261121T090823Z`). Bundle size is set to 1 (`python_runtime` in the config).
  Cost: about **59 events/s** (below).

## Windows and result-update semantics
- **Tumbling event-time windows of 6 h**, aligned to 00/06/12/18 UTC; one result row per
  (station, window). Metrics: `event_count` and `completeness` (= count / 6 hourly records),
  `volume_sum/avg/min/max` in vehicles/hour.
- **On-time result.** A window fires once when the watermark passes its end: `firing = on_time`,
  `update_seq = 0`.
- **Late updates.** Allowed lateness is 6 h. An event for an already-fired window that arrives while
  the watermark is less than 6 h past the window end makes the window fire **again** with the full,
  corrected result: `firing = late_update`, `update_seq = 1, 2, …`. Earlier rows are not retracted.
- **Too late.** An event that arrives after that is not added to any window; it goes to the DLQ with
  `reason = too_late`.
- **How to read the results.** The metrics topic is an **upsert stream**: for each
  `(station_id, window_start_utc)` keep the row with the highest `update_seq` (`events.final_table`).
  This also removes re-emitted rows after a failure (see delivery guarantee).
- **Duplicates.** `event_id` is deterministic, so a re-sent event is recognised. The dedup operator
  keeps every seen id until the watermark passes `event_time + 13 h` (window 6 h + out-of-orderness
  1 h + lateness 6 h): later than that, the event could not change any window anyway.
- **Malformed messages** never reach the windows; the DLQ records the reason (`not_json`,
  `missing_<field>`, `bad_event_time`, `traffic_volume_not_integer`, `traffic_volume_negative`,
  `unknown_record_kind`, `event_id_mismatch`) and the first 500 characters of the payload.

## Delivery guarantee: what was verified
- **Source and state:** Kafka offsets are part of Flink's checkpoints (every 10 s), together with the
  dedup and window state. After a failure the job restarts from the last completed checkpoint and
  re-reads Kafka from the offsets stored in it, so no event is lost and none is counted twice in the
  restored state.
- **Sink: at-least-once.** The Kafka sinks use `DeliveryGuarantee.AT_LEAST_ONCE`: results written
  after the last checkpoint and before the failure are written **again** after the restart.
- **Verified scope (run `m3_20261121T094727Z`):** with the TaskManager killed mid-replay, every one of
  the 10,000 events is in exactly one final window, and the final table equals the oracle; the metrics
  topic contained **66 re-emitted rows**, removed by the upsert rule above. So: **at-least-once
  delivery end to end, effectively-once final results for a consumer that upserts by
  (station, window_start_utc).** End-to-end exactly-once is **not** claimed (it would need the
  transactional Kafka sink and `read_committed` consumers, not set up here).
- The producer uses `acks=all` with retries; a retried send can duplicate an event in Kafka, which the
  dedup operator removes.

## Tests and recorded results

All window results are checked against an **oracle**: the same events grouped by window in plain
Python (`events.oracle`), compared field by field (`events.compare`). Injected test data uses its own
topics (`test-*`), the synthetic station `TEST-301` and `record_kind: injected_test_event`, so it never
mixes with the real replay (spec §8).

| Test | Run | Result |
|---|---|---|
| Full replay (bounded job) | `m3_20261121T092806Z` | 40,558 events (produced in 6.97 s, 5,817/s) → **7,334 windows = oracle** (0 missing, 0 extra, 0 wrong); all 40,558 events in a window; DLQ empty; job 711.6 s |
| Late event within lateness | `m3_20261121T091237Z` | window 2016-07-01 06:00Z: `on_time` 5 events / 541, then `late_update` seq 1 with 6 / 651 |
| Too-late event | same | event 05:00 for window 00:00Z after the watermark passed 11:59:59 → DLQ `too_late`; window stays 5 / 510 |
| Out-of-order within 60 min | same | 08:00 after 09:00 counted on time in window 06:00Z |
| Duplicate messages | same | 2 re-sent events → DLQ `duplicate`, counted once |
| Malformed messages | same | 5 messages → DLQ `not_json`, `missing_traffic_volume`, `bad_event_time`, `traffic_volume_negative`, `unknown_record_kind` |
| Restart / recovery (AC08) | `m3_20261121T094727Z` | see below |
| Unit tests (no Kafka/Flink) | every run | parse reasons, window alignment, metrics, oracle, upsert, scenario checker (`tests/unit/test_streaming_events.py`) |

### Recovery scenario (AC08), run `m3_20261121T094727Z`
Unbounded job; producer replays the first 10,000 events at 50 events/s; the TaskManager container is
killed, restarted 20 s later, and the job must restore from a checkpoint and catch up.

| Time (UTC) | Event (Flink REST records in `evidence/AC08/m3_20261121T094727Z/`) |
|---|---|
| 09:47:48 | job created |
| 09:48:55.305 | checkpoint chk-7 completed |
| 09:49:03 | `docker compose kill flink-taskmanager` |
| ~09:49:23 | `docker compose start flink-taskmanager` |
| 09:49:28.497 | JobManager: "TaskManager … is no longer reachable" → RESTARTING |
| 09:49:38.505 | RUNNING again (fixed-delay restart, 10 s), **restored from chk-7** |
| after catch-up | consumer lag 0; results: 1,854 windows, 10,000 events = oracle; 66 re-emitted rows |

An earlier recovery run (`m3_20261121T092806Z`) also recovered (restored chk-7, results = oracle,
22 re-emitted rows), but its "running again" check passed before the JobManager had noticed the
failure; the check now requires a restored checkpoint (see that run's `NOTE.md`).

### Failed runs (kept in `evidence/AC04/`)
| Run | Problem | Fix |
|---|---|---|
| `m3_20261121T090639Z` | last window never fired: 2 watermark generators, minimum lagged | watermark operator at parallelism 1 |
| `m3_20261121T090823Z` | watermarks lagged a whole burst: Python bundles | bundle size 1 |
| `m3_20261121T091035Z` | last window still missing: PyFlink's internal Extract-Timestamp operator ignored `set_parallelism` | job default parallelism 1, keyed stages 2 |
| `m3_20261121T091418Z` | 6,053 of 7,334 results missing: Kafka deleted them (event-time record timestamps + 7-day retention) | topics with `retention.ms=-1`, `LogAppendTime` |

## Performance (recorded)
- Producer: 40,558 events in 6.97 s (**5,817 events/s**), `acks=all`.
- Flink: 40,558 events in 711.6 s (**≈57 events/s** end to end; 683.7 s ≈ 59/s in the earlier run),
  4-core Codespace, TaskManager 1.8 GB. The limit is PyFlink with bundle size 1 (one Java↔Python
  round trip per element per Python operator). A Java/SQL implementation or larger bundles would be
  much faster; bundle size 1 was kept because it is the setting under which the late-event tests pass.
- Checkpoints during the replay: median end-to-end 11.4 s; failed checkpoints are all from the bounded
  job's shutdown ("task is closing", "unknown task", "coordinator is suspending").

## Known limitations
- One Kafka broker, one partition per topic, replication 1: no broker fault tolerance; parallelism
  shows in the keyed stages only.
- Throughput ≈ 57–59 events/s (PyFlink, bundle size 1).
- Results go to Kafka only; `traffic_window_metrics_v1` (M4) will materialise them as Parquet with the
  upsert rule above.
- `run_m3.sh` deletes and re-creates the topics of the part it runs, so each run starts clean; the
  produced events and collected records are kept in `data/local/m3/<run>/` (git-ignored) and the
  verification reports in `evidence/`.
- The DLQ is evidence, not reprocessed automatically.
