"""PyFlink job: Kafka replay events -> event-time window metrics (spec T04/AC04).

Pipeline (DataStream API, Flink 1.18):
  KafkaSource(raw JSON strings)
   -> Parse            malformed -> DLQ {reason: not_json | missing_* | bad_event_time | ...}
   -> timestamps + watermarks: BoundedOutOfOrderness(max_out_of_orderness), event_time = UCI hour start
   -> key_by(event_id) Dedup   duplicate event_id -> DLQ {reason: duplicate}; seen-state cleared by an
                               event-time timer at event_time + horizon
   -> key_by(station_id) TumblingEventTimeWindows(size) .allowed_lateness(L) .side_output_late_data
        WindowFn: emits one JSON result per firing: firing=on_time (update_seq 0) when the watermark
        passes the window end, firing=late_update (update_seq 1, 2, ...) for each event that arrives
        within L afterwards. Events later than that -> DLQ {reason: too_late}.
   -> KafkaSink(metrics topic), KafkaSink(DLQ topic), delivery guarantee from configs/streaming.yaml

Submit (inside flink-jobmanager):
  flink run -d -py /workspace/src/streaming/flink_window_job.py --mode replay --run-id R [--bounded]
--mode test reads/writes the test-* topics (synthetic station; spec §8 keeps injected data separate).
--bounded stops at the topic's current end; Flink then emits a final MAX watermark, so every window
fires once and the job FINISHES (used for the full replay). Without it the job runs until cancelled.
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import sys
from pathlib import Path

import yaml
from pyflink.common import Configuration, Duration, Types, WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.time import Time
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream import OutputTag, StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (DeliveryGuarantee, KafkaOffsetsInitializer,
                                                 KafkaRecordSerializationSchema, KafkaSink, KafkaSource)
from pyflink.datastream.functions import KeyedProcessFunction, ProcessFunction, ProcessWindowFunction
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.datastream.window import TumblingEventTimeWindows

from src.common.config import load_config
from src.streaming.events import HOUR_MS, parse, to_iso, window_metrics

ROOT = Path(__file__).resolve().parents[2]
MALFORMED = OutputTag("malformed", Types.STRING())
DUPLICATE = OutputTag("duplicate", Types.STRING())
LATE = OutputTag("late", Types.PICKLED_BYTE_ARRAY())


def wm_iso(ms: int) -> str:
    """Watermark as text; Long.MIN (no watermark yet) and Long.MAX (end of a bounded input) are named."""
    if ms <= -(2 ** 62):
        return "none_yet"
    if ms >= 2 ** 62:
        return "end_of_input"
    return to_iso(ms)


def now_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def dlq(reason: str, run_id: str, event: dict | None = None, payload: str | None = None, **extra) -> str:
    d = {"reason": reason, "job_run_id": run_id, "detected_at": now_iso(), **extra}
    if event is not None:
        d.update({k: event.get(k) for k in ("event_id", "station_id", "event_time", "traffic_volume", "record_kind")})
    if payload is not None:
        d["payload"] = payload[:500]
    return json.dumps(d)


class Parse(ProcessFunction):
    def __init__(self, run_id: str) -> None:
        self.run_id = run_id

    def process_element(self, value, ctx):
        e, reason = parse(value)
        if e is None:
            yield MALFORMED, dlq(reason, self.run_id, payload=value if isinstance(value, str) else repr(value))
        else:
            yield e


class EventTime(TimestampAssigner):
    def extract_timestamp(self, value, record_timestamp) -> int:
        return int(value["event_ts_ms"])


class Dedup(KeyedProcessFunction):
    def __init__(self, horizon_ms: int, run_id: str) -> None:
        self.horizon_ms, self.run_id = horizon_ms, run_id
        self.seen = None

    def open(self, runtime_context):
        self.seen = runtime_context.get_state(ValueStateDescriptor("seen", Types.BOOLEAN()))

    def process_element(self, value, ctx):
        if self.seen.value():
            yield DUPLICATE, dlq("duplicate", self.run_id, value,
                                 watermark=wm_iso(ctx.timer_service().current_watermark()))
            return
        self.seen.update(True)
        ctx.timer_service().register_event_time_timer(int(value["event_ts_ms"]) + self.horizon_ms)
        yield value

    def on_timer(self, timestamp, ctx):
        self.seen.clear()
        yield from ()          # PyFlink requires an iterable from on_timer, even when nothing is emitted


class WindowFn(ProcessWindowFunction):
    FIRES = ValueStateDescriptor("fires", Types.INT())

    def __init__(self, size_h: int, expected: int, lateness_h: int, run_id: str, topic: str) -> None:
        self.size_h, self.expected, self.lateness_h = size_h, expected, lateness_h
        self.run_id, self.topic = run_id, topic

    def process(self, key, context, elements):
        events = list(elements)
        fires = context.window_state().get_state(self.FIRES)
        seq = fires.value() or 0
        fires.update(seq + 1)
        m = window_metrics([int(e["traffic_volume"]) for e in events], context.window().start,
                           self.size_h, self.expected)
        kinds = sorted({e["record_kind"] for e in events})
        yield json.dumps({
            "station_id": key, **m,
            "firing": "on_time" if seq == 0 else "late_update", "update_seq": seq,
            "watermark_at_emit": wm_iso(context.current_watermark()),
            "late_event_handling": f"allowed lateness {self.lateness_h} h -> late_update; later -> DLQ too_late",
            "record_kind": "historical_event_replay_window" if kinds == ["historical_event_replay"]
                           else "+".join(kinds) + "_window",
            "source_topic": self.topic, "job_run_id": self.run_id, "emitted_at": now_iso()})

    def clear(self, context):
        context.window_state().get_state(self.FIRES).clear()


def kafka_sink(bootstrap: str, topic: str, guarantee: str) -> KafkaSink:
    return (KafkaSink.builder().set_bootstrap_servers(bootstrap)
            .set_record_serializer(KafkaRecordSerializationSchema.builder().set_topic(topic)
                                   .set_value_serialization_schema(SimpleStringSchema()).build())
            .set_delivery_guarantee(getattr(DeliveryGuarantee, guarantee)).build())


def build(env: StreamExecutionEnvironment, cfg: dict, platform: dict, mode: str, run_id: str,
          bounded: bool) -> None:
    t = cfg["topics"]
    src_t, out_t, dlq_t = ((t["replay"], t["metrics"], t["dlq"]) if mode == "replay"
                           else (t["test_replay"], t["test_metrics"], t["test_dlq"]))
    bootstrap = platform["kafka"]["bootstrap_servers"]
    size_h = int(cfg["window"]["size_hours"])
    lateness_h = int(cfg["watermark"]["allowed_lateness_hours"])
    builder = (KafkaSource.builder().set_bootstrap_servers(bootstrap).set_topics(src_t["name"])
               .set_group_id(f"bdfp-flink-{mode}-{run_id}")
               .set_starting_offsets(KafkaOffsetsInitializer.earliest())
               .set_value_only_deserializer(SimpleStringSchema()))
    if bounded:
        builder = builder.set_bounded(KafkaOffsetsInitializer.latest())
    raw = env.from_source(builder.build(), WatermarkStrategy.no_watermarks(), f"kafka:{src_t['name']}") \
             .set_parallelism(int(src_t["partitions"])).name("source")

    parsed = raw.process(Parse(run_id)).set_parallelism(int(src_t["partitions"])).name("parse")
    wm = (WatermarkStrategy
          .for_bounded_out_of_orderness(Duration.of_minutes(int(cfg["watermark"]["max_out_of_orderness_minutes"])))
          .with_timestamp_assigner(EventTime()))
    # Watermarks are generated with parallelism 1 (the job default; see configs/streaming.yaml
    # parallelism): with several generators fed round-robin, the combined watermark (the minimum)
    # lags and the last windows never fire (runs m3_20261121T090639Z, m3_20261121T091035Z).
    timed = parsed.assign_timestamps_and_watermarks(wm).name("event-time+watermarks")
    keyed_p = int(cfg["parallelism"]["keyed"])
    deduped = (timed.key_by(lambda e: e["event_id"], key_type=Types.STRING())
               .process(Dedup(int(cfg["dedup"]["horizon_hours"]) * HOUR_MS, run_id))
               .set_parallelism(keyed_p).name("dedup"))
    windows = (deduped.key_by(lambda e: e["station_id"], key_type=Types.STRING())
               .window(TumblingEventTimeWindows.of(Time.hours(size_h)))
               .allowed_lateness(lateness_h * HOUR_MS)
               .side_output_late_data(LATE)
               .process(WindowFn(size_h, int(cfg["window"]["expected_events_per_window"]), lateness_h,
                                 run_id, src_t["name"]), output_type=Types.STRING())
               .set_parallelism(keyed_p).name("tumbling-window"))
    too_late = windows.get_side_output(LATE).map(
        lambda e: dlq("too_late", run_id, e), output_type=Types.STRING()).set_parallelism(keyed_p).name("too-late")
    guarantee = cfg["delivery"]["sink_guarantee"]
    windows.sink_to(kafka_sink(bootstrap, out_t["name"], guarantee)).set_parallelism(keyed_p).name("sink:metrics")
    (parsed.get_side_output(MALFORMED).union(deduped.get_side_output(DUPLICATE), too_late)
     .sink_to(kafka_sink(bootstrap, dlq_t["name"], guarantee)).set_parallelism(keyed_p).name("sink:dlq"))


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--mode", choices=["replay", "test"], default="replay")
    ap.add_argument("--run-id", required=True)
    ap.add_argument("--bounded", action="store_true")
    args = ap.parse_args()
    cfg = yaml.safe_load((ROOT / "configs" / "streaming.yaml").read_text())
    platform = load_config()
    py = cfg["python_runtime"]
    conf = Configuration()
    conf.set_integer("python.fn-execution.bundle.size", int(py["bundle_size"]))
    conf.set_integer("python.fn-execution.bundle.time", int(py["bundle_time_ms"]))
    env = StreamExecutionEnvironment.get_execution_environment(conf)
    env.get_config().set_auto_watermark_interval(int(py["auto_watermark_interval_ms"]))
    env.set_parallelism(int(cfg["parallelism"]["default"]))
    env.enable_checkpointing(int(cfg["delivery"]["checkpoint_interval_s"]) * 1000)
    build(env, cfg, platform, args.mode, args.run_id, args.bounded)
    env.execute(f"bdfp-m3-{args.mode}-{args.run_id}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
