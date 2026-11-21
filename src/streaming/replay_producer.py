"""Kafka producer: historical event replay of Silver UCI hours (spec T04/AC04).

Reads /datalake/silver/uci_traffic_hourly/v1/outcome=accepted with Spark, orders the hours by event
time and publishes one JSON event per hour (src/streaming/events.py format, record_kind
historical_event_replay, event_time = hour_start_utc) to the replay topic, key = station_id.

--rate N        events per second (0 = as fast as possible); slow rates make room for a restart test
--limit N       only the first N hours (by event time)
--flush-marker  finish with one replay_flush_marker event 30 days after the last hour, so an
                unbounded Flink job's watermark passes every real window
--events-out F  JSONL of exactly what was sent (input to the oracle; kept in data/local/, not git)
--summary-out F JSON summary: counts, first/last event time, elapsed time, events/s

Run: spark-submit /workspace/src/streaming/replay_producer.py --run-id R --events-out ... --summary-out ...
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import sys
import time
from pathlib import Path

import yaml
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.common.config import PROJECT_ROOT, load_config
from src.streaming.events import HOUR_MS, make_event, to_iso, to_ms


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--run-id", required=True)
    ap.add_argument("--rate", type=float, default=0.0)
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--flush-marker", action="store_true")
    ap.add_argument("--events-out", required=True)
    ap.add_argument("--summary-out", required=True)
    args = ap.parse_args()

    from kafka import KafkaProducer   # kafka-python-ng (docker/spark/requirements.txt)

    cfg = yaml.safe_load((PROJECT_ROOT / "configs" / "streaming.yaml").read_text())
    q = yaml.safe_load((PROJECT_ROOT / "configs" / "quality.yaml").read_text())["uci_metro"]
    platform = load_config()
    root = f"{platform['hdfs']['uri']}{platform['hdfs']['datalake_root']}"
    src = f"{root}/silver/{q['silver_dataset']}/{q['version']}/outcome=accepted"

    spark = SparkSession.builder.appName("bdfp-m3-replay-producer").getOrCreate()
    df = (spark.read.option("basePath", src).parquet(src)
          .select("station_id", "road", "direction", "traffic_volume", "_silver_run_id",
                  F.date_format("hour_start_utc", "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("event_time"))
          .orderBy("event_time"))
    if args.limit:
        df = df.limit(args.limit)
    rows = [r.asDict() for r in df.collect()]
    spark.stop()

    topic = cfg["topics"]["replay"]["name"]
    producer = KafkaProducer(bootstrap_servers=platform["kafka"]["bootstrap_servers"], acks="all",
                             linger_ms=5, retries=5,
                             key_serializer=lambda k: k.encode(), value_serializer=lambda v: v.encode())
    out = Path(args.events_out)
    out.parent.mkdir(parents=True, exist_ok=True)
    sent, t0 = 0, time.perf_counter()
    with out.open("w", encoding="utf-8") as fh:
        def send(e: dict) -> None:
            nonlocal sent
            line = json.dumps(e)
            producer.send(topic, key=e["station_id"], value=line)
            fh.write(line + "\n")
            sent += 1
            if args.rate > 0:
                target = sent / args.rate
                lag = target - (time.perf_counter() - t0)
                if lag > 0:
                    time.sleep(lag)

        for r in rows:
            send(make_event(r["station_id"], r["event_time"], r["traffic_volume"],
                            record_kind=cfg["replay"]["record_kind"], replay_run_id=args.run_id,
                            road=r["road"], direction=r["direction"],
                            source=f"silver/{q['silver_dataset']}/{q['version']} run {r['_silver_run_id']}"))
        if args.flush_marker and rows:
            last = to_ms(rows[-1]["event_time"])
            send(make_event("FLUSH", to_iso(last + 30 * 24 * HOUR_MS), 0, record_kind="replay_flush_marker",
                            replay_run_id=args.run_id, source="end-of-replay marker"))
        producer.flush()
    elapsed = time.perf_counter() - t0
    producer.close()
    summary = {"run_id": args.run_id, "topic": topic, "source": src, "events_sent": sent,
               "replay_events": len(rows), "flush_marker": bool(args.flush_marker and rows),
               "first_event_time": rows[0]["event_time"] if rows else None,
               "last_event_time": rows[-1]["event_time"] if rows else None,
               "rate_limit_per_s": args.rate, "elapsed_s": round(elapsed, 2),
               "events_per_s": round(sent / elapsed, 1) if elapsed else None,
               "record_kind": cfg["replay"]["record_kind"],
               "finished_at": dt.datetime.now(dt.timezone.utc).isoformat()}
    Path(args.summary_out).write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(json.dumps(summary), flush=True)
    return 0 if sent == len(rows) + int(summary["flush_marker"]) else 1


if __name__ == "__main__":
    sys.exit(main())
