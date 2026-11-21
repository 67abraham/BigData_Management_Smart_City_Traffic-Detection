"""Read a Kafka topic from the beginning and save every record as JSONL (consumption evidence, AC04).

Stops when no new record arrived for --idle seconds (or after --timeout). Uses no consumer group, so it
never commits offsets and can be re-run. Runs where kafka-python-ng is installed (spark-master).

Run: python3 -m src.streaming.collect --topic T --out F.jsonl [--idle 15] [--timeout 600]
"""
from __future__ import annotations

import argparse
import json
import sys
import time

from src.common.config import load_config


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--topic", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--idle", type=float, default=15.0)
    ap.add_argument("--timeout", type=float, default=600.0)
    args = ap.parse_args()
    from kafka import KafkaConsumer

    c = KafkaConsumer(args.topic, bootstrap_servers=load_config()["kafka"]["bootstrap_servers"],
                      group_id=None, enable_auto_commit=False, auto_offset_reset="earliest",
                      consumer_timeout_ms=int(args.idle * 1000))
    n, bad, t0 = 0, 0, time.time()
    with open(args.out, "w", encoding="utf-8") as fh:
        for msg in c:
            v = msg.value.decode("utf-8", errors="replace")
            try:
                json.loads(v)
            except ValueError:
                bad += 1
            fh.write(json.dumps({"partition": msg.partition, "offset": msg.offset, "kafka_ts": msg.timestamp,
                                 "value": v}) + "\n")
            n += 1
            if time.time() - t0 > args.timeout:
                break
    c.close()
    print(json.dumps({"topic": args.topic, "records": n, "non_json": bad, "out": args.out}), flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
