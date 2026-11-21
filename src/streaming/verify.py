"""Check Flink window results against the oracle (spec AC04: results must be right, not just produced).

Inputs: the producer's --events-out JSONL, and collect.py JSONL of the metrics and DLQ topics.
Output: JSON report; exit 0 only if every expected window is present with the expected values.

Run: python3 -m src.streaming.verify --events E.jsonl --metrics M.jsonl --dlq D.jsonl --out V.json [--station ID]
"""
from __future__ import annotations

import argparse
import collections
import json
import sys
from pathlib import Path

import yaml

from src.common.config import PROJECT_ROOT
from src.streaming.events import compare, final_table, oracle


def read_values(path: str) -> list[dict]:
    out = []
    for line in Path(path).read_text(encoding="utf-8").splitlines():
        if line.strip():
            out.append(json.loads(json.loads(line)["value"]))
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--events", required=True)
    ap.add_argument("--metrics", required=True)
    ap.add_argument("--dlq", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--station", default="301")
    ap.add_argument("--job-run-id", help="only results/DLQ records written by this Flink job run")
    args = ap.parse_args()
    cfg = yaml.safe_load((PROJECT_ROOT / "configs" / "streaming.yaml").read_text())
    size, exp_n = int(cfg["window"]["size_hours"]), int(cfg["window"]["expected_events_per_window"])

    events = [json.loads(l) for l in Path(args.events).read_text(encoding="utf-8").splitlines() if l.strip()]
    mine = (lambda r: r.get("job_run_id") == args.job_run_id) if args.job_run_id else (lambda r: True)
    results = [r for r in read_values(args.metrics) if r["station_id"] == args.station and mine(r)]
    dlq = [d for d in read_values(args.dlq) if mine(d)]
    final, stats = final_table(results)
    expected = oracle(events, size, exp_n, station_id=args.station)
    cmp = compare(final, expected)
    replay = [e for e in events if e["record_kind"] != "replay_flush_marker"]
    report = {
        "events_produced": len(events), "replay_events": len(replay),
        "distinct_event_ids": len({e["event_id"] for e in replay}),
        "window_results": stats, "final_windows": len(final),
        "events_in_final_windows": sum(r["event_count"] for r in final.values()),
        "dlq_reasons": dict(collections.Counter(d["reason"] for d in dlq)),
        "comparison": cmp,
        "watermark_examples": [{k: r[k] for k in ("window_start_utc", "firing", "update_seq", "watermark_at_emit")}
                               for r in results[:3]],
    }
    Path(args.out).write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")
    print(json.dumps({k: report[k] for k in ("replay_events", "final_windows", "events_in_final_windows",
                                              "window_results", "dlq_reasons")}), flush=True)
    print("comparison:", json.dumps({k: cmp[k] for k in ("match", "windows_expected", "windows_got",
                                                         "n_missing", "n_extra", "n_wrong")}), flush=True)
    return 0 if cmp["match"] else 1


if __name__ == "__main__":
    sys.exit(main())
