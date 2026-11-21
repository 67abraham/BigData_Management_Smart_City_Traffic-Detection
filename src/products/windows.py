"""Final window rows for traffic_window_metrics_v1 from Flink result records (spec T07). Pure Python."""
from __future__ import annotations

import datetime as dt

from src.streaming.events import final_table


def window_rows(results: list[dict], run_id: str) -> list[dict]:
    """Final row per window (highest update_seq) plus how many result rows were seen. Pure."""
    final, _ = final_table(results)
    seen: dict[tuple[str, str], int] = {}
    for r in results:
        k = (r["station_id"], r["window_start_utc"])
        seen[k] = seen.get(k, 0) + 1
    rows = []
    for k, r in sorted(final.items()):
        rows.append({**{f: r[f] for f in ("station_id", "window_size_hours", "event_count", "expected_events",
                                          "completeness", "volume_sum", "volume_avg", "volume_min", "volume_max",
                                          "watermark_at_emit", "record_kind", "source_topic", "job_run_id")},
                     "window_start_utc": dt.datetime.strptime(r["window_start_utc"], "%Y-%m-%dT%H:%M:%SZ"),
                     "window_end_utc": dt.datetime.strptime(r["window_end_utc"], "%Y-%m-%dT%H:%M:%SZ"),
                     "final_firing": r["firing"], "final_update_seq": int(r["update_seq"]),
                     "firings_seen": seen[k], "_product_run_id": run_id,
                     "window_year": int(r["window_start_utc"][:4])})
    return rows


def csv_value(v: object) -> str:
    """CSV cell for the manifest copy: lists joined with ';', booleans lower-case, NULL empty."""
    if v is None:
        return ""
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, list):
        return ";".join(str(x) for x in v)
    return str(v)


def write_manifest_csv(path, cols: list[str], rows: list[dict]) -> None:
    """RFC 4180 CSV (values with commas are quoted; e.g. origin contains a comma)."""
    import csv
    with open(path, "w", encoding="utf-8", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(cols)
        for r in rows:
            w.writerow([csv_value(r[k]) for k in cols])
