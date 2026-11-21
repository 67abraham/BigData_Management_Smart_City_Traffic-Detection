"""Load the dashboard's data from committed evidence and outputs (spec T09). No Spark, no HDFS.

Every loader returns (data, source) where source names the file it read, so the dashboard can show
where each number comes from. Runs are chosen in configs/dashboard.yaml.
"""
from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any

import yaml

ROOT = Path(__file__).resolve().parents[2]


def config() -> dict[str, Any]:
    return yaml.safe_load((ROOT / "configs" / "dashboard.yaml").read_text(encoding="utf-8"))


def _json(rel: str) -> tuple[Any, str]:
    return json.loads((ROOT / rel).read_text(encoding="utf-8")), rel


def _csv(rel: str) -> tuple[list[dict[str, str]], str]:
    with (ROOT / rel).open(encoding="utf-8") as fh:
        return list(csv.DictReader(fh)), rel


def export_dir(cfg: dict[str, Any]) -> str:
    run = cfg["runs"]["dashboard_export"]
    if run == "latest":
        dirs = sorted(p.name for p in (ROOT / "outputs" / "dashboard").glob("m5_*") if (p / "manifest.json").exists())
        if not dirs:
            raise FileNotFoundError("no outputs/dashboard/m5_*/ - run src/visualisation/export_dashboard_data.py")
        run = dirs[-1]
    return f"outputs/dashboard/{run}"


# ---------- sources (AC01) ----------
def sources(cfg: dict[str, Any]) -> tuple[list[dict[str, Any]], str]:
    run = cfg["runs"]["ingestion"]
    base = f"evidence/AC01/{run}"
    rows = []
    for s in ("uci_metro", "open_meteo", "nyc_tlc", "kitti"):
        p, _ = _json(f"{base}/profile_{s}.json")
        status = p.get("manifest_status_counts", {})
        integ = p.get("integrity", {})
        if s == "uci_metro":
            n, lo, hi = p["rows_parsed"], p["date_time_min"], p["date_time_max"]
        elif s == "open_meteo":
            n, lo, hi = p["rows"], p["time_min"], p["time_max"]
        elif s == "nyc_tlc":
            n, lo, hi = p["rows_total"], "2023-01", "2023-03"
        else:
            n, lo, hi = p["label_lines"], "-", "-"
        rows.append({"source": s, "records": n, "first": lo, "last": hi,
                     "files": integ.get("files_in_manifest"), "sha256_match": integ.get("sha256_match"),
                     "download_status": ", ".join(f"{k} {v}" for k, v in status.items())})
    return rows, f"{base}/profile_*.json (run {run})"


# ---------- processing volume ----------
def layer_counts(cfg: dict[str, Any]) -> tuple[dict[str, Any], str]:
    d = export_dir(cfg)
    data, src = _json(f"{d}/layer_counts.json")
    return data, src


def layer_table(counts: dict[str, Any]) -> list[dict[str, Any]]:
    rows = []
    for ds, e in counts.items():
        silver = e.get("silver_records_by_outcome", {})
        rows.append({"dataset": ds, "bronze": e.get("bronze_records"),
                     "silver_accepted": silver.get("accepted"), "silver_quarantine": silver.get("quarantine", 0) if silver else None,
                     "silver_rejected": silver.get("rejected", 0) if silver else None,
                     "gold": e.get("gold_rows"),
                     "products": ", ".join(f"{k} {v:,}" for k, v in (e.get("product_rows") or {}).items())})
    return rows


# ---------- quality (AC05) ----------
def quality(cfg: dict[str, Any]) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[str]]:
    srcs, outcomes, rules = [], [], []
    for ds, run in (("uci_metro", cfg["runs"]["silver_uci_open_meteo"]),
                    ("open_meteo", cfg["runs"]["silver_uci_open_meteo"]), ("nyc_tlc", cfg["runs"]["silver_nyc"])):
        q, src = _json(f"evidence/AC05/{run}/quality_{ds}.json")
        srcs.append(f"{src} (run {run})")
        rec = q["reconciliation"]
        for o in ("accepted", "quarantine", "rejected"):
            outcomes.append({"dataset": ds, "outcome": o, "records": q["outcomes"].get(o, {}).get("records", 0),
                             "reconciled": rec["reconciled"]})
        for rule, n in sorted(q["record_rule_violations"].items(), key=lambda x: -x[1]):
            rules.append({"dataset": ds, "rule": rule, "records": n})
    return outcomes, rules, srcs


# ---------- batch analytics (AC06) ----------
def analytics(cfg: dict[str, Any], name: str) -> tuple[list[dict[str, str]], str]:
    run = cfg["runs"]["analytics"]
    rows, src = _csv(f"outputs/reports/analytics/{run}/{name}.csv")
    return rows, f"{src} (run {run})"


def daily_traffic(cfg: dict[str, Any]) -> tuple[list[dict[str, str]], str]:
    return _csv(f"{export_dir(cfg)}/traffic_daily.csv")


# ---------- streaming (AC04, AC08) ----------
def window_metrics(cfg: dict[str, Any]) -> tuple[list[dict[str, str]], str]:
    return _csv(f"{export_dir(cfg)}/window_metrics.csv")


def replay_verify(cfg: dict[str, Any]) -> tuple[dict[str, Any], str]:
    run = cfg["runs"]["replay"]
    v, src = _json(f"evidence/AC04/{run}/replay_verify.json")
    p, _ = _json(f"evidence/AC04/{run}/replay_producer.json")
    j, _ = _json(f"evidence/AC04/{run}/replay_job.json")
    return {"verify": v, "producer": p, "job_duration_s": j["duration"] / 1000}, f"{src} (run {run})"


def edge_case(cfg: dict[str, Any]) -> tuple[dict[str, Any], str]:
    run = cfg["runs"]["edge"]
    c, src = _json(f"evidence/AC04/{run}/edge_check.json")
    return c, f"{src} (run {run})"


def recovery(cfg: dict[str, Any]) -> tuple[dict[str, Any], str]:
    import datetime as dt
    run = cfg["runs"]["recovery"]
    base = f"evidence/AC08/{run}"
    f = lambda ms: dt.datetime.fromtimestamp(ms / 1000, dt.timezone.utc).strftime("%H:%M:%S")
    ex, _ = _json(f"{base}/recovery_exceptions.json")
    ck, _ = _json(f"{base}/recovery_checkpoints.json")
    before, _ = _json(f"{base}/checkpoints_before_kill.json")
    job, _ = _json(f"{base}/recovery_job.json")
    v, _ = _json(f"{base}/verify.json")
    kill = (ROOT / base / "kill_time.txt").read_text().strip()
    e = ex["exceptionHistory"]["entries"][0]
    timeline = [
        {"time_utc": f(job["start-time"]), "event": "job started"},
        {"time_utc": f(before["latest"]["completed"]["latest_ack_timestamp"]),
         "event": f"checkpoint chk-{before['latest']['completed']['id']} completed"},
        {"time_utc": kill[11:19], "event": "TaskManager killed (docker compose kill)"},
        {"time_utc": f(e["timestamp"]), "event": "JobManager: " + e["stacktrace"].splitlines()[0].split(": ", 1)[-1][:90]},
        {"time_utc": f(job["timestamps"]["RUNNING"]),
         "event": f"RUNNING again, restored from chk-{ck['latest']['restored']['id']}"},
    ]
    return {"timeline": timeline, "verify": v}, f"{base}/ (run {run})"


# ---------- performance (AC06) ----------
def benchmark(cfg: dict[str, Any]) -> tuple[list[dict[str, Any]], str]:
    run = cfg["runs"]["benchmark"]
    b, src = _json(f"outputs/benchmarks/{run}/benchmark_results.json")
    rows = []
    for r in b["results"]:
        conf = ",".join(f"pushdown={v}" for k, v in r["conf"].items())
        rows.append({"pair": r["pair"], "query": r["query"], "config": f"{r['layout']}" + (f" ({conf})" if conf else ""),
                     "median_s": r["median_s"], "warmup_s": r["warmup_s"],
                     "runs_s": ", ".join(str(x["elapsed_s"]) for x in r["runs"] if not x["warmup"]),
                     "mb_read": round(r["input_bytes_median"] / 1e6, 1),
                     "rows_read": r["runs"][1]["input_records"]})
    return rows, f"{src} (run {run})"


# ---------- abnormal cases / failed runs ----------
def failed_runs() -> list[dict[str, str]]:
    """Runs kept in evidence that failed (a FAIL line in summary.txt) or carry a note (FAILED_RUN.md /
    NOTE.md), with the failed steps and the note's first explanatory line."""
    runs: dict[tuple[str, str], dict[str, str]] = {}
    for f in sorted((ROOT / "evidence").glob("AC*/*/summary.txt")):
        fails = [l.split("->")[0].replace("FAIL", "").strip() for l in f.read_text(encoding="utf-8").splitlines()
                 if l.startswith("FAIL")]
        if fails:
            runs[(f.parts[-3], f.parts[-2])] = {"failed_steps": "; ".join(fails), "note": "", "file": ""}
    for f in sorted((ROOT / "evidence").glob("AC*/*/*.md")):
        if f.name not in ("FAILED_RUN.md", "NOTE.md", "NOTE_gold_plan.md"):
            continue
        lines = [l.strip() for l in f.read_text(encoding="utf-8").splitlines() if l.strip() and not l.startswith("#")]
        e = runs.setdefault((f.parts[-3], f.parts[-2]), {"failed_steps": "", "note": "", "file": ""})
        e["note"], e["file"] = (lines[0][:220] if lines else ""), str(f.relative_to(ROOT))
    return [{"criterion": c, "run": r, **v} for (c, r), v in sorted(runs.items())]
