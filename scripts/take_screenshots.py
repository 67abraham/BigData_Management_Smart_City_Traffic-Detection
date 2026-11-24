#!/usr/bin/env python3
"""Screenshots of the cluster UIs and the dashboard as visual evidence (spec §9 D07: "verifiable screenshots").

Screenshots only support the logs; every number is in the run's log/JSON files. Each shot is checked:
the page must contain the expected text (e.g. the run's own job id, "restored"), otherwise it is FAIL.

Needs a headless browser, outside the project requirements (optional tool):
    python3 -m venv /tmp/pw && /tmp/pw/bin/pip install playwright==1.47.0 && /tmp/pw/bin/playwright install chromium
    /tmp/pw/bin/python scripts/take_screenshots.py --spark-app app-... --flink-job <jid> --runs m2_...,m3_...,m5_...
(playwright 1.47 is the last release that installs Chromium on Ubuntu 20.04, the Codespace image's base.)
Writes evidence/<AC>/screenshots_<UTC>/<name>.png and an index.md per AC folder.
"""
from __future__ import annotations

import argparse
import datetime as dt
import pathlib
import sys

from playwright.sync_api import Page, sync_playwright


def shots(spark_app: str, flink_job: str) -> list[dict]:
    """(AC, file name, URL, text the page must show, optional Streamlit tab to click, caption)."""
    return [
        dict(ac="AC02", name="hdfs_namenode_overview", url="http://localhost:9870/dfshealth.html#tab-overview",
             expect=["Live Nodes", "Summary"], caption="HDFS NameNode overview: capacity, live DataNodes, blocks"),
        dict(ac="AC02", name="hdfs_datanodes", url="http://localhost:9870/dfshealth.html#tab-datanode",
             expect=["datanode1", "datanode2", "datanode3"], caption="HDFS DataNodes tab: the three live DataNodes"),
        dict(ac="AC02", name="hdfs_browse_datalake", url="http://localhost:9870/explorer.html#/datalake",
             expect=["bronze", "silver", "gold"], caption="HDFS file browser: Bronze/Silver/Gold layers under /datalake"),
        dict(ac="AC03", name="spark_history_jobs", url=f"http://localhost:18080/history/{spark_app}/jobs/",
             expect=["bdfp-m2-silver-nyc_tlc", "Completed Jobs"], caption=f"Spark History: jobs of the NYC Silver job {spark_app}"),
        dict(ac="AC03", name="spark_history_stages", url=f"http://localhost:18080/history/{spark_app}/stages/",
             expect=["Shuffle", "Completed Stages"], caption=f"Spark History: stages with shuffle read/write ({spark_app})"),
        dict(ac="AC03", name="spark_history_sql", url=f"http://localhost:18080/history/{spark_app}/SQL/",
             expect=["Completed Queries"], caption=f"Spark History: SQL queries / plans ({spark_app})"),
        dict(ac="AC04", name="flink_jobs_overview", url="http://localhost:8082/#/job/completed",
             expect=["bdfp-m3-replay"], caption="Flink UI: completed jobs of the M3 run (replay, edge, recovery)"),
        dict(ac="AC08", name="flink_recovery_checkpoints",
             url=f"http://localhost:8082/#/job/completed/{flink_job}/checkpoints",
             expect=["Restored", "chk-"], caption=f"Flink UI: checkpoints of recovery job {flink_job}, restored after the TaskManager kill"),
        dict(ac="AC09", name="dashboard_pipeline", url="http://localhost:8501", tab="Pipeline",
             expect=["Pipeline"], caption="Dashboard: Pipeline tab (processing volume, runs shown; pinned in configs/dashboard.yaml)"),
        dict(ac="AC09", name="dashboard_quality", url="http://localhost:8501", tab="Quality",
             expect=["Quality"], caption="Dashboard: Quality tab"),
        dict(ac="AC09", name="dashboard_streaming", url="http://localhost:8501", tab="Streaming",
             expect=["Streaming"], caption="Dashboard: Streaming tab (window metrics)"),
        dict(ac="AC09", name="dashboard_performance", url="http://localhost:8501", tab="Performance",
             expect=["Performance"], caption="Dashboard: Performance tab (benchmark comparison)"),
        dict(ac="AC09", name="dashboard_abnormal", url="http://localhost:8501", tab="Abnormal cases",
             expect=["Abnormal"], caption="Dashboard: Abnormal cases tab (late/duplicate/malformed, failure + recovery)"),
    ]


def missing_text(body: str, expect: list[str]) -> list[str]:
    """Expected strings that the page text does not contain (case-sensitive)."""
    return [e for e in expect if e not in body]


def take(page: Page, s: dict, out: pathlib.Path) -> tuple[bool, str]:
    # Streamlit scrolls inside its own container, so a full-page shot needs a tall window
    page.set_viewport_size({"width": 1440, "height": 3200 if s.get("tab") else 1000})
    page.goto(s["url"], wait_until="networkidle", timeout=60_000)
    if s.get("tab"):
        page.get_by_role("tab", name=s["tab"], exact=True).click()
    page.wait_for_timeout(4_000)  # Angular (Flink) / Streamlit render after network idle
    miss = missing_text(page.inner_text("body"), s["expect"])
    page.screenshot(path=str(out), full_page=True)
    return (not miss, "ok" if not miss else f"page lacks {miss}")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--spark-app", required=True)
    ap.add_argument("--flink-job", required=True)
    ap.add_argument("--runs", required=True, help="run ids the shots belong to, written to each index.md")
    a = ap.parse_args()
    stamp = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    root = pathlib.Path(__file__).resolve().parent.parent / "evidence"
    fails, rows = 0, {}
    with sync_playwright() as p:
        b = p.chromium.launch()
        page = b.new_page(viewport={"width": 1440, "height": 1000})
        for s in shots(a.spark_app, a.flink_job):
            d = root / s["ac"] / f"screenshots_{stamp}"
            d.mkdir(parents=True, exist_ok=True)
            ok, msg = take(page, s, d / f"{s['name']}.png")
            fails += not ok
            print(f"{'PASS' if ok else 'FAIL'}  {s['ac']}/{d.name}/{s['name']}.png  {msg}")
            rows.setdefault(d, []).append(f"| `{s['name']}.png` | {s['caption']} | `{s['url']}` | {'PASS' if ok else 'FAIL: ' + msg} |")
        b.close()
    for d, r in rows.items():
        (d / "index.md").write_text(
            f"# Screenshots {stamp}\n\nTaken by `scripts/take_screenshots.py` (headless Chromium, Playwright 1.47) "
            f"right after run(s) {a.runs}. The cluster UIs (HDFS, Spark History, Flink) show that run's jobs; "
            "the dashboard shows the runs pinned in `configs/dashboard.yaml` (each panel names its run).\n"
            "Supporting evidence only: the numbers are in the runs' log/JSON files.\n"
            "Check = the page contained the expected text when the shot was taken.\n\n"
            "| File | Shows | URL | Check |\n|---|---|---|---|\n" + "\n".join(r) + "\n")
    print(f"failures: {fails}")
    return 1 if fails else 0


if __name__ == "__main__":
    sys.exit(main())
