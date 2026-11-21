"""Render a Silver job's REPORT_JSON as a Markdown quality report (spec T05). Pure Python.

Run: python -m src.quality.report_md <report.json> <rules.json> > quality_report.md
"""
from __future__ import annotations

import json
import sys
from typing import Any, Mapping, Sequence


def _n(x: Any) -> str:
    return f"{x:,}" if isinstance(x, int) else str(x)


def render(rep: Mapping[str, Any], rules: Sequence[Mapping[str, str]]) -> str:
    rec = rep["reconciliation"]
    out = [f"# Quality report — `{rep['dataset']}` {rep['version']}", "",
           f"- Run: `{rep.get('run_id')}` · Spark app `{rep.get('spark_app_id')}`",
           f"- Input: `{rep.get('input')}` (Bronze ingest_date {rep['bronze_ingest_date']})",
           f"- Output: `{rep.get('output')}`", "",
           "## Reconciliation", "",
           "| Bronze rows | Source rows in Silver (all outcomes) | Reconciled |", "|---:|---:|---|",
           f"| {_n(rec['bronze_rows'])} | {_n(rec['silver_source_rows'])} | {'yes' if rec['reconciled'] else '**NO**'} |",
           "", "## Outcomes", "", "| Outcome | Silver records | Source rows |", "|---|---:|---:|"]
    for o in ("accepted", "quarantine", "rejected"):
        v = rep["outcomes"].get(o, {"records": 0, "source_rows": 0})
        out.append(f"| {o} | {_n(v['records'])} | {_n(v['source_rows'])} |")
    out += ["", "## Rules", "",
            "Source rows = Bronze rows that broke the rule before collapsing; "
            "records = Silver records carrying it.", "",
            "| Rule | Category | Severity | Source rows | Records | Meaning |", "|---|---|---|---:|---:|---|"]
    rv, cv = rep.get("row_rule_violations", {}), rep.get("record_rule_violations", {})
    for r in rules:
        src = _n(rv[r["name"]]) if r["name"] in rv else ("—" if r.get("stage") == "hour" else "0")
        out.append(f"| `{r['name']}` | {r['category']} | {r['severity']} | {src} | "
                   f"{_n(cv.get(r['name'], 0))} | {r['description']} |")
    c, d, cp = rep["coverage"], rep["duplicates"], rep["completeness"]
    out += ["", "## Dataset-level checks", "",
            f"- **Source completeness:** expected files {cp['expected_files']}; rows per file in Bronze "
            f"{cp['files_in_bronze']}; all present: **{'yes' if cp['all_expected_files_present'] else 'NO'}**",
            f"- **Duplicate hours:** {_n(d['hours_with_several_rows'])} hours had several source rows; "
            f"{_n(d['extra_rows_collapsed'])} extra rows collapsed",
            f"- **Hourly coverage** (accepted + quarantine): {_n(c['distinct_hours'])} distinct hours from "
            f"{c['first_hour_local']} to {c['last_hour_local']} (local); {_n(c['hours_in_span'])} hours in that "
            f"span, so **{_n(c['absent_hours'])} hours absent**",
            f"- **DST:** {rep['dst']['ambiguous_hours_in_data']} ambiguous fall-back hours in the data; "
            f"UTC offsets {rep['dst']['utc_offsets']}",
            f"- **Holidays:** {rep['holidays']['dates']} dates, {_n(rep['holidays']['hours'])} hours marked"]
    return "\n".join(out) + "\n"


def main(argv: list[str]) -> int:
    rep = json.loads(open(argv[1], encoding="utf-8").read())
    rules = json.loads(open(argv[2], encoding="utf-8").read())
    sys.stdout.write(render(rep, rules))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
