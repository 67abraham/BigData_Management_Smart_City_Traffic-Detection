# Failed run, kept as evidence

The Spark job itself succeeded (its log shows `reconciled: true`, 48,204 = 48,204), but the evidence
step failed: the job printed its report as one long `REPORT_JSON` stdout line, and a Spark INFO log
line was written into the middle of it, so `quality_uci_metro.json` here is truncated/invalid JSON.
Fix: `src/batch/silver_uci.py` now writes the report and the plan to files (`--report-out`,
`--explain-out`). The re-run is `m2_20261121T080753Z` (0 failures).
