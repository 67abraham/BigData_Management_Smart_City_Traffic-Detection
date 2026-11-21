"""Write each contract's `release` block from a recorded build + read-back run (spec §7). Pure Python.

Refuses to release a product whose read-back did not pass. Row count, size, files, location, coverage
and quality summary all come from the reports; nothing is typed in by hand.

Run: python3 -m src.products.release --build B.json --readback R.json --evidence evidence/AC07/<run>
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Mapping

from src.products import contracts


def release_block(built: Mapping[str, Any], rb: Mapping[str, Any], evidence: str) -> dict[str, Any]:
    if not rb.get("passed"):
        raise ValueError(f"{built['product']}: read-back did not pass; not releasing")
    q = dict(built["quality_summary"])
    return {
        "run_id": built["run_id"], "built_at": built["built_at"], "spark_app_id": built.get("spark_app_id"),
        "location": built["location"], "row_count": built["row_count"], "size_bytes": built["size_bytes"],
        "files": built["files"], "inputs": built["inputs"],
        **({"input_sha256": built["input_sha256"], "flink_job_run_id": built["flink_job_run_id"]}
           if "input_sha256" in built else {}),
        **({"csv_copy": built["csv_copy"]} if "csv_copy" in built else {}),
        "temporal_coverage": q.pop("temporal_coverage", None),
        "quality_summary": q,
        "readback": {"run_id": rb["run_id"], "passed": rb["passed"], "checked_at": rb["checked_at"],
                     "checks_passed": sum(c["passed"] for c in rb["checks"]), "checks_total": len(rb["checks"])},
        "evidence": evidence,
    }


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--build", required=True)
    ap.add_argument("--readback", required=True)
    ap.add_argument("--evidence", required=True)
    args = ap.parse_args()
    built = json.loads(Path(args.build).read_text(encoding="utf-8"))
    rb = json.loads(Path(args.readback).read_text(encoding="utf-8"))
    failed = 0
    for name in contracts.PRODUCTS:
        if name not in built or name not in rb:
            print(f"{name}: missing from reports"); failed += 1
            continue
        try:
            block = release_block(built[name], rb[name], args.evidence)
        except ValueError as e:
            print(e); failed += 1
            continue
        p = contracts.write_release(name, block)
        errs = contracts.validate(contracts.load(name))
        print(f"{name}: released -> {p.name}; rows {block['row_count']}, {block['size_bytes']} bytes"
              + (f"; CONTRACT INVALID {errs}" if errs else ""))
        failed += bool(errs)
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
