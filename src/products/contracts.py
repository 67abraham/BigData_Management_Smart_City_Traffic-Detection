"""Data-product contracts (spec T07, §7): load, validate structure, and update the release block. Pure.

A contract is contracts/<name>_v<k>.yaml. The hand-written part defines the product; the `release`
block is written by write_release() from a recorded build + read-back run, never by hand.
"""
from __future__ import annotations

import datetime as dt
from pathlib import Path
from typing import Any, Mapping

import yaml

ROOT = Path(__file__).resolve().parents[2]
CONTRACTS = ROOT / "contracts"
PRODUCTS = ("traffic_hourly", "traffic_features", "road_image_manifest", "traffic_window_metrics")

# spec §7: name/version, schema, data source and licence, temporal/spatial coverage, units, quality summary,
# row count/size, file/table location, reproducible production method, reading instructions, known limitations.
# spec T07: fields, types, units, granularity, keys/uniqueness, time semantics, origin, version, quality rules,
# access method.
REQUIRED = ("name", "version", "release_level", "description", "location", "granularity", "primary_key",
            "time_semantics", "spatial_coverage", "temporal_coverage", "sources", "licence", "lineage",
            "fields", "quality_rules", "production", "reading_instructions", "known_limitations")
RELEASE_REQUIRED = ("run_id", "built_at", "row_count", "size_bytes", "files", "location", "quality_summary",
                    "readback")
FIELD_TYPES = ("string", "int", "bigint", "double", "boolean", "date", "timestamp", "timestamp_ntz",
               "array<string>")


def path(name: str, version: str = "v1") -> Path:
    return CONTRACTS / f"{name}_{version}.yaml"


def load(name: str, version: str = "v1") -> dict[str, Any]:
    return yaml.safe_load(path(name, version).read_text(encoding="utf-8"))


def validate(c: Mapping[str, Any]) -> list[str]:
    """Problems with a contract's structure; empty list = valid."""
    errs = [f"missing key: {k}" for k in REQUIRED if k not in c or c[k] in (None, "", [])]
    names = [f.get("name") for f in c.get("fields", [])]
    if len(names) != len(set(names)):
        errs.append("duplicate field names")
    for f in c.get("fields", []):
        if f.get("type") not in FIELD_TYPES:
            errs.append(f"field {f.get('name')}: unknown type {f.get('type')!r}")
        if "nullable" not in f or not f.get("description"):
            errs.append(f"field {f.get('name')}: needs nullable and description")
    for k in c.get("primary_key", []):
        if k not in names:
            errs.append(f"primary key column {k} is not a field")
        elif any(f["name"] == k and f["nullable"] for f in c["fields"]):
            errs.append(f"primary key column {k} must not be nullable")
    for p in (c.get("location") or {}).get("partitioning", []) or []:
        if p not in names:
            errs.append(f"partition column {p} is not a field")
    rules = [r.get("name") for r in c.get("quality_rules", [])]
    if len(rules) != len(set(rules)) or any(not r.get("rule") for r in c.get("quality_rules", [])):
        errs.append("quality_rules need unique names and a rule")
    for s in c.get("sources", []):
        if not s.get("licence") or not s.get("url"):
            errs.append(f"source {s.get('name')}: needs url and licence")
    if "release" in c:
        errs += [f"release missing: {k}" for k in RELEASE_REQUIRED if k not in c["release"]]
    return errs


def field_names(c: Mapping[str, Any]) -> list[str]:
    return [f["name"] for f in c["fields"]]


def write_release(name: str, release: Mapping[str, Any], version: str = "v1") -> Path:
    """Replace the generated `release:` block at the end of the contract file, keeping the hand-written part."""
    p = path(name, version)
    text = p.read_text(encoding="utf-8")
    marker = "\n# ---- generated release block"
    head = text.split(marker)[0].rstrip("\n")
    block = yaml.safe_dump({"release": dict(release)}, sort_keys=False, allow_unicode=True, width=110)
    stamp = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    p.write_text(f"{head}\n{marker.strip()} (src/products/release.py, {stamp}; do not edit by hand) ----\n"
                 f"{block}", encoding="utf-8")
    return p
