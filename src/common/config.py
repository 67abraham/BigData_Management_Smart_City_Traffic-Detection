"""Load configs/platform.yaml with environment-variable overrides.

Override rule: BDFP_<SECTION>_<KEY> (upper-case) replaces config[section][key],
e.g. BDFP_HDFS_URI=hdfs://other:8020. Values from the environment are strings.
"""
from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Mapping

import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_CONFIG = PROJECT_ROOT / "configs" / "platform.yaml"
ENV_PREFIX = "BDFP_"


def load_config(
    path: str | Path | None = None,
    env: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    """Return the platform config dict, with BDFP_* environment overrides applied."""
    cfg_path = Path(path or os.environ.get("BDFP_CONFIG", DEFAULT_CONFIG))
    with cfg_path.open(encoding="utf-8") as fh:
        cfg: dict[str, Any] = yaml.safe_load(fh) or {}

    env = os.environ if env is None else env
    for section, values in cfg.items():
        if not isinstance(values, dict):
            continue
        for key in values:
            var = f"{ENV_PREFIX}{section}_{key}".upper()
            if var in env:
                values[key] = env[var]
    return cfg


def hdfs_layer_path(cfg: Mapping[str, Any], layer: str, source: str, ingest_date: str) -> str:
    """Build /datalake/<layer>/<source>/ingest_date=YYYY-MM-DD (CLAUDE.md conventions)."""
    hdfs = cfg["hdfs"]
    if layer not in hdfs["layers"]:
        raise ValueError(f"unknown layer {layer!r}; expected one of {hdfs['layers']}")
    root = str(hdfs["datalake_root"]).rstrip("/")
    return f"{root}/{layer}/{source}/ingest_date={ingest_date}"
