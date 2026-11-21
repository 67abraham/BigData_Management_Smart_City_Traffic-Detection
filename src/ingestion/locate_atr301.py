"""Look up MnDOT ATR station 301 in MnDOT's official CONTINUOUS_COLLECTION_SITES layer.

UCI documents the traffic data only as "I-94 westbound, MN DoT ATR station 301, roughly
midway between Minneapolis and St Paul" — no coordinates. The Open-Meteo request must use a
defensible location (spec §4), so we take it from MnDOT's own GIS layer and save the raw answer
with its source URL. The layer's field names are not assumed: every attribute of every
candidate feature is kept, and candidates are features whose attributes contain the station id.

Run in the Codespace (the MnDOT server did not respond from outside the US during development):
    python -m src.ingestion.locate_atr301
Writes data/manifests/atr301_location.json (committed; it is official public metadata).
Exit code 2 = no unique match; the JSON then lists all candidates for a human decision.
"""
from __future__ import annotations

import json
import re
import sys
from typing import Any

import yaml

from src.common.config import PROJECT_ROOT
from src.ingestion.common import http_session, setup_logging, utc_now, log


def _matches(attrs: dict[str, Any], station: str) -> bool:
    pat = re.compile(rf"(?<!\d)0*{re.escape(station)}(?!\d)")
    return any(v is not None and pat.fullmatch(str(v).strip()) for v in attrs.values())


def _mentions_i94(attrs: dict[str, Any]) -> bool:
    return any(v is not None and re.search(r"\bI[- ]?94\b", str(v), re.I) for v in attrs.values())


def main() -> int:
    setup_logging()
    src_cfg = yaml.safe_load((PROJECT_ROOT / "configs" / "sources.yaml").read_text())["atr301_location"]
    base = src_cfg["feature_service"]
    station = str(src_cfg["station_id"])
    s = http_session()

    service = s.get(base, params={"f": "json"}, timeout=60).json()
    layers = service.get("layers", [])
    log.info("layers: %s", [(l["id"], l["name"]) for l in layers])

    candidates: list[dict[str, Any]] = []
    queried: list[str] = []
    for layer in layers:
        url = f"{base}/{layer['id']}/query"
        offset = 0
        while True:
            params = {"where": "1=1", "outFields": "*", "returnGeometry": "true", "outSR": "4326",
                      "f": "json", "resultOffset": offset, "resultRecordCount": 1000}
            page = s.get(url, params=params, timeout=120).json()
            if "error" in page:
                raise RuntimeError(f"layer {layer['id']}: {page['error']}")
            feats = page.get("features", [])
            for f in feats:
                if _matches(f.get("attributes", {}), station):
                    candidates.append({"layer_id": layer["id"], "layer_name": layer["name"],
                                       "attributes": f["attributes"], "geometry": f.get("geometry"),
                                       "mentions_i94": _mentions_i94(f["attributes"])})
            queried.append(f"{url} (offset {offset}, {len(feats)} features)")
            if not page.get("exceededTransferLimit") or not feats:
                break
            offset += len(feats)

    i94 = [c for c in candidates if c["mentions_i94"]]
    chosen = i94[0] if len(i94) == 1 else (candidates[0] if len(candidates) == 1 else None)
    out: dict[str, Any] = {
        "station_id": station,
        "retrieved_at": utc_now(),
        "source": base,
        "arcgis_item": f"https://www.arcgis.com/home/item.html?id={src_cfg['arcgis_item_id']}",
        "queries": queried,
        "candidates": candidates,
        "chosen": None,
    }
    if chosen and chosen.get("geometry") and "x" in chosen["geometry"]:
        out["chosen"] = {"longitude": chosen["geometry"]["x"], "latitude": chosen["geometry"]["y"],
                         "layer_id": chosen["layer_id"], "attributes": chosen["attributes"]}

    dest = PROJECT_ROOT / src_cfg["output"]
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_text(json.dumps(out, indent=2, default=str) + "\n", encoding="utf-8")
    log.info("%d candidate(s), chosen=%s -> %s", len(candidates), bool(out["chosen"]), dest)
    print(json.dumps(out["chosen"] or {"candidates": len(candidates)}, indent=2, default=str))
    return 0 if out["chosen"] else 2


if __name__ == "__main__":
    sys.exit(main())
