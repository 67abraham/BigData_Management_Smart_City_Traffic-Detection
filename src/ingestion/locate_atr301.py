"""Look up MnDOT ATR station 301 in MnDOT's official CONTINUOUS_COLLECTION_SITES layer.

UCI documents the traffic data only as "I-94 westbound, MN DoT ATR station 301, roughly
midway between Minneapolis and St Paul" — no coordinates. The Open-Meteo request must use a
defensible location (spec §4), so we take it from MnDOT's own GIS layer and save the raw answer
with its source URL.

Two routes to the same official layer (same ArcGIS item, published by MnDOT_GIS):
  1. MnDOT's ArcGIS FeatureServer (webgis.dot.state.mn.us). It timed out both from outside the US
     and from the GitHub Codespace (run m1_20261121T060408Z), so it is tried first with a short
     connect timeout.
  2. ArcGIS Hub's export of that item (hub.arcgis.com, as linked from gis.data.mn.gov).
Candidates are features whose `id_field` (DOT_ID = the ATR station number) equals the station id.
Matching on one named field matters: OBJECTID 301 is a different, unrelated site.

    python -m src.ingestion.locate_atr301
Writes data/manifests/atr301_location.json (committed; it is official public metadata).
Exit code 2 = no unique match; the JSON then lists all candidates for a human decision.
"""
from __future__ import annotations

import csv
import io
import json
import re
import sys
from typing import Any

import yaml

from src.common.config import PROJECT_ROOT
from src.ingestion.common import http_session, setup_logging, sha256_bytes, utc_now, log


def _matches(attrs: dict[str, Any], station: str, field: str | None = None) -> bool:
    """True if `field` (or, when None, any attribute) holds the station id, allowing leading zeros."""
    pat = re.compile(rf"(?<!\d)0*{re.escape(station)}(?!\d)")
    values = [attrs.get(field)] if field else list(attrs.values())
    return any(v is not None and pat.fullmatch(str(v).strip()) for v in values)


def _mentions_i94(attrs: dict[str, Any]) -> bool:
    return any(v is not None and re.search(r"\bI[- ]?94\b", str(v), re.I) for v in attrs.values())


def _features_from_featureserver(s, base: str) -> tuple[list[dict[str, Any]], list[str]]:
    service = s.get(base, params={"f": "json"}, timeout=(15, 120)).json()
    layers = service.get("layers", [])
    log.info("layers: %s", [(l["id"], l["name"]) for l in layers])
    feats_out: list[dict[str, Any]] = []
    queried: list[str] = []
    for layer in layers:
        url = f"{base}/{layer['id']}/query"
        offset = 0
        while True:
            params = {"where": "1=1", "outFields": "*", "returnGeometry": "true", "outSR": "4326",
                      "f": "json", "resultOffset": offset, "resultRecordCount": 1000}
            page = s.get(url, params=params, timeout=(15, 120)).json()
            if "error" in page:
                raise RuntimeError(f"layer {layer['id']}: {page['error']}")
            feats = page.get("features", [])
            for f in feats:
                feats_out.append({"layer_id": layer["id"], "layer_name": layer["name"],
                                  "attributes": f.get("attributes", {}), "geometry": f.get("geometry")})
            queried.append(f"{url} (offset {offset}, {len(feats)} features)")
            if not page.get("exceededTransferLimit") or not feats:
                break
            offset += len(feats)
    return feats_out, queried


def _features_from_hub_csv(text: str, layer_id: int) -> list[dict[str, Any]]:
    """Parse ArcGIS Hub's CSV export (X/Y = lon/lat in EPSG:4326, then the layer's attributes)."""
    rows = csv.DictReader(io.StringIO(text.lstrip("﻿")))
    out = []
    for r in rows:
        x, y = r.pop("X", None), r.pop("Y", None)
        geom = {"x": float(x), "y": float(y)} if x not in (None, "") and y not in (None, "") else None
        out.append({"layer_id": layer_id, "layer_name": "hub_csv_export", "attributes": r, "geometry": geom})
    return out


def main() -> int:
    setup_logging()
    src_cfg = yaml.safe_load((PROJECT_ROOT / "configs" / "sources.yaml").read_text())["atr301_location"]
    base = src_cfg["feature_service"]
    station = str(src_cfg["station_id"])
    id_field = src_cfg["id_field"]
    s = http_session()

    route: dict[str, Any]
    try:
        features, queried = _features_from_featureserver(s, base)
        route = {"method": "mndot_featureserver", "url": base}
    except Exception as e:  # noqa: BLE001 — any failure of route 1 falls back to route 2, and is recorded
        log.warning("MnDOT FeatureServer failed (%s: %s); falling back to ArcGIS Hub export",
                    type(e).__name__, str(e)[:200])
        hub_url = src_cfg["hub_csv_url"]
        resp = s.get(hub_url, timeout=(15, 180))
        resp.raise_for_status()
        features = _features_from_hub_csv(resp.content.decode("utf-8-sig"), int(src_cfg["hub_layer_id"]))
        queried = [f"{hub_url} ({len(features)} features)"]
        route = {"method": "arcgis_hub_csv_export", "url": hub_url,
                 "featureserver_error": f"{type(e).__name__}: {str(e)[:300]}",
                 "bytes": len(resp.content), "sha256": sha256_bytes(resp.content)}

    candidates = [dict(f, mentions_i94=_mentions_i94(f["attributes"]))
                  for f in features if _matches(f["attributes"], station, id_field)]
    i94 = [c for c in candidates if c["mentions_i94"]]
    chosen = i94[0] if len(i94) == 1 else (candidates[0] if len(candidates) == 1 else None)
    out: dict[str, Any] = {
        "station_id": station,
        "id_field": id_field,
        "retrieved_at": utc_now(),
        "source": base,
        "arcgis_item": f"https://www.arcgis.com/home/item.html?id={src_cfg['arcgis_item_id']}",
        "route": route,
        "features_scanned": len(features),
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
    log.info("route=%s, %d feature(s) scanned, %d candidate(s), chosen=%s -> %s",
             route["method"], len(features), len(candidates), bool(out["chosen"]), dest)
    print(json.dumps(out["chosen"] or {"candidates": len(candidates)}, indent=2, default=str))
    return 0 if out["chosen"] else 2


if __name__ == "__main__":
    sys.exit(main())
