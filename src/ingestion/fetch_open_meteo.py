"""T01 API ingestion: Open-Meteo Historical Weather API (ERA5 reanalysis), one call per year.

Each raw API response is saved byte-for-byte as JSON with its request URL and SHA-256.
This is REANALYSIS data on a ~0.25° grid, not a weather-station observation (spec §4).
The location is MnDOT ATR station 301 (data/manifests/atr301_location.json, produced by
src.ingestion.locate_atr301) unless latitude/longitude are set in configs/sources.yaml.
Run: python -m src.ingestion.fetch_open_meteo [--force]
"""
from __future__ import annotations

import datetime as dt
import json
import sys
import time
from urllib.parse import urlencode

import yaml

from src.common.config import PROJECT_ROOT, load_config
from src.ingestion.common import (FileRecord, Manifest, State, base_argparser, http_session,
                                  landing_dir, log, setup_logging, sha256_bytes, sha256_file, utc_now)

SOURCE = "open_meteo"


def year_chunks(start: str, end: str) -> list[tuple[str, str]]:
    """Split [start, end] (inclusive ISO dates) into calendar-year pieces."""
    s, e = dt.date.fromisoformat(start), dt.date.fromisoformat(end)
    if s > e:
        raise ValueError("start_date after end_date")
    out = []
    while s <= e:
        y_end = min(dt.date(s.year, 12, 31), e)
        out.append((s.isoformat(), y_end.isoformat()))
        s = y_end + dt.timedelta(days=1)
    return out


def resolve_location(src: dict) -> tuple[float, float, str]:
    if src.get("latitude") is not None and src.get("longitude") is not None:
        return float(src["latitude"]), float(src["longitude"]), "configs/sources.yaml"
    loc_file = PROJECT_ROOT / "data" / "manifests" / "atr301_location.json"
    if not loc_file.exists():
        raise SystemExit(f"no location: run `python -m src.ingestion.locate_atr301` first ({loc_file} missing)")
    chosen = json.loads(loc_file.read_text())["chosen"]
    if not chosen:
        raise SystemExit(f"{loc_file} has no unique ATR 301 match; set latitude/longitude in configs/sources.yaml")
    return float(chosen["latitude"]), float(chosen["longitude"]), str(loc_file.relative_to(PROJECT_ROOT))


def main() -> int:
    setup_logging()
    args = base_argparser(__doc__).parse_args()
    cfg = load_config()
    src = yaml.safe_load((PROJECT_ROOT / "configs" / "sources.yaml").read_text())[SOURCE]
    lat, lon, loc_origin = resolve_location(src)
    out = landing_dir(cfg, SOURCE, args.ingest_date)
    state = State(cfg, SOURCE)
    s = http_session()
    man = Manifest(SOURCE, args.ingest_date, utc_now(), params={
        "endpoint": src["endpoint"], "latitude": lat, "longitude": lon, "location_from": loc_origin,
        "model": src["model"], "timezone": src["timezone"], "hourly": src["hourly"],
        "start_date": src["start_date"], "end_date": src["end_date"]})

    for i, (a, b) in enumerate(year_chunks(src["start_date"], src["end_date"])):
        params = {"latitude": lat, "longitude": lon, "start_date": a, "end_date": b,
                  "hourly": ",".join(src["hourly"]), "models": src["model"], "timezone": src["timezone"]}
        url = f"{src['endpoint']}?{urlencode(params)}"
        dest = out / f"open_meteo_{a}_{b}.json"
        prev = state.get(url)
        if prev and not args.force and dest.exists() and sha256_file(dest) == prev["sha256"]:
            log.info("unchanged request, not re-called: %s", url)
            man.files.append(FileRecord(dest.name, url, "unchanged", prev["sha256"], prev["bytes"],
                                        prev.get("source_version"), downloaded_at=prev.get("downloaded_at")))
            continue
        if i:
            time.sleep(float(src.get("pause_seconds", 0)))
        try:
            r = s.get(url, timeout=120)
            r.raise_for_status()
            body = r.content
            payload = json.loads(body)
            if "hourly" not in payload or "time" not in payload["hourly"]:
                raise ValueError(f"unexpected response keys: {list(payload)}")
            dest.write_bytes(body)
            digest = sha256_bytes(body)
            status = "downloaded" if not prev else (
                "redownloaded_identical" if prev["sha256"] == digest else "redownloaded_changed")
            version = f"open-meteo archive {src['model']} retrieved:{utc_now()[:10]}"
            rec = FileRecord(dest.name, url, status, digest, len(body), version, downloaded_at=utc_now(),
                             extra={"hours": len(payload["hourly"]["time"]),
                                    "grid_latitude": payload.get("latitude"),
                                    "grid_longitude": payload.get("longitude"),
                                    "elevation": payload.get("elevation"),
                                    "hourly_units": payload.get("hourly_units")})
            state.put(rec, dest)
            state.data[url]["source_version"] = version
        except Exception as exc:
            log.error("failed %s: %s", url, exc)
            rec = FileRecord(dest.name, url, "failed", error=f"{type(exc).__name__}: {exc}")
        man.files.append(rec)

    state.save()
    print(man.write(out))
    return 1 if man.failed else 0


if __name__ == "__main__":
    sys.exit(main())
