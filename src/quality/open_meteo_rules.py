"""Open-Meteo (ERA5 reanalysis) quality rules (spec T05), built from configs/quality.yaml. Pure Python.

Row-level rules run on Bronze rows. Column contract they expect:
  time (string as delivered), time_utc (timestamp; NULL if unparseable),
  grid_latitude, grid_longitude, and the variables temperature_2m, precipitation, rain, snowfall,
  cloud_cover, weather_code, wind_speed_10m (numeric, as typed by the JSON reader)
Key-level rules run after rows are grouped by (grid point, time_utc):
  source_rows (int), n_distinct_values (int)
`request_start` / `request_end` ('YYYY-MM-DD HH:MM:SS') are the requested period from sources.yaml.
"""
from __future__ import annotations

import math
from typing import Any, Mapping

from src.quality.rules import Rule, validate_rules

VARIABLES = ("temperature_2m", "precipitation", "rain", "snowfall", "cloud_cover", "weather_code",
             "wind_speed_10m")


def row_rules(cfg: Mapping[str, Any], request_start: str, request_end: str) -> list[Rule]:
    r = cfg["ranges"]
    codes = ", ".join(str(int(c)) for c in cfg["wmo_codes"])
    non_finite = " OR ".join(f"isnan(CAST({v} AS DOUBLE))" for v in VARIABLES)
    missing = " OR ".join(f"{v} IS NULL" for v in VARIABLES)
    return validate_rules([
        Rule("key_missing", "business_key", "reject",
             "time IS NULL OR trim(time) = '' OR grid_latitude IS NULL OR grid_longitude IS NULL",
             "business key (grid point, hour) incomplete"),
        Rule("ts_unparseable", "timestamp", "reject",
             "time IS NOT NULL AND trim(time) <> '' AND time_utc IS NULL",
             f"time does not match {cfg['time_format']} on the hour"),
        Rule("ts_outside_request", "timestamp", "quarantine",
             f"time_utc < TIMESTAMP '{request_start}' OR time_utc > TIMESTAMP '{request_end}'",
             f"hour outside the requested period {request_start} .. {request_end} UTC"),
        Rule("missing_variable", "missing", "quarantine", missing,
             "one of the requested hourly variables is NULL"),
        Rule("type_non_finite", "type", "quarantine", non_finite,
             "a variable is NaN"),
        Rule("type_integer_codes", "type", "quarantine",
             "weather_code <> floor(weather_code) OR cloud_cover <> floor(cloud_cover)",
             "weather_code or cloud_cover is not an integer"),
        Rule("range_temperature_2m", "range", "quarantine",
             f"temperature_2m < {r['temperature_2m_min']} OR temperature_2m > {r['temperature_2m_max']}",
             f"temperature_2m outside {r['temperature_2m_min']} .. {r['temperature_2m_max']} °C"),
        Rule("range_precipitation", "range", "quarantine",
             f"precipitation < 0 OR precipitation > {r['precipitation_mm_max']} "
             f"OR rain < 0 OR rain > {r['precipitation_mm_max']}",
             f"precipitation or rain outside 0 .. {r['precipitation_mm_max']} mm"),
        Rule("range_snowfall", "range", "quarantine",
             f"snowfall < 0 OR snowfall > {r['snowfall_cm_max']}",
             f"snowfall outside 0 .. {r['snowfall_cm_max']} cm"),
        Rule("range_cloud_cover", "range", "quarantine",
             f"cloud_cover < {r['cloud_cover_min']} OR cloud_cover > {r['cloud_cover_max']}",
             f"cloud_cover outside {r['cloud_cover_min']} .. {r['cloud_cover_max']} %"),
        Rule("range_wind_speed_10m", "range", "quarantine",
             f"wind_speed_10m < 0 OR wind_speed_10m > {r['wind_speed_10m_max']}",
             f"wind_speed_10m outside 0 .. {r['wind_speed_10m_max']} km/h"),
        Rule("range_weather_code_unknown", "range", "quarantine",
             f"weather_code IS NOT NULL AND weather_code NOT IN ({codes})",
             "weather_code is not one of the documented WMO codes"),
        Rule("range_rain_above_precipitation", "range", "quarantine",
             f"rain > precipitation + {r['rain_above_precipitation_tolerance_mm']}",
             "rain exceeds total precipitation (rain is part of precipitation)"),
    ])


def key_rules(cfg: Mapping[str, Any]) -> list[Rule]:
    return validate_rules([
        Rule("unique_key_conflicting", "uniqueness", "quarantine",
             "source_rows > 1 AND n_distinct_values > 1",
             "several rows for one (grid point, hour) disagree; values set to NULL"),
        Rule("unique_key_duplicate", "uniqueness", "flag",
             "source_rows > 1 AND n_distinct_values = 1",
             "identical rows for one (grid point, hour); collapsed to one"),
    ])


def catalogue(cfg: Mapping[str, Any], request_start: str, request_end: str) -> list[dict[str, str]]:
    return ([{"stage": "row", **r.__dict__} for r in row_rules(cfg, request_start, request_end)]
            + [{"stage": "key", **r.__dict__} for r in key_rules(cfg)])


def request_window(sources: Mapping[str, Any]) -> tuple[str, str]:
    """Requested period from sources.yaml open_meteo (end_date is inclusive, so its last hour is 23:00)."""
    s = sources["open_meteo"]
    return f"{s['start_date']} 00:00:00", f"{s['end_date']} 23:00:00"


# ---------- dataset-level checks (pure; used by src/batch/silver_open_meteo.py) ----------
def units_check(raw_meta: list[dict[str, Any]], cfg: Mapping[str, Any]) -> dict[str, Any]:
    """Every raw response must report the expected units, timezone and UTC offset."""
    exp_units = dict(cfg["expected_units"])
    bad = [m["file"] for m in raw_meta
           if {k: v for k, v in (m["hourly_units"] or {}).items() if k != "time"} != exp_units
           or m["timezone"] != cfg["expected_timezone"]
           or int(m["utc_offset_seconds"]) != int(cfg["expected_utc_offset_seconds"])]
    return {"files_checked": len(raw_meta), "files_not_matching": bad, "ok": bool(raw_meta) and not bad,
            "expected_units": exp_units, "expected_timezone": cfg["expected_timezone"]}


def station_in_cell(grid: tuple[float, float], station: Mapping[str, Any], res_deg: float) -> dict[str, Any]:
    """Is the station inside the grid cell centred on the returned grid point? Pure function."""
    glat, glon = grid
    half = res_deg / 2
    inside = abs(station["latitude"] - glat) <= half and abs(station["longitude"] - glon) <= half
    la1, lo1, la2, lo2 = map(math.radians, (station["latitude"], station["longitude"], glat, glon))
    a = math.sin((la2 - la1) / 2) ** 2 + math.cos(la1) * math.cos(la2) * math.sin((lo2 - lo1) / 2) ** 2
    return {"grid_point": [glat, glon], "cell_half_width_deg": half, "station_inside_cell": inside,
            "distance_km": round(2 * 6371.0088 * math.asin(math.sqrt(a)), 2)}



if __name__ == "__main__":   # python -m src.quality.open_meteo_rules -> rule catalogue as JSON
    import json
    import sys

    import yaml

    from src.common.config import PROJECT_ROOT
    cfg = yaml.safe_load((PROJECT_ROOT / "configs" / "quality.yaml").read_text())["open_meteo"]
    src = yaml.safe_load((PROJECT_ROOT / "configs" / "sources.yaml").read_text())
    json.dump(catalogue(cfg, *request_window(src)), sys.stdout, indent=2)
    sys.stdout.write("\n")
