"""Unit tests (no Spark): Open-Meteo rules, dataset-level checks, UCI <-> ERA5 time alignment."""
import json
from pathlib import Path

import pytest
import yaml

from src.quality.alignment import offset_hours, offsets_by_variable
from src.quality.open_meteo_rules import (catalogue, key_rules, request_window, row_rules, station_in_cell,
                                          units_check)
from src.quality.rules import CATEGORIES

ROOT = Path(__file__).resolve().parents[2]
Q = yaml.safe_load((ROOT / "configs" / "quality.yaml").read_text())
CFG = Q["open_meteo"]
SOURCES = yaml.safe_load((ROOT / "configs" / "sources.yaml").read_text())


# ---------- alignment ----------
@pytest.mark.parametrize("valid_time, label, expected", [
    ("instant", "start", 0),               # value at the hour start U
    ("preceding_hour_sum", "start", 1),    # sum at U+1h covers [U, U+1h)
    ("instant", "end", -1),
    ("preceding_hour_sum", "end", 0),
])
def test_offset_hours(valid_time, label, expected):
    assert offset_hours(valid_time, label) == expected


def test_offset_hours_rejects_unknown_values():
    with pytest.raises(ValueError):
        offset_hours("mean", "start")
    with pytest.raises(ValueError):
        offset_hours("instant", "middle")


def test_offsets_by_variable_from_config():
    offs = offsets_by_variable(CFG["valid_time"], Q["traffic_weather"]["uci_hour_label"])
    assert offs == {0: ["cloud_cover", "temperature_2m", "weather_code", "wind_speed_10m"],
                    1: ["precipitation", "rain", "snowfall"]}


# ---------- rules ----------
def test_request_window_from_sources_yaml():
    assert request_window(SOURCES) == ("2012-10-01 00:00:00", "2018-10-01 23:00:00")


def test_open_meteo_rules_cover_categories_and_minimum():
    rules = row_rules(CFG, *request_window(SOURCES)) + key_rules(CFG)
    assert {r.category for r in rules} == set(CATEGORIES) - {"completeness"}   # completeness = dataset checks
    assert len(rules) >= 6
    assert len(catalogue(CFG, *request_window(SOURCES))) == len(rules)


def test_wmo_codes_match_open_meteo_docs():
    # the 29 codes of the docs' WMO table, including 97 (heavy thunderstorm)
    assert len(CFG["wmo_codes"]) == 29 and 97 in CFG["wmo_codes"] and 42 not in CFG["wmo_codes"]


# ---------- dataset-level checks ----------
def _meta(**over):
    m = {"file": "f.json", "hourly_units": {"time": "iso8601", **CFG["expected_units"]},
         "timezone": "GMT", "utc_offset_seconds": 0}
    m.update(over)
    return m


def test_units_check_passes_on_expected_units():
    assert units_check([_meta(), _meta(file="g.json")], CFG)["ok"] is True


def test_units_check_fails_on_other_unit_or_timezone():
    bad_unit = _meta(file="bad.json", hourly_units={**CFG["expected_units"], "snowfall": "mm"})
    bad_tz = _meta(file="tz.json", timezone="America/Chicago", utc_offset_seconds=-21600)
    r = units_check([_meta(), bad_unit, bad_tz], CFG)
    assert r["ok"] is False and r["files_not_matching"] == ["bad.json", "tz.json"]


def test_units_check_fails_on_no_files():
    assert units_check([], CFG)["ok"] is False


def test_station_inside_recorded_era5_cell():
    # coordinates as recorded in M1 (data/manifests/atr301_location.json, MnDOT)
    loc = json.loads((ROOT / "data" / "manifests" / "atr301_location.json").read_text())["chosen"]
    r = station_in_cell((45.0, -93.25), loc, CFG["grid_resolution_deg"])
    assert r["station_inside_cell"] is True
    assert r["distance_km"] == pytest.approx(10.4, abs=0.1)


def test_station_outside_cell():
    r = station_in_cell((45.0, -93.25), {"latitude": 45.2, "longitude": -93.25}, 0.25)
    assert r["station_inside_cell"] is False
