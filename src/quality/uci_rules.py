"""UCI Metro quality rules (spec T05), built from configs/quality.yaml. Pure Python.

Row-level rules run on typed Bronze rows (one per source line). Column contract they expect:
  raw strings  : date_time, traffic_volume, temp, rain_1h, snow_1h, clouds_all, weather_main,
                 weather_description, holiday (as delivered; may be NULL or ''), _corrupt_record
  typed        : hour_start_local (timestamp_ntz), volume_num, temp_k, rain_1h_mm, snow_1h_mm,
                 clouds_all_pct (double), is_dst_nonexistent, is_dst_ambiguous (boolean)
Hour-level rules run after rows are collapsed to one per local hour:
  n_distinct_volume (int), weather_numeric_conflict (boolean)
"""
from __future__ import annotations

from typing import Any, Mapping

from src.quality.rules import Rule, validate_rules

WEATHER_NUMERIC = ("temp", "rain_1h", "snow_1h", "clouds_all")
WEATHER_TEXT = ("weather_main", "weather_description")


def _blank(col: str) -> str:
    return f"({col} IS NULL OR trim({col}) = '')"


def _present(col: str) -> str:
    return f"({col} IS NOT NULL AND trim({col}) <> '')"


def row_rules(cfg: Mapping[str, Any]) -> list[Rule]:
    r = cfg["ranges"]
    lo, hi = cfg["coverage_start_local"], cfg["coverage_end_local"]
    not_numeric = " OR ".join(f"({_present(c)} AND try_cast(trim({c}) AS DOUBLE) IS NULL)"
                              for c in WEATHER_NUMERIC)
    missing_weather = " OR ".join(_blank(c) for c in WEATHER_NUMERIC + WEATHER_TEXT)
    return validate_rules([
        Rule("type_malformed_line", "type", "reject", "_corrupt_record IS NOT NULL",
             "the CSV line could not be parsed into the 9 UCI columns (Bronze _corrupt_record)"),
        # business key: (station, local hour). The station is constant for this source.
        Rule("key_date_time_missing", "business_key", "reject", _blank("date_time"),
             "date_time (part of the business key station+hour) is missing"),
        Rule("missing_traffic_volume", "missing", "reject", _blank("traffic_volume"),
             "traffic_volume, the measured value, is missing"),
        Rule("missing_weather_field", "missing", "quarantine", missing_weather,
             "one of the UCI weather fields is missing"),
        Rule("type_traffic_volume", "type", "reject",
             f"{_present('traffic_volume')} AND NOT (trim(traffic_volume) RLIKE '^-?[0-9]+$')",
             "traffic_volume is not an integer"),
        Rule("type_weather_numeric", "type", "quarantine", not_numeric,
             "temp, rain_1h, snow_1h or clouds_all is not numeric"),
        Rule("ts_unparseable", "timestamp", "reject",
             f"{_present('date_time')} AND hour_start_local IS NULL",
             f"date_time does not match {cfg['timestamp_format']} on the hour"),
        Rule("ts_dst_nonexistent", "timestamp", "reject", "is_dst_nonexistent",
             f"local time does not exist in {cfg['timezone']} (spring-forward gap)"),
        Rule("ts_dst_ambiguous", "timestamp", "quarantine", "is_dst_ambiguous",
             f"local time occurs twice in {cfg['timezone']} (fall-back hour); UTC hour is uncertain"),
        Rule("ts_outside_coverage", "timestamp", "quarantine",
             f"hour_start_local < TIMESTAMP_NTZ '{lo}' OR hour_start_local > TIMESTAMP_NTZ '{hi}'",
             f"timestamp outside the documented source period {lo} .. {hi}"),
        Rule("range_volume_negative", "range", "reject", "volume_num < 0",
             "traffic_volume is negative"),
        Rule("range_volume_zero", "range", "quarantine", "volume_num = 0",
             "traffic_volume is 0 vehicles/hour (possible outage or closure)"),
        Rule("range_volume_high", "range", "quarantine", f"volume_num > {r['traffic_volume_max']}",
             f"traffic_volume above the sanity bound {r['traffic_volume_max']}"),
        Rule("range_temp_k", "range", "quarantine",
             f"temp_k < {r['temp_k_min']} OR temp_k > {r['temp_k_max']}",
             f"temp outside {r['temp_k_min']} .. {r['temp_k_max']} K"),
        Rule("range_rain_1h", "range", "quarantine",
             f"rain_1h_mm < 0 OR rain_1h_mm > {r['rain_1h_mm_max']}",
             f"rain_1h outside 0 .. {r['rain_1h_mm_max']} mm"),
        Rule("range_snow_1h", "range", "quarantine",
             f"snow_1h_mm < 0 OR snow_1h_mm > {r['snow_1h_mm_max']}",
             f"snow_1h outside 0 .. {r['snow_1h_mm_max']} mm"),
        Rule("range_clouds_all", "range", "quarantine",
             f"clouds_all_pct < {r['clouds_all_min']} OR clouds_all_pct > {r['clouds_all_max']}",
             f"clouds_all outside {r['clouds_all_min']} .. {r['clouds_all_max']} percent"),
    ])


def hour_rules(cfg: Mapping[str, Any]) -> list[Rule]:
    return validate_rules([
        Rule("unique_hour_conflicting_volume", "uniqueness", "quarantine", "n_distinct_volume > 1",
             "several source rows for one hour disagree on traffic_volume"),
        Rule("unique_hour_conflicting_weather", "uniqueness", "flag", "weather_numeric_conflict",
             "several source rows for one hour disagree on a numeric weather field; that field is set to NULL"),
    ])


def catalogue(cfg: Mapping[str, Any]) -> list[dict[str, str]]:
    """All UCI rules as plain dicts (for the quality report and the evidence folder)."""
    return ([{"stage": "row", **r.__dict__} for r in row_rules(cfg)]
            + [{"stage": "hour", **r.__dict__} for r in hour_rules(cfg)])


if __name__ == "__main__":   # python -m src.quality.uci_rules  -> rule catalogue as JSON
    import json
    import sys

    import yaml

    from src.common.config import PROJECT_ROOT
    cfg = yaml.safe_load((PROJECT_ROOT / "configs" / "quality.yaml").read_text())["uci_metro"]
    json.dump(catalogue(cfg), sys.stdout, indent=2)
    sys.stdout.write("\n")
