"""NYC TLC Yellow trip quality rules and schema unification (spec T03, T05). Pure Python.

Rules run on unified Silver columns (see configs/quality.yaml nyc_tlc.columns) plus:
  passenger_count_src, ratecode_id_src (double, value before the int cast)
  trip_minutes (double), pickup_month (string 'YYYY-MM'), source_month (string 'YYYY-MM')
  pu_borough, do_borough (string from the zone lookup; NULL if the ID is not in it)
  dup_count (int, rows sharing the duplicate key)
"""
from __future__ import annotations

from typing import Any, Mapping, Sequence

from src.quality.rules import Rule, validate_rules


def _in(col: str, values: Sequence[Any]) -> str:
    vals = ", ".join(f"'{v}'" if isinstance(v, str) else str(int(v)) for v in values)
    return f"{col} IN ({vals})"


def unify_plan(columns: Sequence[tuple[str, str]], cfg: Mapping[str, Any]) -> dict[str, Any]:
    """Map one month's Bronze schema onto the Silver schema.

    columns: [(bronze_name, spark_type_string)] of that month.
    Returns {"mapping": [(bronze_name, silver_name, from_type, to_type)], "missing": [...], "extra": [...]}.
    Renames (e.g. Airport_fee -> airport_fee) are applied before matching.
    """
    target = {k: tuple(v) for k, v in cfg["columns"].items()}
    renames = dict(cfg.get("renames") or {})
    mapping, extra = [], []
    seen = set()
    for name, typ in columns:
        canon = renames.get(name, name)
        if canon in target:
            silver, to_type = target[canon]
            mapping.append((name, silver, typ, to_type))
            seen.add(canon)
        elif not name.startswith("_") and name not in ("ingest_date", "source_month"):
            extra.append(name)
    return {"mapping": mapping, "missing": sorted(set(target) - seen), "extra": sorted(extra)}


def row_rules(cfg: Mapping[str, Any]) -> list[Rule]:
    r = cfg["ranges"]
    flex = int(cfg["flex_fare_payment_type"])
    any_null = " OR ".join(f"{c} IS NULL" for c in cfg["flex_fare_null_fields"])
    unknown_zone = ", ".join(str(int(z)) for z in cfg["zone_unknown_ids"])
    return validate_rules([
        Rule("key_missing", "business_key", "reject",
             "pickup_datetime_local IS NULL OR dropoff_datetime_local IS NULL "
             "OR pu_location_id IS NULL OR do_location_id IS NULL",
             "pickup/drop-off time or zone missing"),
        Rule("zone_not_in_lookup", "business_key", "quarantine",
             "pu_borough IS NULL OR do_borough IS NULL",
             "pickup or drop-off LocationID not in the TLC taxi zone lookup"),
        Rule("zone_unknown_or_outside", "business_key", "flag",
             f"pu_location_id IN ({unknown_zone}) OR do_location_id IN ({unknown_zone})",
             "zone 264 (Unknown) or 265 (Outside of NYC)"),
        Rule("ts_dropoff_before_pickup", "timestamp", "reject",
             "dropoff_datetime_local < pickup_datetime_local", "drop-off earlier than pickup"),
        Rule("ts_pickup_outside_file_month", "timestamp", "quarantine",
             "pickup_month <> source_month", "pickup not in the month of the file it was published in"),
        Rule("ts_zero_duration", "timestamp", "quarantine",
             "trip_minutes = 0", "pickup and drop-off at the same second"),
        Rule("range_duration_over_max", "range", "quarantine",
             f"trip_minutes > {r['trip_minutes_max']}", f"trip longer than {r['trip_minutes_max']} minutes"),
        Rule("type_non_integer_code", "type", "quarantine",
             "passenger_count_src <> floor(passenger_count_src) OR ratecode_id_src <> floor(ratecode_id_src)",
             "passenger_count or RatecodeID is not a whole number before the int cast"),
        Rule("range_distance_negative", "range", "reject", "trip_distance_mi < 0", "negative trip distance"),
        Rule("range_distance_over_max", "range", "quarantine",
             f"trip_distance_mi > {r['trip_distance_mi_max']}",
             f"trip distance above {r['trip_distance_mi_max']} miles"),
        Rule("flag_distance_zero", "range", "flag", "trip_distance_mi = 0", "trip distance 0 miles"),
        Rule("range_amount_negative", "range", "quarantine",
             "fare_amount_usd < 0 OR total_amount_usd < 0", "negative fare or total (refund/dispute)"),
        Rule("range_total_over_max", "range", "quarantine",
             f"total_amount_usd > {r['total_amount_usd_max']}",
             f"total amount above {r['total_amount_usd_max']} USD"),
        Rule("flag_passenger_count_zero", "range", "flag", "passenger_count = 0",
             "driver-reported passenger count is 0"),
        Rule("range_vendor_unknown", "range", "quarantine",
             f"vendor_id IS NULL OR NOT {_in('vendor_id', cfg['vendor_ids'])}",
             "VendorID not in the data dictionary"),
        Rule("range_ratecode_unknown", "range", "quarantine",
             f"ratecode_id IS NOT NULL AND NOT {_in('ratecode_id', cfg['ratecode_ids'])}",
             "RatecodeID not in the data dictionary"),
        Rule("range_payment_type_unknown", "range", "quarantine",
             f"payment_type IS NULL OR NOT {_in('payment_type', cfg['payment_types'])}",
             "payment_type not in the data dictionary"),
        Rule("range_store_fwd_unknown", "range", "quarantine",
             f"store_and_fwd_flag IS NOT NULL AND NOT {_in('store_and_fwd_flag', cfg['store_and_fwd_flags'])}",
             "store_and_fwd_flag not Y/N"),
        Rule("missing_fields_flex_fare", "missing", "flag",
             f"payment_type = {flex} AND ({any_null})",
             "Flex Fare trip (payment_type 0) without passenger count / rate code / surcharges"),
        Rule("missing_fields_non_flex", "missing", "quarantine",
             f"payment_type <> {flex} AND ({any_null})",
             "passenger count / rate code / surcharges missing on a non-Flex-Fare trip"),
        Rule("unique_trip_duplicate", "uniqueness", "quarantine",
             "dup_count > 1", "several rows share the duplicate key (all copies quarantined)"),
    ])


def catalogue(cfg: Mapping[str, Any]) -> list[dict[str, str]]:
    return [{"stage": "row", **r.__dict__} for r in row_rules(cfg)]


if __name__ == "__main__":   # python -m src.quality.nyc_tlc_rules -> rule catalogue as JSON
    import json
    import sys

    import yaml

    from src.common.config import PROJECT_ROOT
    cfg = yaml.safe_load((PROJECT_ROOT / "configs" / "quality.yaml").read_text())["nyc_tlc"]
    json.dump(catalogue(cfg), sys.stdout, indent=2)
    sys.stdout.write("\n")
