"""Unit tests (no Spark): NYC TLC schema unification and rule set."""
import json
from pathlib import Path

import pytest
import yaml

from src.quality.nyc_tlc_rules import catalogue, row_rules, unify_plan
from src.quality.rules import CATEGORIES

ROOT = Path(__file__).resolve().parents[2]
CFG = yaml.safe_load((ROOT / "configs" / "quality.yaml").read_text())["nyc_tlc"]
# Per-month Bronze schemas as recorded by M1 (run m1_20261121T070826Z)
PROFILE = json.loads((ROOT / "evidence" / "AC01" / "m1_20261121T070826Z" / "profile_nyc_tlc.json").read_text())


def month_cols(m):
    return [tuple(c) for c in PROFILE["months"][m]["columns"]]


@pytest.mark.parametrize("month", ["2023-01", "2023-02", "2023-03"])
def test_every_recorded_month_maps_onto_silver_schema(month):
    plan = unify_plan(month_cols(month), CFG)
    assert plan["missing"] == [] and plan["extra"] == []
    assert len(plan["mapping"]) == len(CFG["columns"]) == 19


def test_airport_fee_spelling_and_types_are_unified():
    jan = {b: (s, f, t) for b, s, f, t in unify_plan(month_cols("2023-01"), CFG)["mapping"]}
    feb = {b: (s, f, t) for b, s, f, t in unify_plan(month_cols("2023-02"), CFG)["mapping"]}
    assert jan["airport_fee"][0] == feb["Airport_fee"][0] == "airport_fee_usd"
    assert jan["VendorID"][1:] == ("bigint", "int") and feb["VendorID"][1:] == ("int", "int")
    assert jan["passenger_count"][1:] == ("double", "int") and feb["passenger_count"][1:] == ("bigint", "int")


def test_unify_plan_reports_missing_and_extra_columns():
    cols = [c for c in month_cols("2023-01") if c[0] != "tip_amount"] + [("cbd_congestion_fee", "double")]
    plan = unify_plan(cols, CFG)
    assert plan["missing"] == ["tip_amount"] and plan["extra"] == ["cbd_congestion_fee"]


def test_provenance_and_partition_columns_are_not_extra():
    cols = month_cols("2023-01") + [("_sha256", "string"), ("ingest_date", "date"), ("source_month", "string")]
    assert unify_plan(cols, CFG)["extra"] == []


def test_code_lists_match_tlc_dictionary_2025_03_18():
    assert CFG["vendor_ids"] == [1, 2, 6, 7]
    assert CFG["ratecode_ids"] == [1, 2, 3, 4, 5, 6, 99]
    assert CFG["payment_types"] == [0, 1, 2, 3, 4, 5, 6]
    assert CFG["store_and_fwd_flags"] == ["Y", "N"]


def test_nyc_rules_cover_categories():
    rules = row_rules(CFG)
    assert {r.category for r in rules} == set(CATEGORIES) - {"completeness"}   # completeness = job checks
    assert len(catalogue(CFG)) == len(rules) >= 6


def test_flex_fare_rules_split_on_payment_type():
    by = {r.name: r for r in row_rules(CFG)}
    assert "payment_type = 0" in by["missing_fields_flex_fare"].violation_expr
    assert "payment_type <> 0" in by["missing_fields_non_flex"].violation_expr
    assert by["missing_fields_flex_fare"].severity == "flag"
    assert by["missing_fields_non_flex"].severity == "quarantine"
