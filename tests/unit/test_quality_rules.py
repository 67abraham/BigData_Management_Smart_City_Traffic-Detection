"""Unit tests for the pure quality logic (no Spark): rule model, outcomes, DST table, UCI rule set."""
from pathlib import Path

import pytest
import yaml

from src.quality.rules import CATEGORIES, Rule, dst_anomalies, outcome, validate_rules
from src.quality.uci_rules import hour_rules, row_rules

CFG = yaml.safe_load((Path(__file__).resolve().parents[2] / "configs" / "quality.yaml").read_text())["uci_metro"]


# ---------- outcome ----------
@pytest.mark.parametrize("sev, expected", [
    ([], "accepted"),
    (["flag"], "accepted"),
    (["flag", "quarantine"], "quarantine"),
    (["quarantine", "reject", "flag"], "rejected"),
])
def test_outcome_worst_severity_wins(sev, expected):
    assert outcome(sev) == expected


def test_outcome_rejects_unknown_severity():
    with pytest.raises(ValueError):
        outcome(["fatal"])


# ---------- rule model ----------
def test_rule_validates_category_severity_and_name():
    with pytest.raises(ValueError):
        Rule("x", "spelling", "reject", "a IS NULL", "")
    with pytest.raises(ValueError):
        Rule("x", "type", "drop", "a IS NULL", "")
    with pytest.raises(ValueError):
        Rule("not an identifier", "type", "reject", "a IS NULL", "")


def test_validate_rules_rejects_duplicate_names():
    r = Rule("same", "type", "reject", "a IS NULL", "")
    with pytest.raises(ValueError, match="duplicate"):
        validate_rules([r, r])


# ---------- DST table (America/Chicago) ----------
def test_dst_anomalies_chicago_2016():
    nonexistent, ambiguous = dst_anomalies("America/Chicago", 2016, 2016)
    assert nonexistent == ["2016-03-13 02:00:00"]        # 2nd Sunday of March
    assert ambiguous == ["2016-11-06 01:00:00"]          # 1st Sunday of November


def test_dst_anomalies_one_pair_per_year():
    nonexistent, ambiguous = dst_anomalies("America/Chicago", 2012, 2018)
    assert len(nonexistent) == len(ambiguous) == 7


def test_dst_anomalies_none_for_utc():
    assert dst_anomalies("UTC", 2016, 2016) == ([], [])


# ---------- UCI rule set ----------
def test_uci_rules_cover_all_spec_categories():
    # spec T05 asks for type, timestamp, business key, missing, range, uniqueness, completeness.
    # completeness is a dataset-level check in the job report, not a per-record rule.
    cats = {r.category for r in row_rules(CFG) + hour_rules(CFG)}
    assert cats == set(CATEGORIES) - {"completeness"}


def test_uci_rules_at_least_six():
    assert len(row_rules(CFG) + hour_rules(CFG)) >= 6            # spec T05 minimum


def test_uci_range_rules_use_config_thresholds():
    cfg = {**CFG, "ranges": {**CFG["ranges"], "traffic_volume_max": 1234, "rain_1h_mm_max": 9.5}}
    by_name = {r.name: r for r in row_rules(cfg)}
    assert "1234" in by_name["range_volume_high"].violation_expr
    assert "9.5" in by_name["range_rain_1h"].violation_expr


def test_uci_rule_names_unique_across_stages():
    validate_rules(row_rules(CFG) + hour_rules(CFG))


# ---------- Markdown report ----------
def _report(reconciled: bool) -> dict:
    return {
        "dataset": "uci_traffic_hourly", "version": "v1", "bronze_ingest_date": "2026-11-21",
        "reconciliation": {"bronze_rows": 10, "silver_source_rows": 10 if reconciled else 9,
                           "reconciled": reconciled},
        "outcomes": {"accepted": {"records": 7, "source_rows": 8}, "rejected": {"records": 2, "source_rows": 2}},
        "row_rule_violations": {"type_traffic_volume": 2},
        "record_rule_violations": {"type_traffic_volume": 2},
        "duplicates": {"hours_with_several_rows": 1, "extra_rows_collapsed": 1},
        "coverage": {"first_hour_local": "a", "last_hour_local": "b", "distinct_hours": 7,
                     "hours_in_span": 9, "absent_hours": 2},
        "completeness": {"expected_files": ["f"], "files_in_bronze": {"f": 10}, "all_expected_files_present": True},
        "dst": {"ambiguous_hours_in_data": 0, "utc_offsets": {"-5": 7}},
        "holidays": {"dates": 0, "hours": 0},
    }


def test_report_md_lists_every_rule_and_missing_outcome_as_zero():
    from src.quality.report_md import render
    from src.quality.uci_rules import catalogue
    md = render(_report(True), catalogue(CFG))
    for r in catalogue(CFG):
        assert f"`{r['name']}`" in md
    assert "| quarantine | 0 | 0 |" in md
    assert "**2 hours absent**" in md


def test_report_md_shows_failed_reconciliation():
    from src.quality.report_md import render
    assert "**NO**" in render(_report(False), [])
