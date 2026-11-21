"""Quality rule model (spec T05). Pure Python: no PySpark import, so unit tests run anywhere.

A Rule is a named check with a category (what kind of rule, as listed in spec T05), a severity
(what happens to a record that violates it) and a Spark SQL boolean expression that is TRUE when
the record VIOLATES the rule. src/quality/spark_rules.py evaluates the expressions.
"""
from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from typing import Iterable
from zoneinfo import ZoneInfo

# spec T05: "data types, timestamps, business keys, missing values, numeric ranges, uniqueness,
# and source completeness"
CATEGORIES = ("type", "timestamp", "business_key", "missing", "range", "uniqueness", "completeness")
SEVERITIES = ("reject", "quarantine", "flag")          # worst first
OUTCOMES = ("accepted", "quarantine", "rejected")


@dataclass(frozen=True)
class Rule:
    name: str
    category: str
    severity: str
    violation_expr: str   # Spark SQL, TRUE when the record violates the rule (NULL counts as not violated)
    description: str

    def __post_init__(self) -> None:
        if self.category not in CATEGORIES:
            raise ValueError(f"rule {self.name}: unknown category {self.category!r}")
        if self.severity not in SEVERITIES:
            raise ValueError(f"rule {self.name}: unknown severity {self.severity!r}")
        if not self.name.isidentifier():
            raise ValueError(f"rule name must be an identifier: {self.name!r}")


def validate_rules(rules: Iterable[Rule]) -> list[Rule]:
    rules = list(rules)
    names = [r.name for r in rules]
    dupes = sorted({n for n in names if names.count(n) > 1})
    if dupes:
        raise ValueError(f"duplicate rule names: {dupes}")
    return rules


def outcome(severities: Iterable[str]) -> str:
    """Outcome for a record from the severities of the rules it violates (worst wins)."""
    s = set(severities)
    unknown = s - set(SEVERITIES)
    if unknown:
        raise ValueError(f"unknown severities: {sorted(unknown)}")
    if "reject" in s:
        return "rejected"
    if "quarantine" in s:
        return "quarantine"
    return "accepted"


def dst_anomalies(tz_name: str, first_year: int, last_year: int) -> tuple[list[str], list[str]]:
    """Local wall-clock hours that do not exist (spring forward) or occur twice (fall back).

    Returns two sorted lists of 'YYYY-MM-DD HH:00:00' strings for the years first..last inclusive.
    A wall time is nonexistent if converting it to UTC and back gives a different wall time; it is
    ambiguous if it exists and its two possible offsets (fold=0/1) differ.
    """
    tz = ZoneInfo(tz_name)
    utc = dt.timezone.utc
    nonexistent: list[str] = []
    ambiguous: list[str] = []
    t = dt.datetime(first_year, 1, 1)
    end = dt.datetime(last_year + 1, 1, 1)
    while t < end:
        a, b = t.replace(tzinfo=tz, fold=0), t.replace(tzinfo=tz, fold=1)
        if a.utcoffset() != b.utcoffset():
            back = a.astimezone(utc).astimezone(tz).replace(tzinfo=None)
            (ambiguous if back == t else nonexistent).append(t.strftime("%Y-%m-%d %H:%M:%S"))
        t += dt.timedelta(hours=1)
    return nonexistent, ambiguous
