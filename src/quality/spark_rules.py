"""Evaluate quality rules on a Spark DataFrame (spec T05).

apply_rules() adds:
  _violations  array<struct<rule:string, category:string, severity:string>>  (rules this record breaks)
  _outcome     'accepted' | 'quarantine' | 'rejected'  (worst severity wins, see rules.outcome)
A rule expression that evaluates to NULL counts as "not violated", so each rule must test for the
missing case explicitly (the missing_* rules do).
"""
from __future__ import annotations

from typing import Iterable

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from src.quality.rules import Rule

VIOLATION_TYPE = "array<struct<rule:string,category:string,severity:string>>"


def violations_col(rules: Iterable[Rule]) -> Column:
    items = [F.when(F.coalesce(F.expr(r.violation_expr), F.lit(False)),
                    F.struct(F.lit(r.name).alias("rule"), F.lit(r.category).alias("category"),
                             F.lit(r.severity).alias("severity")))
             for r in rules]
    if not items:
        return F.expr(f"CAST(array() AS {VIOLATION_TYPE})")
    return F.filter(F.array(*items), lambda v: v.isNotNull())


def outcome_col(violations: Column) -> Column:
    sev = violations.getField("severity")
    return (F.when(F.array_contains(sev, "reject"), "rejected")
             .when(F.array_contains(sev, "quarantine"), "quarantine")
             .otherwise("accepted"))


def apply_rules(df: DataFrame, rules: Iterable[Rule], existing: str | None = None) -> DataFrame:
    """Add _violations/_outcome. If `existing` names an array column of earlier violations, append to it."""
    new = violations_col(list(rules))
    v = F.concat(F.col(existing), new) if existing else new
    return df.withColumn("_violations", v).withColumn("_outcome", outcome_col(F.col("_violations")))


def rule_counts(df: DataFrame) -> dict[str, int]:
    """Records violating each rule (a record breaking two rules counts once for each)."""
    rows = (df.select(F.explode("_violations").alias("v"))
              .groupBy(F.col("v.rule").alias("rule")).count().collect())
    return {r["rule"]: int(r["count"]) for r in rows}


def outcome_counts(df: DataFrame) -> dict[str, int]:
    return {r["_outcome"]: int(r["count"]) for r in df.groupBy("_outcome").count().collect()}
