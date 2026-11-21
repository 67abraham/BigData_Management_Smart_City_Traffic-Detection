"""Unit tests for the dashboard loaders: they read the committed evidence of the configured runs."""
from src.visualisation import data as D

CFG = D.config()


def test_sources_cover_four_sources_from_the_configured_run():
    rows, src = D.sources(CFG)
    assert [r["source"] for r in rows] == ["uci_metro", "open_meteo", "nyc_tlc", "kitti"]
    assert CFG["runs"]["ingestion"] in src and all(r["records"] > 0 for r in rows)


def test_quality_outcomes_reconcile():
    outcomes, rules, srcs = D.quality(CFG)
    assert all(o["reconciled"] for o in outcomes) and len(srcs) == 3


def test_benchmark_rows_have_three_timed_runs():
    rows, _ = D.benchmark(CFG)
    assert rows and all(len(r["runs_s"].split(", ")) == 3 for r in rows)


def test_failed_runs_include_known_failures():
    runs = {(r["criterion"], r["run"]) for r in D.failed_runs()}
    assert ("AC04", "m3_20261121T091418Z") in runs          # Kafka retention
    assert ("AC01", "m1_20261121T060408Z") in runs          # MnDOT timeout (FAIL in summary)
