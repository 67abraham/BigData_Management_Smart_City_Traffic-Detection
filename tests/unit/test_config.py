import pytest

from src.common.config import DEFAULT_CONFIG, hdfs_layer_path, load_config


def test_default_config_loads() -> None:
    cfg = load_config(DEFAULT_CONFIG, env={})
    assert cfg["hdfs"]["uri"].startswith("hdfs://")
    assert cfg["hdfs"]["layers"] == ["bronze", "silver", "gold"]


def test_env_override() -> None:
    cfg = load_config(DEFAULT_CONFIG, env={"BDFP_KAFKA_BOOTSTRAP_SERVERS": "localhost:29092"})
    assert cfg["kafka"]["bootstrap_servers"] == "localhost:29092"


def test_layer_path() -> None:
    cfg = load_config(DEFAULT_CONFIG, env={})
    assert hdfs_layer_path(cfg, "bronze", "uci_metro", "2026-11-19") == (
        "/datalake/bronze/uci_metro/ingest_date=2026-11-19"
    )
    with pytest.raises(ValueError):
        hdfs_layer_path(cfg, "platinum", "uci_metro", "2026-11-19")
