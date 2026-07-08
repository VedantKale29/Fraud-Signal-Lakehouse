"""Stage-0 gate: config loads, env vars override, bad path raises ConfigError."""

import pytest

from fraud_lakehouse.common.config import load_config
from fraud_lakehouse.common.exceptions import ConfigError


def test_missing_file_raises_config_error(tmp_path):
    with pytest.raises(ConfigError):
        load_config(tmp_path / "nope.yaml")


def test_loads_and_env_override(tmp_path, monkeypatch):
    p = tmp_path / "config.yaml"
    p.write_text(
        "env: dev\ns3:\n  bucket: from-file\nkafka: {}\nspark: {}\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("FSL_S3_BUCKET", "from-env")
    cfg = load_config(p)
    assert cfg.s3.bucket == "from-env"       # env wins
    assert cfg.kafka.bootstrap_servers == "localhost:9092"  # default fills gaps
    assert cfg.spark.shuffle_partitions == 64
