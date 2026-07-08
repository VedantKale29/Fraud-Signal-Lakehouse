"""
Typed config loader: configs/config.yaml + environment overrides.

Every module gets its settings through here — no hard-coded bucket names,
topic names, or paths anywhere else in the codebase.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

import yaml

from fraud_lakehouse.common.exceptions import ConfigError
from fraud_lakehouse.common.logger import get_logger

logger = get_logger(__name__)

DEFAULT_CONFIG_PATH = Path(os.getenv("FSL_CONFIG", "configs/config.yaml"))


@dataclass(frozen=True)
class S3Config:
    bucket: str
    bronze_prefix: str
    silver_prefix: str
    gold_prefix: str


@dataclass(frozen=True)
class KafkaConfig:
    bootstrap_servers: str
    transactions_topic: str
    consumer_group: str


@dataclass(frozen=True)
class SparkConfig:
    app_name: str
    catalog_name: str
    warehouse: str
    shuffle_partitions: int


@dataclass(frozen=True)
class AppConfig:
    env: str
    s3: S3Config
    kafka: KafkaConfig
    spark: SparkConfig


def load_config(path: Path | str = DEFAULT_CONFIG_PATH) -> AppConfig:
    """Load YAML config; environment variables win over file values."""
    path = Path(path)
    if not path.exists():
        raise ConfigError(f"Config file not found: {path.resolve()}")
    try:
        raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        env = os.getenv("FSL_ENV", raw.get("env", "dev"))
        s3 = raw.get("s3", {})
        kafka = raw.get("kafka", {})
        spark = raw.get("spark", {})
        cfg = AppConfig(
            env=env,
            s3=S3Config(
                bucket=os.getenv("FSL_S3_BUCKET", s3.get("bucket", "")),
                bronze_prefix=s3.get("bronze_prefix", "bronze"),
                silver_prefix=s3.get("silver_prefix", "silver"),
                gold_prefix=s3.get("gold_prefix", "gold"),
            ),
            kafka=KafkaConfig(
                bootstrap_servers=os.getenv(
                    "FSL_KAFKA_BOOTSTRAP", kafka.get("bootstrap_servers", "localhost:9092")
                ),
                transactions_topic=kafka.get("transactions_topic", "onchain.transactions"),
                consumer_group=kafka.get("consumer_group", "fsl-stream"),
            ),
            spark=SparkConfig(
                app_name=spark.get("app_name", "fraud-signal-lakehouse"),
                catalog_name=spark.get("catalog_name", "glue_catalog"),
                warehouse=spark.get("warehouse", "s3://CHANGE_ME/warehouse"),
                shuffle_partitions=int(spark.get("shuffle_partitions", 64)),
            ),
        )
        logger.info("Loaded config for env=%s from %s", cfg.env, path)
        return cfg
    except ConfigError:
        raise
    except Exception as e:  # malformed yaml, wrong types, etc.
        logger.error("Failed to parse config %s", path, exc_info=True)
        raise ConfigError(f"Failed to parse config {path}", e) from e
