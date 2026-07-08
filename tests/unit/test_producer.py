"""Stage-2 gate for the chaos producer: the knobs actually inject chaos."""

import random
from datetime import datetime

from fraud_lakehouse.common.config import AppConfig, KafkaConfig, S3Config, SparkConfig
from fraud_lakehouse.streaming.producer import ChaosProfile, TransactionProducer


def _cfg():
    return AppConfig(
        env="test",
        s3=S3Config("b", "br", "s", "g"),
        kafka=KafkaConfig("localhost:9092", "t", "g"),
        spark=SparkConfig("t", "c", "w", 2),
    )


def test_event_shape_matches_contract():
    p = TransactionProducer(_cfg(), ChaosProfile(late_fraction=0.0))
    e = p.make_event(1)
    assert set(e) == {
        "tx_id", "wallet_id", "counterparty_id", "value",
        "asset", "event_ts", "produced_ts",
    }
    assert e["value"] >= 0


def test_late_fraction_one_makes_every_event_late():
    random.seed(42)
    p = TransactionProducer(_cfg(), ChaosProfile(late_fraction=1.0, late_minutes_max=20))
    for i in range(50):
        e = p.make_event(i)
        lag = datetime.fromisoformat(e["produced_ts"]) - datetime.fromisoformat(e["event_ts"])
        assert 0 < lag.total_seconds() <= 20 * 60


def test_late_fraction_zero_means_no_lag():
    p = TransactionProducer(_cfg(), ChaosProfile(late_fraction=0.0))
    e = p.make_event(1)
    assert e["event_ts"] == e["produced_ts"]
