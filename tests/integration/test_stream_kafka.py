"""Stage-2 INTEGRATION gates -- require the local docker stack:

    make up                       # kafka + minio
    pytest tests/integration -m integration

These are the tests that prove runtime claims (late-data drop beyond the
watermark, checkpoint recovery). Unit tests can't prove those -- don't
pretend otherwise.
"""

import json
import time

import pytest

pytestmark = pytest.mark.integration

BOOTSTRAP = "localhost:9092"
TOPIC = "onchain.transactions.test"


@pytest.fixture(scope="module")
def kafka_up():
    from kafka import KafkaAdminClient
    from kafka.admin import NewTopic

    try:
        admin = KafkaAdminClient(bootstrap_servers=BOOTSTRAP, request_timeout_ms=3000)
    except Exception:
        pytest.skip("kafka not running -- `make up` first")
    try:
        admin.create_topics([NewTopic(TOPIC, num_partitions=1, replication_factor=1)])
    except Exception:
        pass
    return admin


def test_producer_roundtrip(kafka_up):
    """Chaos producer -> Kafka -> consumer sees >= sent (dupes included)."""
    from kafka import KafkaConsumer

    from fraud_lakehouse.common.config import load_config
    from fraud_lakehouse.streaming.producer import ChaosProfile, TransactionProducer

    cfg = load_config()
    cfg = type(cfg)(
        env=cfg.env, s3=cfg.s3,
        kafka=type(cfg.kafka)(BOOTSTRAP, TOPIC, "itest"),
        spark=cfg.spark,
    )
    TransactionProducer(cfg, ChaosProfile(duplicate_fraction=0.5)).run(
        events_per_sec=200, total=100
    )
    consumer = KafkaConsumer(
        TOPIC, bootstrap_servers=BOOTSTRAP, auto_offset_reset="earliest",
        consumer_timeout_ms=5000, group_id=f"itest-{time.time()}",
    )
    msgs = [json.loads(m.value) for m in consumer]
    assert len(msgs) >= 100                      # duplicates arrived too
    assert {m["tx_id"] for m in msgs} == {f"tx-{i}" for i in range(100)}


# TODO(Stage 2, on your machine):
#   test_late_data_beyond_watermark_dropped  -- producer late_minutes_max=20,
#       watermark 15m -> assert 20m-late events absent from fact_wallet_window
#   test_kill_restart_recovers_from_checkpoint -- SIGKILL stream mid-batch,
#       restart, diff sink vs batch-computed reference (zero loss, zero dupes)
