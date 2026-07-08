"""
Stage 2 -- Structured Streaming job: Kafka -> windowed velocity features
-> exactly-once MERGE into Iceberg (Part 10 SS5.1).

Exactly-once formula (Part 7): replayable source (Kafka offsets)
+ S3 checkpoint + transactional Iceberg sink via foreachBatch MERGE.
"""

from __future__ import annotations

from fraud_lakehouse.common.config import AppConfig, load_config
from fraud_lakehouse.common.exceptions import StreamingError
from fraud_lakehouse.common.logger import get_logger
from fraud_lakehouse.utils.spark_session import get_spark

logger = get_logger(__name__)

WATERMARK = "15 minutes"   # tuned from measured p99 event lag (SS5.2 late-data test)
WINDOW = "10 minutes"


def build_features(events_df):
    """Windowed per-wallet velocity features. Pure -> unit-testable."""
    try:
        from pyspark.sql import functions as F

        return (
            events_df.withWatermark("event_ts", WATERMARK)
            .groupBy(F.window("event_ts", WINDOW), F.col("wallet_id"))
            .agg(
                F.count("tx_id").alias("tx_count"),
                F.sum("value").alias("total_value"),
                F.approx_count_distinct("counterparty_id").alias("distinct_counterparties"),
            )
        )
    except Exception as e:
        logger.error("build_features failed", exc_info=True)
        raise StreamingError("build_features failed", e) from e


def run(cfg: AppConfig, local: bool = False) -> None:
    spark = get_spark(cfg, local=local)
    logger.info("Stream job starting | topic=%s", cfg.kafka.transactions_topic)
    try:
        raw = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", cfg.kafka.bootstrap_servers)
            .option("subscribe", cfg.kafka.transactions_topic)
            .option("startingOffsets", "latest")
            .load()
        )
        # TODO(Stage 2): parse JSON, build_features, foreachBatch MERGE into
        # {catalog}.gold.fact_wallet_window with checkpointLocation on S3.
        _ = raw
        raise NotImplementedError("Stage 2 build task -- see Part 10 SS5.1")
    except NotImplementedError:
        raise
    except Exception as e:
        logger.error("Stream job crashed", exc_info=True)
        raise StreamingError("Stream job crashed", e) from e


if __name__ == "__main__":
    run(load_config(), local=True)
