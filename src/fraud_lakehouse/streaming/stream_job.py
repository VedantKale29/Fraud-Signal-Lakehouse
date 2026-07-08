"""
Stage 2 -- Kafka -> windowed velocity features -> exactly-once into Iceberg.

EXACTLY-ONCE, SPELLED OUT (the Part-7 formula, now implemented):
  1. replayable source : Kafka retains offsets -> any batch is re-readable
  2. checkpoint        : Spark records which offsets each micro-batch
                         covered, atomically, on S3
  3. transactional sink: foreachBatch runs a MERGE keyed on
                         (wallet_id, window_start) -- so if a batch is
                         REPLAYED after a crash, the MERGE updates the same
                         rows instead of inserting duplicates.
  Crash between sink-commit and checkpoint-commit? The batch replays and
  the MERGE makes the replay a no-op. That is the whole trick -- and the
  kill-and-restart chaos test (tests/integration) proves it.

PURE vs PLUMBING (same rule as Stage 1):
  parse_events / build_features / prepare_batch  -> pure, unit-tested here
  run()                                          -> Kafka+checkpoint wiring
"""

from __future__ import annotations

from fraud_lakehouse.common.config import AppConfig, load_config
from fraud_lakehouse.common.exceptions import StreamingError
from fraud_lakehouse.common.logger import get_logger
from fraud_lakehouse.utils.spark_session import get_spark

logger = get_logger(__name__)

WATERMARK = "15 minutes"   # tuned from measured p99 event lag (SS5.2 gate)
WINDOW = "10 minutes"

EVENT_SCHEMA_DDL = (
    "tx_id STRING, wallet_id STRING, counterparty_id STRING, "
    "value DOUBLE, asset STRING, event_ts STRING, produced_ts STRING"
)


def parse_events(raw_df):
    """Kafka `value` bytes -> typed columns. Unparseable JSON -> _corrupt
    flag (kept for a dead-letter sink, never silently dropped -- the
    streaming twin of the batch quarantine)."""
    try:
        from pyspark.sql import functions as F

        parsed = raw_df.select(
            F.from_json(F.col("value").cast("string"), EVENT_SCHEMA_DDL).alias("e"),
            F.col("value").cast("string").alias("_raw"),
        )
        return parsed.select(
            "e.tx_id",
            "e.wallet_id",
            "e.counterparty_id",
            F.col("e.value").alias("value"),
            "e.asset",
            F.to_timestamp("e.event_ts").alias("event_ts"),
            F.to_timestamp("e.produced_ts").alias("produced_ts"),
            F.col("e.tx_id").isNull().alias("_corrupt"),
            "_raw",
        )
    except Exception as e:
        logger.error("parse_events failed", exc_info=True)
        raise StreamingError("parse_events failed", e) from e


def build_features(events_df):
    """Watermarked, windowed per-wallet velocity features (pure)."""
    try:
        from pyspark.sql import functions as F

        return (
            events_df.withWatermark("event_ts", WATERMARK)
            .groupBy(F.window("event_ts", WINDOW).alias("w"), F.col("wallet_id"))
            .agg(
                F.count("tx_id").alias("tx_count"),
                F.sum("value").alias("total_value"),
                F.approx_count_distinct("counterparty_id").alias("distinct_counterparties"),
                F.max("value").alias("max_single_value"),
            )
            .select(
                F.col("w.start").alias("window_start"),
                F.col("w.end").alias("window_end"),
                "wallet_id",
                "tx_count",
                "total_value",
                "distinct_counterparties",
                "max_single_value",
            )
        )
    except Exception as e:
        logger.error("build_features failed", exc_info=True)
        raise StreamingError("build_features failed", e) from e


def prepare_batch(batch_df):
    """Last defence inside foreachBatch: collapse any within-batch dupes on
    the MERGE key so the MERGE itself can never be ambiguous."""
    try:
        from pyspark.sql import Window
        from pyspark.sql import functions as F

        w = Window.partitionBy("wallet_id", "window_start").orderBy(
            F.col("tx_count").desc()
        )
        return (
            batch_df.withColumn("_rn", F.row_number().over(w))
            .filter(F.col("_rn") == 1)
            .drop("_rn")
        )
    except Exception as e:
        logger.error("prepare_batch failed", exc_info=True)
        raise StreamingError("prepare_batch failed", e) from e


MERGE_SQL = """
MERGE INTO {catalog}.gold.fact_wallet_window AS t
USING batch_updates AS s
ON  t.wallet_id = s.wallet_id AND t.window_start = s.window_start
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
"""


def make_upsert(catalog: str):
    """foreachBatch callback: idempotent MERGE keyed on (wallet, window)."""

    def _upsert(batch_df, batch_id: int) -> None:
        try:
            cleaned = prepare_batch(batch_df)
            cleaned.createOrReplaceTempView("batch_updates")
            cleaned.sparkSession.sql(MERGE_SQL.format(catalog=catalog))
            logger.info("micro-batch %d merged (%d rows)", batch_id, cleaned.count())
        except Exception as e:
            logger.error("micro-batch %d failed", batch_id, exc_info=True)
            raise StreamingError(f"micro-batch {batch_id} upsert failed", e) from e

    return _upsert


def run(cfg: AppConfig, local: bool = False) -> None:
    spark = get_spark(cfg, local=local)
    logger.info("stream job starting | topic=%s", cfg.kafka.transactions_topic)
    try:
        raw = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", cfg.kafka.bootstrap_servers)
            .option("subscribe", cfg.kafka.transactions_topic)
            .option("startingOffsets", "earliest")
            .load()
        )
        events = parse_events(raw).filter("NOT _corrupt")
        features = build_features(events)
        checkpoint = f"s3a://{cfg.s3.bucket}/_checkpoints/fact_wallet_window"
        (
            features.writeStream.foreachBatch(make_upsert(cfg.spark.catalog_name))
            .option("checkpointLocation", checkpoint)
            .outputMode("update")
            .start()
            .awaitTermination()
        )
    except Exception as e:
        logger.error("stream job crashed", exc_info=True)
        raise StreamingError("stream job crashed", e) from e


if __name__ == "__main__":
    run(load_config(), local=True)
