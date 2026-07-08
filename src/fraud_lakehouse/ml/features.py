"""
Stage 4 -- the feature mart: the DE -> ML handoff, made explicit.

One row per wallet, combining batch behavioural features from the silver
transaction table with the fraud label (majority vote over the wallet's
transactions -- Elliptic labels transactions; our unit of decision is the
wallet). Written to gold as `fact_wallet_features`; the model trains on
EXACTLY what the mart serves -- no separate feature pipeline, so
training-serving skew is structurally impossible (the SS7.2 parity gate).
"""

from __future__ import annotations

from fraud_lakehouse.common.exceptions import TransformError
from fraud_lakehouse.common.logger import get_logger

logger = get_logger(__name__)

FEATURE_COLUMNS = [
    "tx_count",
    "total_value",
    "avg_value",
    "max_value",
    "distinct_counterparties",
    "active_days",
]


def build_wallet_features(transactions, labels=None):
    """silver.transactions (+ optional labels) -> one row per wallet.

    Pure function: unit-testable on tiny frames, and the SAME function the
    batch job calls -- the spec and the implementation are one thing.
    """
    try:
        from pyspark.sql import functions as F

        feats = transactions.groupBy("wallet_id").agg(
            F.count("tx_id").alias("tx_count"),
            F.sum("value").cast("double").alias("total_value"),
            F.avg("value").cast("double").alias("avg_value"),
            F.max("value").cast("double").alias("max_value"),
            F.approx_count_distinct("counterparty_id").alias("distinct_counterparties"),
            F.countDistinct(F.to_date("event_ts")).alias("active_days"),
            F.min("event_ts").alias("first_seen_ts"),  # drives the time split
        )
        if labels is not None:
            wallet_labels = (
                transactions.join(labels, "tx_id", "left")
                .groupBy("wallet_id")
                .agg(
                    F.sum(F.when(F.col("fraud_label") == "ILLICIT", 1).otherwise(0)).alias(
                        "n_illicit"
                    ),
                    F.sum(F.when(F.col("fraud_label") == "LICIT", 1).otherwise(0)).alias("n_licit"),
                )
                .withColumn(
                    "label",
                    F.when(F.col("n_illicit") > 0, 1)  # any illicit tx -> risky wallet
                    .when(F.col("n_licit") > 0, 0)
                    .otherwise(F.lit(None).cast("int")),  # fully unknown -> excluded from training
                )
                .select("wallet_id", "label")
            )
            feats = feats.join(wallet_labels, "wallet_id", "left")
        logger.info("wallet features built")
        return feats
    except Exception as e:
        logger.error("build_wallet_features failed", exc_info=True)
        raise TransformError("build_wallet_features failed", e) from e
