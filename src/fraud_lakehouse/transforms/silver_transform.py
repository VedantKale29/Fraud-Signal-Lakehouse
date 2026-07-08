"""
Stage 1 -- bronze -> silver: dedupe, cast, validate, quarantine.

Pure functions on DataFrames so every rule is unit-testable with chispa
on tiny in-memory frames (Part 10 SS4.2, unit-test gate).
"""

from __future__ import annotations

from fraud_lakehouse.common.exceptions import TransformError
from fraud_lakehouse.common.logger import get_logger

logger = get_logger(__name__)

REQUIRED_COLUMNS = ["tx_id", "wallet_id", "event_ts", "value", "asset"]


def deduplicate(df):
    """Keep the latest record per tx_id (by event_ts). Pure + testable."""
    try:
        from pyspark.sql import Window
        from pyspark.sql import functions as F

        w = Window.partitionBy("tx_id").orderBy(F.col("event_ts").desc())
        out = (
            df.withColumn("_rn", F.row_number().over(w))
            .filter(F.col("_rn") == 1)
            .drop("_rn")
        )
        logger.debug("deduplicate applied")
        return out
    except Exception as e:
        logger.error("deduplicate failed", exc_info=True)
        raise TransformError("deduplicate failed", e) from e


def split_quarantine(df):
    """Return (clean_df, quarantine_df). Bad rows are kept, never dropped."""
    try:
        from pyspark.sql import functions as F

        bad_cond = (
            F.col("tx_id").isNull()
            | F.col("event_ts").isNull()
            | (F.col("value") < 0)
        )
        return df.filter(~bad_cond), df.filter(bad_cond)
    except Exception as e:
        logger.error("split_quarantine failed", exc_info=True)
        raise TransformError("split_quarantine failed", e) from e
