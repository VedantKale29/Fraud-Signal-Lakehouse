"""
Stage 1 -- bronze -> silver: cast/normalize, dedupe, quarantine.

DESIGN RULE: every rule is a PURE FUNCTION on DataFrames (no I/O, no
session creation inside). That is what makes the Part-10 SS4.2 unit gate
possible: chispa asserts on tiny in-memory frames, running in CI in
seconds. The silver *job* (I/O, Iceberg writes) composes these functions.

Pipeline order inside the silver job:
    raw -> cast_and_normalize -> split_quarantine -> deduplicate -> write
(quarantine BEFORE dedupe so a bad duplicate never silently wins).
"""

from __future__ import annotations

from fraud_lakehouse.common.exceptions import TransformError
from fraud_lakehouse.common.logger import get_logger

logger = get_logger(__name__)

REQUIRED_COLUMNS = ["tx_id", "wallet_id", "event_ts", "value", "asset"]
ACCEPTED_ASSETS = ["BTC", "ETH"]  # mirrors docs/data_contract.md


def cast_and_normalize(df):
    """Enforce the contract's types + trivial normalisation.

    - event_ts -> timestamp (bad strings become NULL -> quarantined next step)
    - value    -> decimal(38,8) (on-chain precision; never float for money)
    - asset    -> upper-cased, trimmed
    - ids      -> trimmed strings
    """
    try:
        from pyspark.sql import functions as F
        from pyspark.sql.types import DecimalType

        return (
            df.withColumn("tx_id", F.trim(F.col("tx_id").cast("string")))
            .withColumn("wallet_id", F.trim(F.col("wallet_id").cast("string")))
            .withColumn("event_ts", F.col("event_ts").cast("timestamp"))
            .withColumn("value", F.col("value").cast(DecimalType(38, 8)))
            .withColumn("asset", F.upper(F.trim(F.col("asset"))))
        )
    except Exception as e:
        logger.error("cast_and_normalize failed", exc_info=True)
        raise TransformError("cast_and_normalize failed", e) from e


def split_quarantine(df):
    """Return (clean_df, quarantine_df). Bad rows are KEPT, never dropped.

    A row is quarantined if it breaches the data contract:
    null key / null event time / negative value / unknown asset.
    The quarantine table is what lets you answer 'how do you handle bad
    data?' with evidence instead of hand-waving.
    """
    try:
        from pyspark.sql import functions as F

        bad_cond = (
            F.col("tx_id").isNull()
            | F.col("wallet_id").isNull()
            | F.col("event_ts").isNull()
            | (F.col("value") < 0)
            | (~F.col("asset").isin(ACCEPTED_ASSETS))
        )
        clean, quarantine = df.filter(~bad_cond), df.filter(bad_cond)
        return clean, quarantine
    except Exception as e:
        logger.error("split_quarantine failed", exc_info=True)
        raise TransformError("split_quarantine failed", e) from e


def deduplicate(df):
    """Keep exactly one record per tx_id -- the latest by event_ts.

    Ties on event_ts are broken deterministically by produced order via a
    stable secondary sort on wallet_id so re-runs give identical output
    (idempotency gate depends on determinism).
    """
    try:
        from pyspark.sql import Window
        from pyspark.sql import functions as F

        w = Window.partitionBy("tx_id").orderBy(F.col("event_ts").desc(), F.col("wallet_id").asc())
        return df.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1).drop("_rn")
    except Exception as e:
        logger.error("deduplicate failed", exc_info=True)
        raise TransformError("deduplicate failed", e) from e


def to_silver(df):
    """The full bronze->silver transform chain. Returns (silver, quarantine)."""
    normalized = cast_and_normalize(df)
    clean, quarantine = split_quarantine(normalized)
    silver = deduplicate(clean)
    logger.info("to_silver applied (cast -> quarantine -> dedupe)")
    return silver, quarantine
