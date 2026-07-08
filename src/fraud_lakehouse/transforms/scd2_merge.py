"""
Stage 1 -- SCD Type 2 on dim_wallet (Parts 4 + 6 of the series).

TWO LAYERS, ONE SPEC:

1. apply_scd2_batch(dims, updates)  -- PURE DataFrame function defining
   the exact SCD2 semantics. Unit-tested (the two-change-wallet gate)
   with plain local Spark: no Iceberg, no S3, runs in CI.

2. apply_scd2(spark, catalog)       -- PRODUCTION path: the same semantics
   executed as the classic TWO-PASS Iceberg operation:
     pass 1 (MERGE) : close current rows whose risk_tier changed
     pass 2 (INSERT): open the new current rows (changed + brand-new wallets)
   Iceberg makes both passes ACID; a failure between passes is recovered
   by re-running (MERGE matches nothing the 2nd time -> idempotent).

Update-batch rule: if a wallet appears multiple times in one batch we keep
only the LATEST snapshot_ts -- intra-batch history is below our grain.
"""

from __future__ import annotations

from fraud_lakehouse.common.exceptions import TransformError
from fraud_lakehouse.common.logger import get_logger

logger = get_logger(__name__)

DIM_COLUMNS = ["wallet_id", "risk_tier", "valid_from", "valid_to", "is_current"]


def _latest_update_per_wallet(updates):
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    w = Window.partitionBy("wallet_id").orderBy(F.col("snapshot_ts").desc())
    return updates.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1).drop("_rn")


def apply_scd2_batch(dims, updates):
    """Return the NEW full state of dim_wallet after applying one batch.

    Semantics (this function IS the spec the MERGE must match):
      unchanged wallet (same tier)      -> row untouched
      changed wallet                    -> current row closed
                                           (valid_to = snapshot_ts,
                                            is_current = false)
                                           + new current row opened
      brand-new wallet                  -> new current row opened
      historical (non-current) rows     -> never touched
    """
    try:
        from pyspark.sql import functions as F

        upd = _latest_update_per_wallet(updates)

        current = dims.filter(F.col("is_current"))
        history = dims.filter(~F.col("is_current"))

        joined = current.alias("c").join(upd.alias("u"), "wallet_id", "full_outer")

        changed = joined.filter(
            F.col("c.risk_tier").isNotNull()
            & F.col("u.risk_tier").isNotNull()
            & (F.col("c.risk_tier") != F.col("u.risk_tier"))
        )
        unchanged = joined.filter(
            F.col("c.risk_tier").isNotNull()
            & (F.col("u.risk_tier").isNull() | (F.col("c.risk_tier") == F.col("u.risk_tier")))
        )
        brand_new = joined.filter(F.col("c.risk_tier").isNull())

        untouched_current = unchanged.select(
            "wallet_id", "c.risk_tier", "c.valid_from", "c.valid_to", "c.is_current"
        )
        closed = changed.select(
            "wallet_id",
            "c.risk_tier",
            "c.valid_from",
            F.col("u.snapshot_ts").alias("valid_to"),
            F.lit(False).alias("is_current"),
        )
        opened = (
            changed.select("wallet_id", "u.risk_tier", "u.snapshot_ts")
            .unionByName(brand_new.select("wallet_id", "u.risk_tier", "u.snapshot_ts"))
            .select(
                "wallet_id",
                "risk_tier",
                F.col("snapshot_ts").alias("valid_from"),
                F.lit(None).cast("timestamp").alias("valid_to"),
                F.lit(True).alias("is_current"),
            )
        )

        new_state = (
            history.select(*DIM_COLUMNS)
            .unionByName(untouched_current.select(*DIM_COLUMNS))
            .unionByName(closed.select(*DIM_COLUMNS))
            .unionByName(opened.select(*DIM_COLUMNS))
        )
        logger.info("SCD2 batch plan computed")
        return new_state
    except Exception as e:
        logger.error("apply_scd2_batch failed", exc_info=True)
        raise TransformError("apply_scd2_batch failed", e) from e


# ----------------------------------------------------------------------
# Production path: the same semantics as two ACID passes on Iceberg.
# ----------------------------------------------------------------------

SCD2_CLOSE_SQL = """
MERGE INTO {catalog}.gold.dim_wallet AS t
USING (
    SELECT wallet_id, risk_tier, snapshot_ts
    FROM   {catalog}.silver.wallet_updates_latest
) AS s
ON  t.wallet_id = s.wallet_id AND t.is_current = true
WHEN MATCHED AND t.risk_tier <> s.risk_tier THEN
    UPDATE SET t.valid_to = s.snapshot_ts, t.is_current = false
"""

SCD2_OPEN_SQL = """
INSERT INTO {catalog}.gold.dim_wallet
SELECT  s.wallet_id,
        s.risk_tier,
        s.snapshot_ts       AS valid_from,
        CAST(NULL AS TIMESTAMP) AS valid_to,
        true                AS is_current
FROM    {catalog}.silver.wallet_updates_latest s
LEFT JOIN {catalog}.gold.dim_wallet t
       ON t.wallet_id = s.wallet_id AND t.is_current = true
WHERE   t.wallet_id IS NULL      -- brand-new, or just closed in pass 1
"""


def apply_scd2(spark, catalog: str) -> None:
    """Two-pass SCD2 on Iceberg. Idempotent: re-running matches nothing."""
    logger.info("SCD2 pass 1 (close changed rows) | catalog=%s", catalog)
    try:
        spark.sql(SCD2_CLOSE_SQL.format(catalog=catalog))
        logger.info("SCD2 pass 2 (open new current rows)")
        spark.sql(SCD2_OPEN_SQL.format(catalog=catalog))
        logger.info("SCD2 complete")
    except Exception as e:
        logger.error("SCD2 two-pass failed", exc_info=True)
        raise TransformError("SCD2 two-pass MERGE failed on dim_wallet", e) from e
