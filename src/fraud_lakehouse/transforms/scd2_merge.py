"""
Stage 1 -- SCD Type 2 upkeep of dim_wallet via Iceberg MERGE (Part 4 + 6).

The MERGE closes the current row (valid_to, is_current=false) and inserts
the new version atomically. The unit-test gate feeds a wallet whose risk
tier changes twice and asserts exactly 3 rows / one is_current.
"""

from __future__ import annotations

from fraud_lakehouse.common.exceptions import TransformError
from fraud_lakehouse.common.logger import get_logger

logger = get_logger(__name__)

SCD2_MERGE_SQL = """
MERGE INTO {catalog}.gold.dim_wallet AS t
USING (
    SELECT wallet_id, risk_tier, snapshot_ts FROM {catalog}.silver.wallet_updates
) AS s
ON  t.wallet_id = s.wallet_id AND t.is_current = true
WHEN MATCHED AND t.risk_tier <> s.risk_tier THEN
    UPDATE SET t.valid_to = s.snapshot_ts, t.is_current = false
WHEN NOT MATCHED THEN
    INSERT (wallet_id, risk_tier, valid_from, valid_to, is_current)
    VALUES (s.wallet_id, s.risk_tier, s.snapshot_ts, NULL, true)
"""


def apply_scd2(spark, catalog: str) -> None:
    """Run the SCD2 MERGE + the follow-up insert of changed-wallet new rows."""
    logger.info("SCD2 MERGE start (catalog=%s)", catalog)
    try:
        spark.sql(SCD2_MERGE_SQL.format(catalog=catalog))
        # TODO(Stage 1): second pass inserting the *new* current rows for
        # wallets whose old row was just closed (classic 2-step SCD2 MERGE).
        raise NotImplementedError("Stage 1 build task -- see Part 10 SS4.1")
    except NotImplementedError:
        raise
    except Exception as e:
        logger.error("SCD2 MERGE failed", exc_info=True)
        raise TransformError("SCD2 MERGE failed on dim_wallet", e) from e
