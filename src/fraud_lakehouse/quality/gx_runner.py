"""
Stage 1/3 -- Great Expectations gate at the ingest boundary (WAP audit step).

A failed suite raises DataQualityError so Airflow halts BEFORE silver:
bad data must never become queryable (Part 10 SS4.2 / SS6.1).
"""

from __future__ import annotations

from fraud_lakehouse.common.exceptions import DataQualityError, SchemaContractError
from fraud_lakehouse.common.logger import get_logger

logger = get_logger(__name__)

# Mirrors docs/data_contract.md -- the contract IS the suite.
CONTRACT_COLUMNS = {"tx_id", "wallet_id", "event_ts", "value", "asset"}


def audit_dataframe(df) -> None:
    """Run contract checks on a batch; raise DataQualityError on breach."""
    try:
        cols = set(df.columns)
        missing = CONTRACT_COLUMNS - cols
        if missing:
            raise SchemaContractError(f"Contract breach: missing columns {missing}")
        # TODO(Stage 1): full GX checkpoint -- row-count deltas within bounds,
        # event_ts not in future, tx_id uniqueness, value >= 0.
        logger.info("GX audit passed schema-contract check (%d columns)", len(cols))
    except DataQualityError:
        raise
    except Exception as e:
        logger.error("GX audit crashed", exc_info=True)
        raise DataQualityError("GX audit crashed (not a data failure)", e) from e
