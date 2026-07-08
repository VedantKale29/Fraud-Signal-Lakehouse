"""
Stage 1 -- the AUDIT step of Write-Audit-Publish, at the ingest boundary.

Checks are implemented natively in PySpark against docs/data_contract.md
(the contract IS the suite). A breach raises DataQualityError -> the
Airflow task fails -> nothing downstream runs -> bad data never becomes
queryable. That exception firing is the system WORKING, not breaking.

Great Expectations checkpoint wrapping (HTML data docs for the portfolio)
is an additive Stage-1 polish task; the enforcement logic lives here so
the gate is dependency-light and unit-testable.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from fraud_lakehouse.common.exceptions import DataQualityError, SchemaContractError
from fraud_lakehouse.common.logger import get_logger

logger = get_logger(__name__)

CONTRACT_COLUMNS = {"tx_id", "wallet_id", "event_ts", "value", "asset"}
ROW_COUNT_MIN = 1  # tune per docs/data_contract.md volumes
ROW_COUNT_MAX = 10_000_000  # sanity ceiling: a 200x day is a bug, not growth


@dataclass
class AuditReport:
    """Every check's result -- logged AND attached to the raised error."""

    row_count: int = 0
    breaches: list[str] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        return not self.breaches


def audit_dataframe(df, now_ts=None) -> AuditReport:
    """Run all contract checks. Returns the report; raises on any breach.

    `now_ts` is injectable so the future-timestamp check is deterministic
    in tests (never test against wall-clock time).
    """
    try:
        from datetime import datetime, timezone

        from pyspark.sql import functions as F

        report = AuditReport()
        now_ts = now_ts or datetime.now(timezone.utc)

        # 1. schema contract ------------------------------------------------
        missing = CONTRACT_COLUMNS - set(df.columns)
        if missing:
            raise SchemaContractError(f"contract breach: missing columns {sorted(missing)}")

        # single pass over the data for all aggregate checks ---------------
        agg = df.agg(
            F.count(F.lit(1)).alias("n_rows"),
            F.count("tx_id").alias("n_tx"),
            F.countDistinct("tx_id").alias("n_tx_distinct"),
            F.sum(F.when(F.col("value") < 0, 1).otherwise(0)).alias("n_negative"),
            F.sum(F.when(F.col("event_ts") > F.lit(now_ts), 1).otherwise(0)).alias("n_future"),
            F.sum(F.when(F.col("event_ts").isNull(), 1).otherwise(0)).alias("n_null_ts"),
        ).collect()[0]

        report.row_count = agg["n_rows"]

        # 2. volume bounds ---------------------------------------------------
        if not (ROW_COUNT_MIN <= agg["n_rows"] <= ROW_COUNT_MAX):
            report.breaches.append(
                f"row_count {agg['n_rows']} outside [{ROW_COUNT_MIN}, {ROW_COUNT_MAX}]"
            )
        # 3. uniqueness of the dedupe key -------------------------------------
        if agg["n_tx"] != agg["n_tx_distinct"]:
            report.breaches.append(
                f"tx_id not unique: {agg['n_tx'] - agg['n_tx_distinct']} duplicate rows"
            )
        # 4. value + event-time rules ------------------------------------------
        if agg["n_negative"]:
            report.breaches.append(f"{agg['n_negative']} rows with negative value")
        if agg["n_future"]:
            report.breaches.append(f"{agg['n_future']} rows with event_ts in the future")
        if agg["n_null_ts"]:
            report.breaches.append(f"{agg['n_null_ts']} rows with null event_ts")

        if not report.passed:
            logger.error("GX audit FAILED: %s", report.breaches)
            raise DataQualityError(f"audit failed: {report.breaches}")

        logger.info("GX audit passed | rows=%d", report.row_count)
        return report
    except DataQualityError:
        raise
    except Exception as e:
        logger.error("GX audit crashed (infrastructure, not data)", exc_info=True)
        raise DataQualityError("GX audit crashed (not a data failure)", e) from e
