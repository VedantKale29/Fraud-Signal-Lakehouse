"""
Custom exception hierarchy for the Fraud-Signal Lakehouse.

Every layer of the pipeline raises its own exception type, all inheriting
from ``LakehouseError``. Each exception automatically captures WHERE it
happened (file, line, function) from the active traceback, so logs and
Airflow task failures are immediately diagnosable.

Usage pattern (in every module):

    from fraud_lakehouse.common.exceptions import IngestionError
    from fraud_lakehouse.common.logger import get_logger

    logger = get_logger(__name__)

    try:
        ...
    except Exception as e:
        logger.error("Bronze landing failed", exc_info=True)
        raise IngestionError("Bronze landing failed for partition %s" % dt, e) from e
"""

from __future__ import annotations

import sys
import traceback
from typing import Optional


def _error_message_detail(message: str, original: Optional[BaseException]) -> str:
    """Build a rich error string: message + originating file/line/function."""
    _, _, tb = sys.exc_info()
    if tb is not None:
        # Walk to the deepest frame — where the error actually occurred.
        while tb.tb_next is not None:
            tb = tb.tb_next
        frame = tb.tb_frame
        detail = (
            f"{message} | file={frame.f_code.co_filename} "
            f"| func={frame.f_code.co_name} | line={tb.tb_lineno}"
        )
    else:
        detail = message
    if original is not None:
        detail += f" | caused_by={type(original).__name__}: {original}"
    return detail


class LakehouseError(Exception):
    """Base exception for every failure inside the fraud-signal lakehouse."""

    def __init__(self, message: str, original: Optional[BaseException] = None):
        self.message = message
        self.original = original
        self.detail = _error_message_detail(message, original)
        super().__init__(self.detail)

    def __str__(self) -> str:  # pragma: no cover - trivial
        return self.detail

    def full_traceback(self) -> str:
        """Return the formatted traceback of the original exception, if any."""
        if self.original is None:
            return self.detail
        return "".join(
            traceback.format_exception(
                type(self.original), self.original, self.original.__traceback__
            )
        )


class ConfigError(LakehouseError):
    """Bad/missing configuration (configs/*.yaml, env vars, secrets)."""


class IngestionError(LakehouseError):
    """Failures landing raw data into the bronze layer."""


class TransformError(LakehouseError):
    """Failures in Spark/dbt silver->gold transformations (incl. SCD2 MERGE)."""


class DataQualityError(LakehouseError):
    """A Great Expectations / dbt test gate failed — Write-Audit-Publish halt.

    This exception is *expected* in production: it is the audit step doing
    its job. Airflow should fail the task and alert, never skip.
    """


class StreamingError(LakehouseError):
    """Kafka / Structured Streaming failures (producer, stream job, sink)."""


class PublishError(LakehouseError):
    """Failure during the atomic publish/swap step of Write-Audit-Publish."""


class SchemaContractError(DataQualityError):
    """Incoming data violates the data contract in docs/data_contract.md."""
