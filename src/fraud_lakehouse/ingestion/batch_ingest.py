"""
Stage 1 — land raw transaction extracts into the bronze layer.

Bronze rules (Part 10 SS4.1): append-only, immutable, partitioned by
ingest_date, registered in the Glue catalog. Bad files are quarantined,
never silently dropped.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

from fraud_lakehouse.common.config import AppConfig, load_config
from fraud_lakehouse.common.exceptions import IngestionError
from fraud_lakehouse.common.logger import get_logger

logger = get_logger(__name__)


class BronzeIngestor:
    """Lands one logical date of raw data into s3://<bucket>/bronze/."""

    def __init__(self, cfg: AppConfig):
        self.cfg = cfg

    def bronze_path(self, logical_date: date) -> str:
        return (
            f"s3://{self.cfg.s3.bucket}/{self.cfg.s3.bronze_prefix}"
            f"/transactions/ingest_date={logical_date.isoformat()}"
        )

    def run(self, source: Path, logical_date: date) -> str:
        """Copy/validate raw extract -> bronze. Returns the bronze path.

        Idempotent by design: re-running the same logical_date overwrites
        the same partition (never appends a duplicate).
        """
        target = self.bronze_path(logical_date)
        logger.info("Bronze ingest start | source=%s -> %s", source, target)
        try:
            if not source.exists():
                raise FileNotFoundError(f"raw extract missing: {source}")
            # TODO(Stage 1): boto3 multipart upload + Glue partition registration
            raise NotImplementedError("Stage 1 build task -- see Part 10 SS4.1")
        except NotImplementedError:
            raise
        except Exception as e:
            logger.error("Bronze ingest failed for %s", logical_date, exc_info=True)
            raise IngestionError(
                f"Bronze ingest failed for logical_date={logical_date}", e
            ) from e


if __name__ == "__main__":
    ingestor = BronzeIngestor(load_config())
    print(ingestor.bronze_path(date.today()))
