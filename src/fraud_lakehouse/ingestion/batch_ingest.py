"""
Stage 1 -- land raw transaction extracts into the bronze layer.

Bronze rules (Part 10 SS4.1):
  append-only & IMMUTABLE per partition  -> we overwrite whole partitions,
                                            never mutate inside one
  partitioned by ingest_date             -> ingest_date=YYYY-MM-DD/ prefix
  idempotent                             -> delete_prefix + re-upload
  registered in Glue                     -> so Athena/Spark see it instantly

Glue registration is best-effort in dev (MinIO has no Glue); controlled by
register_glue=True in the prod DAG.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

from fraud_lakehouse.common.config import AppConfig, load_config
from fraud_lakehouse.common.exceptions import IngestionError
from fraud_lakehouse.common.logger import get_logger
from fraud_lakehouse.utils import s3_utils

logger = get_logger(__name__)


class BronzeIngestor:
    """Lands one logical date of raw data into s3://<bucket>/bronze/."""

    def __init__(self, cfg: AppConfig, register_glue: bool = False):
        self.cfg = cfg
        self.register_glue = register_glue

    # -- path helpers ----------------------------------------------------
    def partition_prefix(self, logical_date: date) -> str:
        return (
            f"{self.cfg.s3.bronze_prefix}/transactions/" f"ingest_date={logical_date.isoformat()}"
        )

    def bronze_uri(self, logical_date: date) -> str:
        return f"s3://{self.cfg.s3.bucket}/{self.partition_prefix(logical_date)}"

    # -- main entrypoint --------------------------------------------------
    def run(self, source: Path, logical_date: date) -> str:
        """Validate -> overwrite partition -> upload -> (register). Returns URI."""
        source = Path(source)
        prefix = self.partition_prefix(logical_date)
        logger.info("bronze ingest start | %s -> %s", source, self.bronze_uri(logical_date))
        try:
            files = self._validate_source(source)

            # IDEMPOTENCY: wipe the partition, then write. Run it twice,
            # get byte-identical results -- the SS4.2 idempotency gate.
            s3_utils.delete_prefix(self.cfg.s3.bucket, prefix)
            for f in files:
                s3_utils.upload_file(f, self.cfg.s3.bucket, f"{prefix}/{f.name}")

            if self.register_glue:
                self._register_partition(logical_date)

            logger.info("bronze ingest done | %d file(s)", len(files))
            return self.bronze_uri(logical_date)
        except IngestionError:
            raise
        except Exception as e:
            logger.error("bronze ingest failed for %s", logical_date, exc_info=True)
            raise IngestionError(f"Bronze ingest failed for logical_date={logical_date}", e) from e

    # -- internals ----------------------------------------------------------
    @staticmethod
    def _validate_source(source: Path) -> list[Path]:
        """A file or a directory of files; must exist and be non-empty."""
        if not source.exists():
            raise IngestionError(f"raw extract missing: {source}")
        files = sorted(source.glob("*")) if source.is_dir() else [source]
        files = [f for f in files if f.is_file() and f.stat().st_size > 0]
        if not files:
            raise IngestionError(f"no non-empty files found at: {source}")
        return files

    def _register_partition(self, logical_date: date) -> None:
        """Register/refresh the ingest_date partition in the Glue catalog."""
        try:
            import boto3

            glue = boto3.client("glue")
            location = self.bronze_uri(logical_date)
            try:
                glue.create_partition(
                    DatabaseName="bronze",
                    TableName="transactions",
                    PartitionInput={
                        "Values": [logical_date.isoformat()],
                        "StorageDescriptor": {"Location": location},
                    },
                )
                logger.info("glue partition created: %s", location)
            except glue.exceptions.AlreadyExistsException:
                logger.info("glue partition already registered (idempotent re-run)")
        except Exception as e:
            logger.error("glue registration failed", exc_info=True)
            raise IngestionError("Glue partition registration failed", e) from e


if __name__ == "__main__":
    ingestor = BronzeIngestor(load_config())
    print(ingestor.bronze_uri(date.today()))
