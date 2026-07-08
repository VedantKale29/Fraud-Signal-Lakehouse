"""Stage-1 gate: bronze ingest is idempotent -- run twice, identical bytes.
Uses moto to mock S3 so the test needs zero AWS credentials or network."""

from datetime import date

import boto3
import pytest
from moto import mock_aws

from fraud_lakehouse.common.config import AppConfig, KafkaConfig, S3Config, SparkConfig
from fraud_lakehouse.common.exceptions import IngestionError
from fraud_lakehouse.ingestion.batch_ingest import BronzeIngestor
from fraud_lakehouse.utils.s3_utils import list_keys

BUCKET = "test-lake"


def _cfg() -> AppConfig:
    return AppConfig(
        env="test",
        s3=S3Config(bucket=BUCKET, bronze_prefix="bronze", silver_prefix="s", gold_prefix="g"),
        kafka=KafkaConfig("localhost:9092", "t", "g"),
        spark=SparkConfig("t", "c", "w", 2),
    )


@pytest.fixture
def raw_dir(tmp_path):
    d = tmp_path / "raw"
    d.mkdir()
    (d / "part-000.parquet").write_bytes(b"fake-parquet-bytes-1")
    (d / "part-001.parquet").write_bytes(b"fake-parquet-bytes-2")
    return d


@mock_aws
def test_ingest_lands_files_under_date_partition(raw_dir):
    boto3.client("s3").create_bucket(
        Bucket=BUCKET, CreateBucketConfiguration={"LocationConstraint": "ap-south-1"}
    )
    BronzeIngestor(_cfg()).run(raw_dir, date(2026, 7, 1))
    keys = list_keys(BUCKET, "bronze/transactions/ingest_date=2026-07-01")
    assert len(keys) == 2


@mock_aws
def test_rerun_same_date_is_idempotent(raw_dir):
    """THE idempotency gate: second run of the same logical date must not
    duplicate anything -- delete-prefix-then-write makes it an overwrite."""
    boto3.client("s3").create_bucket(
        Bucket=BUCKET, CreateBucketConfiguration={"LocationConstraint": "ap-south-1"}
    )
    ing = BronzeIngestor(_cfg())
    ing.run(raw_dir, date(2026, 7, 1))
    ing.run(raw_dir, date(2026, 7, 1))  # retry / backfill of the same date
    keys = list_keys(BUCKET, "bronze/transactions/ingest_date=2026-07-01")
    assert len(keys) == 2               # NOT 4


@mock_aws
def test_missing_source_raises_ingestion_error(tmp_path):
    boto3.client("s3").create_bucket(
        Bucket=BUCKET, CreateBucketConfiguration={"LocationConstraint": "ap-south-1"}
    )
    with pytest.raises(IngestionError, match="missing"):
        BronzeIngestor(_cfg()).run(tmp_path / "nope", date(2026, 7, 1))
