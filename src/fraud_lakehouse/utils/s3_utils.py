"""Thin, testable wrappers around boto3 S3 calls used across the pipeline."""

from __future__ import annotations

from pathlib import Path

from fraud_lakehouse.common.exceptions import IngestionError
from fraud_lakehouse.common.logger import get_logger

logger = get_logger(__name__)


def upload_file(local_path: Path, bucket: str, key: str) -> str:
    """Upload one file to s3://bucket/key. Returns the s3 URI."""
    try:
        import boto3  # lazy import

        s3 = boto3.client("s3")
        s3.upload_file(str(local_path), bucket, key)
        uri = f"s3://{bucket}/{key}"
        logger.info("Uploaded %s -> %s", local_path, uri)
        return uri
    except Exception as e:
        logger.error("S3 upload failed for %s", local_path, exc_info=True)
        raise IngestionError(f"S3 upload failed for {local_path}", e) from e


def object_exists(bucket: str, key: str) -> bool:
    """HEAD an object; used by idempotency checks before overwriting."""
    try:
        import boto3
        from botocore.exceptions import ClientError

        s3 = boto3.client("s3")
        try:
            s3.head_object(Bucket=bucket, Key=key)
            return True
        except ClientError as ce:
            if ce.response["Error"]["Code"] in ("404", "NoSuchKey", "NotFound"):
                return False
            raise
    except Exception as e:
        logger.error("S3 head_object failed for s3://%s/%s", bucket, key, exc_info=True)
        raise IngestionError(f"S3 head_object failed for s3://{bucket}/{key}", e) from e
