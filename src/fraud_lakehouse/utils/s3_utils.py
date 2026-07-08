"""Thin, testable wrappers around boto3 S3. One client factory, endpoint-
aware: set FSL_S3_ENDPOINT=http://localhost:9000 and the SAME code talks
to the docker-compose MinIO instead of AWS -- dev costs nothing."""

from __future__ import annotations

import os
from pathlib import Path

from fraud_lakehouse.common.exceptions import IngestionError
from fraud_lakehouse.common.logger import get_logger

logger = get_logger(__name__)


def s3_client():
    try:
        import boto3

        endpoint = os.getenv("FSL_S3_ENDPOINT")  # MinIO in dev, unset in AWS
        return boto3.client("s3", endpoint_url=endpoint) if endpoint else boto3.client("s3")
    except Exception as e:
        logger.error("boto3 client creation failed", exc_info=True)
        raise IngestionError("boto3 client creation failed", e) from e


def delete_prefix(bucket: str, prefix: str) -> int:
    """Delete every object under prefix. THE idempotency primitive:
    delete-then-write makes a partition upload an overwrite, so re-running
    the same logical date can never double the data."""
    try:
        s3 = s3_client()
        deleted = 0
        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            keys = [{"Key": o["Key"]} for o in page.get("Contents", [])]
            if keys:
                s3.delete_objects(Bucket=bucket, Delete={"Objects": keys})
                deleted += len(keys)
        logger.info("delete_prefix s3://%s/%s -> %d objects", bucket, prefix, deleted)
        return deleted
    except Exception as e:
        logger.error("delete_prefix failed", exc_info=True)
        raise IngestionError(f"delete_prefix failed for s3://{bucket}/{prefix}", e) from e


def upload_file(local_path: Path, bucket: str, key: str) -> str:
    try:
        s3_client().upload_file(str(local_path), bucket, key)
        uri = f"s3://{bucket}/{key}"
        logger.info("uploaded %s -> %s", local_path, uri)
        return uri
    except Exception as e:
        logger.error("upload failed for %s", local_path, exc_info=True)
        raise IngestionError(f"S3 upload failed for {local_path}", e) from e


def list_keys(bucket: str, prefix: str) -> list[str]:
    try:
        s3 = s3_client()
        keys: list[str] = []
        for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
            keys += [o["Key"] for o in page.get("Contents", [])]
        return keys
    except Exception as e:
        raise IngestionError(f"list_keys failed for s3://{bucket}/{prefix}", e) from e
