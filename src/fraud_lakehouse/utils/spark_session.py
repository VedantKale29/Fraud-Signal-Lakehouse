"""Single place that builds a SparkSession with Iceberg + Glue wired in."""

from __future__ import annotations

from fraud_lakehouse.common.config import AppConfig
from fraud_lakehouse.common.exceptions import TransformError
from fraud_lakehouse.common.logger import get_logger

logger = get_logger(__name__)


def get_spark(cfg: AppConfig, local: bool = False):
    """Return a SparkSession configured for Iceberg (Glue catalog or local).

    Imported lazily so the rest of the package works without pyspark
    installed (e.g. the Kafka producer container).
    """
    try:
        import os
        import sys

        # Pin worker Python to this exact interpreter (prevents
        # PYTHON_VERSION_MISMATCH when multiple Pythons are installed).
        os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
        os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

        from pyspark.sql import SparkSession  # lazy import by design

        builder = (
            SparkSession.builder.appName(cfg.spark.app_name)
            .config("spark.sql.shuffle.partitions", cfg.spark.shuffle_partitions)
            .config(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            )
            .config(
                f"spark.sql.catalog.{cfg.spark.catalog_name}",
                "org.apache.iceberg.spark.SparkCatalog",
            )
        )
        if local:
            os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
            builder = (
                builder.master("local[*]")
                .config("spark.driver.host", "127.0.0.1")
                .config("spark.driver.bindAddress", "127.0.0.1")
                .config(f"spark.sql.catalog.{cfg.spark.catalog_name}.type", "hadoop")
                .config(
                    f"spark.sql.catalog.{cfg.spark.catalog_name}.warehouse",
                    "file:///tmp/fsl_warehouse",
                )
            )
        else:
            builder = builder.config(
                f"spark.sql.catalog.{cfg.spark.catalog_name}.catalog-impl",
                "org.apache.iceberg.aws.glue.GlueCatalog",
            ).config(
                f"spark.sql.catalog.{cfg.spark.catalog_name}.warehouse",
                cfg.spark.warehouse,
            )
        spark = builder.getOrCreate()
        logger.info("SparkSession started (local=%s, app=%s)", local, cfg.spark.app_name)
        return spark
    except Exception as e:
        logger.error("Could not start SparkSession", exc_info=True)
        raise TransformError("Could not start SparkSession", e) from e
