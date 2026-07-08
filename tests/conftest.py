"""Shared fixtures. LOG_TO_FILE=0 keeps CI containers clean.

CRITICAL (especially on Windows): pin Spark's worker Python to THIS
interpreter. Otherwise Spark picks whatever `python` is on PATH, and a
3.10-driver/3.11-worker mismatch kills every UDF/window test with
[PYTHON_VERSION_MISMATCH] buried under walls of Connection-reset noise.
Must run BEFORE any SparkSession is created -- hence module level, here.
"""

import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
# Bind everything to loopback: avoids Windows firewall/hostname-resolution
# games when the JVM and Python workers talk to each other.
os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
os.environ.setdefault("LOG_TO_FILE", "0")

import pytest


@pytest.fixture(scope="session")
def spark():
    """One local SparkSession for all unit tests (JVM startup is the slow
    part -- share it). Tiny shuffle partitions keep tests fast."""
    from pyspark.sql import SparkSession

    s = (
        SparkSession.builder.master("local[2]")
        .appName("fsl-unit-tests")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield s
    s.stop()
