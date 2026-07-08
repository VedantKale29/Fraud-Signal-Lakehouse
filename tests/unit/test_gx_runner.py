"""Stage-1 gate: the WAP audit passes clean data and halts on every breach."""

from datetime import datetime, timezone

import pytest

from fraud_lakehouse.common.exceptions import DataQualityError, SchemaContractError
from fraud_lakehouse.quality.gx_runner import audit_dataframe
from fraud_lakehouse.transforms.silver_transform import cast_and_normalize

COLS = ["tx_id", "wallet_id", "counterparty_id", "event_ts", "value", "asset"]
NOW = datetime(2026, 7, 7, tzinfo=timezone.utc)   # injected: deterministic tests


def _df(spark, rows):
    return cast_and_normalize(spark.createDataFrame(rows, COLS))


def test_clean_batch_passes(spark):
    report = audit_dataframe(
        _df(spark, [("t1", "w1", "w2", "2026-07-01 10:00:00", "10", "BTC")]), now_ts=NOW
    )
    assert report.passed and report.row_count == 1


def test_missing_column_raises_schema_contract_error(spark):
    df = spark.createDataFrame([("t1",)], ["tx_id"])
    with pytest.raises(SchemaContractError):
        audit_dataframe(df, now_ts=NOW)


def test_duplicate_tx_id_halts(spark):
    df = _df(
        spark,
        [
            ("t1", "w1", "w2", "2026-07-01 10:00:00", "10", "BTC"),
            ("t1", "w1", "w2", "2026-07-01 11:00:00", "10", "BTC"),
        ],
    )
    with pytest.raises(DataQualityError, match="not unique"):
        audit_dataframe(df, now_ts=NOW)


def test_future_event_ts_halts(spark):
    df = _df(spark, [("t1", "w1", "w2", "2027-01-01 10:00:00", "10", "BTC")])
    with pytest.raises(DataQualityError, match="future"):
        audit_dataframe(df, now_ts=NOW)
