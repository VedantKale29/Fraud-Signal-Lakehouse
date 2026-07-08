"""Stage-1 unit gate: every silver rule on tiny in-memory frames (SS4.2)."""

from datetime import datetime

from chispa.dataframe_comparer import assert_df_equality

from fraud_lakehouse.transforms.silver_transform import (
    cast_and_normalize,
    deduplicate,
    split_quarantine,
    to_silver,
)

COLS = ["tx_id", "wallet_id", "counterparty_id", "event_ts", "value", "asset"]


def _raw(spark, rows):
    return spark.createDataFrame(rows, COLS)


def test_cast_and_normalize_types_and_case(spark):
    df = _raw(spark, [(" t1 ", "w1", "w2", "2026-07-01 10:00:00", "12.5", " btc ")])
    out = cast_and_normalize(df).collect()[0]
    assert out.tx_id == "t1"  # trimmed
    assert out.asset == "BTC"  # upper + trimmed
    assert isinstance(out.event_ts, datetime)  # real timestamp now
    assert float(out.value) == 12.5  # decimal, not string


def test_quarantine_catches_every_contract_breach(spark):
    df = cast_and_normalize(
        _raw(
            spark,
            [
                ("t1", "w1", "w2", "2026-07-01 10:00:00", "10", "BTC"),  # clean
                (None, "w1", "w2", "2026-07-01 10:00:00", "10", "BTC"),  # null key
                ("t3", "w1", "w2", None, "10", "BTC"),  # null ts
                ("t4", "w1", "w2", "2026-07-01 10:00:00", "-5", "BTC"),  # negative
                ("t5", "w1", "w2", "2026-07-01 10:00:00", "10", "DOGE"),  # bad asset
            ],
        )
    )
    clean, quarantine = split_quarantine(df)
    assert clean.count() == 1
    assert quarantine.count() == 4  # kept, never dropped


def test_deduplicate_keeps_latest_by_event_ts(spark):
    df = cast_and_normalize(
        _raw(
            spark,
            [
                ("t1", "w1", "w2", "2026-07-01 10:00:00", "10", "BTC"),
                ("t1", "w1", "w2", "2026-07-01 11:00:00", "99", "BTC"),  # later wins
                ("t2", "w9", "w2", "2026-07-01 09:00:00", "7", "ETH"),
            ],
        )
    )
    out = deduplicate(df)
    assert out.count() == 2
    assert float(out.filter("tx_id = 't1'").collect()[0].value) == 99.0


def test_to_silver_is_deterministic(spark):
    """Run the chain twice on the same input -> byte-identical output.
    Determinism is the precondition for the pipeline idempotency gate."""
    rows = [
        ("t1", "w1", "w2", "2026-07-01 10:00:00", "10", "BTC"),
        ("t1", "w1", "w2", "2026-07-01 11:00:00", "99", "BTC"),
        ("t2", None, "w2", "2026-07-01 10:00:00", "10", "BTC"),
    ]
    s1, _ = to_silver(_raw(spark, rows))
    s2, _ = to_silver(_raw(spark, rows))
    assert_df_equality(s1, s2, ignore_row_order=True)
