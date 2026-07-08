"""THE Stage-1 gate (Part 10 SS4.2): a wallet whose risk tier changes twice
-> exactly 3 dim rows, correct contiguous ranges, exactly one is_current."""

from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from fraud_lakehouse.transforms.scd2_merge import DIM_COLUMNS, apply_scd2_batch

DIM_SCHEMA = StructType(
    [
        StructField("wallet_id", StringType()),
        StructField("risk_tier", StringType()),
        StructField("valid_from", TimestampType()),
        StructField("valid_to", TimestampType()),
        StructField("is_current", BooleanType()),
    ]
)


def _updates(spark, rows):
    return spark.createDataFrame(rows, ["wallet_id", "risk_tier", "snapshot_ts"]).withColumn(
        "snapshot_ts", F.col("snapshot_ts").cast("timestamp")
    )


def _empty_dims(spark):
    return spark.createDataFrame([], DIM_SCHEMA)


def test_two_changes_yield_three_rows_one_current(spark):
    dims = _empty_dims(spark)
    # t0: wallet appears as LOW
    dims = apply_scd2_batch(dims, _updates(spark, [("w1", "LOW", "2026-01-01 00:00:00")]))
    # t1: LOW -> MEDIUM
    dims = apply_scd2_batch(dims, _updates(spark, [("w1", "MEDIUM", "2026-02-01 00:00:00")]))
    # t2: MEDIUM -> HIGH
    dims = apply_scd2_batch(dims, _updates(spark, [("w1", "HIGH", "2026-03-01 00:00:00")]))

    rows = sorted(dims.collect(), key=lambda r: r.valid_from)
    assert len(rows) == 3                                   # exactly 3 versions
    assert [r.risk_tier for r in rows] == ["LOW", "MEDIUM", "HIGH"]
    assert [r.is_current for r in rows] == [False, False, True]  # one current
    # contiguous, non-overlapping ranges:
    assert rows[0].valid_to == rows[1].valid_from
    assert rows[1].valid_to == rows[2].valid_from
    assert rows[2].valid_to is None


def test_unchanged_tier_is_a_noop(spark):
    dims = apply_scd2_batch(
        _empty_dims(spark), _updates(spark, [("w1", "LOW", "2026-01-01 00:00:00")])
    )
    again = apply_scd2_batch(dims, _updates(spark, [("w1", "LOW", "2026-02-01 00:00:00")]))
    assert again.count() == 1                # no new version for no change
    assert again.collect()[0].is_current is True


def test_multiple_updates_in_one_batch_keep_latest(spark):
    dims = apply_scd2_batch(
        _empty_dims(spark),
        _updates(
            spark,
            [
                ("w1", "LOW", "2026-01-01 00:00:00"),
                ("w1", "HIGH", "2026-01-02 00:00:00"),  # later snapshot wins
            ],
        ),
    )
    row = dims.collect()[0]
    assert dims.count() == 1 and row.risk_tier == "HIGH"


def test_untouched_wallets_and_history_survive(spark):
    dims = apply_scd2_batch(
        _empty_dims(spark),
        _updates(
            spark,
            [("w1", "LOW", "2026-01-01 00:00:00"), ("w2", "HIGH", "2026-01-01 00:00:00")],
        ),
    )
    dims = apply_scd2_batch(dims, _updates(spark, [("w1", "MEDIUM", "2026-02-01 00:00:00")]))
    assert dims.count() == 3                                    # w1 x2 + w2 x1
    w2 = dims.filter("wallet_id = 'w2'").collect()[0]
    assert w2.risk_tier == "HIGH" and w2.is_current is True     # untouched
    assert set(dims.columns) == set(DIM_COLUMNS)
