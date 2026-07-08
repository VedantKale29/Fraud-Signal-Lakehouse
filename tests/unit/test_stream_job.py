"""Stage-2 unit gates for the PURE stream functions.
(Watermark *dropping* is runtime behaviour -> integration test with Kafka;
here we pin parsing, window math, and the MERGE-key dedupe.)"""

import json

from fraud_lakehouse.streaming.stream_job import (
    build_features,
    parse_events,
    prepare_batch,
)


def _kafka_like(spark, payloads):
    """Mimic the Kafka source: one binary `value` column."""
    return spark.createDataFrame([(json.dumps(p).encode(),) for p in payloads], ["value"])


def _evt(tx, wallet, ts, value=10.0, cp="w-9"):
    return {
        "tx_id": tx,
        "wallet_id": wallet,
        "counterparty_id": cp,
        "value": value,
        "asset": "BTC",
        "event_ts": ts,
        "produced_ts": ts,
    }


def test_parse_events_typed_and_corrupt_flagged(spark):
    good = _evt("t1", "w1", "2026-07-01T10:00:00")
    df = spark.createDataFrame([(json.dumps(good).encode(),), (b"not-json{",)], ["value"])
    out = parse_events(df)
    rows = {bool(r._corrupt): r for r in out.collect()}
    assert rows[False].tx_id == "t1"
    assert rows[False].event_ts.hour == 10  # real timestamp
    assert rows[True]._raw.startswith("not-json")  # dead-letter keeps payload


def test_build_features_window_math(spark):
    """3 tx for w1 inside one 10-min window, 1 in the next -> 2 rows with
    the exact counts/sums. Every number explainable -- the SS5.2 standard."""
    df = parse_events(
        _kafka_like(
            spark,
            [
                _evt("t1", "w1", "2026-07-01T10:01:00", 10.0),
                _evt("t2", "w1", "2026-07-01T10:04:00", 20.0, cp="w-8"),
                _evt("t3", "w1", "2026-07-01T10:09:59", 30.0),
                _evt("t4", "w1", "2026-07-01T10:11:00", 5.0),
            ],
        )
    )
    feats = {r.window_start.minute: r for r in build_features(df).collect()}
    assert feats[0].tx_count == 3
    assert float(feats[0].total_value) == 60.0
    assert float(feats[0].max_single_value) == 30.0
    assert feats[10].tx_count == 1


def test_prepare_batch_collapses_merge_key_dupes(spark):
    df = spark.createDataFrame(
        [
            ("w1", "2026-07-01 10:00:00", 3, 60.0),
            ("w1", "2026-07-01 10:00:00", 5, 90.0),  # same MERGE key -> keep richer
            ("w2", "2026-07-01 10:00:00", 1, 10.0),
        ],
        ["wallet_id", "window_start", "tx_count", "total_value"],
    )
    out = prepare_batch(df)
    assert out.count() == 2
    assert out.filter("wallet_id='w1'").collect()[0].tx_count == 5
