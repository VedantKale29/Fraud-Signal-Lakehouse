"""Stage-4 ML gates (SS7.2): feature math, ZERO temporal leakage,
imbalance-aware metrics, scoring shape."""

import numpy as np
import pandas as pd
import pytest

from fraud_lakehouse.ml.features import build_wallet_features
from fraud_lakehouse.ml.score import score_wallets
from fraud_lakehouse.ml.train import (
    ModelTrainingError,
    precision_at_k,
    time_split,
    train_and_evaluate,
)


def test_wallet_feature_math(spark):
    tx = spark.createDataFrame(
        [
            ("t1", "w1", "c1", "2026-01-01 10:00:00", 10.0),
            ("t2", "w1", "c2", "2026-01-01 11:00:00", 30.0),
            ("t3", "w1", "c1", "2026-01-02 10:00:00", 20.0),
            ("t4", "w2", "c9", "2026-01-05 10:00:00", 5.0),
        ],
        ["tx_id", "wallet_id", "counterparty_id", "event_ts", "value"],
    ).withColumn("event_ts", __import__("pyspark").sql.functions.col("event_ts").cast("timestamp"))
    labels = spark.createDataFrame(
        [("t1", "ILLICIT"), ("t2", "LICIT"), ("t4", "LICIT")], ["tx_id", "fraud_label"]
    )
    row = {r.wallet_id: r for r in build_wallet_features(tx, labels).collect()}
    w1 = row["w1"]
    assert w1.tx_count == 3
    assert w1.total_value == 60.0
    assert w1.max_value == 30.0
    assert w1.active_days == 2
    assert w1.label == 1  # any illicit tx -> risky wallet
    assert row["w2"].label == 0


def _synthetic_frame(n=400, seed=7):
    """Learnable synthetic wallets: illicit ones move more value faster."""
    rng = np.random.default_rng(seed)
    y = rng.random(n) < 0.15
    df = pd.DataFrame(
        {
            "wallet_id": [f"w-{i}" for i in range(n)],
            "tx_count": rng.poisson(5, n) + y * rng.poisson(30, n),
            "total_value": rng.exponential(500, n) + y * rng.exponential(8000, n),
            "distinct_counterparties": rng.poisson(3, n) + y * rng.poisson(20, n),
            "active_days": rng.integers(1, 30, n),
            "label": y.astype(int),
            "first_seen_ts": pd.date_range("2026-01-01", periods=n, freq="h"),
        }
    )
    df["avg_value"] = df["total_value"] / df["tx_count"].clip(lower=1)
    df["max_value"] = df["total_value"] * 0.6
    return df


def test_time_split_has_zero_leakage():
    """THE gate: every training wallet predates every test wallet."""
    train, test = time_split(_synthetic_frame(), test_fraction=0.3)
    assert train["first_seen_ts"].max() <= test["first_seen_ts"].min()
    assert len(train) + len(test) == 400


def test_time_split_rejects_degenerate_input():
    with pytest.raises(ModelTrainingError, match="degenerate"):
        time_split(_synthetic_frame(n=2), test_fraction=0.0)


def test_precision_at_k_math():
    y = np.array([1, 0, 1, 0, 0])
    s = np.array([0.9, 0.8, 0.7, 0.2, 0.1])
    assert precision_at_k(y, s, 2) == 0.5  # top-2 = [1, 0]
    assert precision_at_k(y, s, 3) == 2 / 3


def test_train_and_evaluate_beats_random_baseline():
    model, report = train_and_evaluate(_synthetic_frame(), k=30)
    assert report.pr_auc > 0.5  # far above ~0.15 base rate
    assert 0 <= report.precision_at_k <= 1
    assert report.n_train > report.n_test


def test_scoring_shape_and_range():
    frame = _synthetic_frame()
    model, _ = train_and_evaluate(frame, k=20)
    scored = score_wallets(model, frame, model_version="gbt-v1")
    assert list(scored.columns) == ["wallet_id", "fraud_score", "model_version", "scored_at"]
    assert scored["fraud_score"].between(0, 1).all()
    assert len(scored) == len(frame)
