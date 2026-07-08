"""
Stage 4 -- train + evaluate the fraud model. Two non-negotiables:

1. TIME-BASED SPLIT, never random. Fraud drifts; a random split lets the
   model peek at the future and inflates every metric. We split on each
   wallet's first_seen_ts: train on the past, test on the future --
   exactly how the model would be used in production.

2. PR-AUC + precision@k, never accuracy. Elliptic is ~90% licit, so a
   model predicting "all licit" scores 90% accuracy while catching zero
   fraud. PR-AUC measures ranking quality under imbalance; precision@k
   answers the operational question: "if analysts review the top-k
   alerts, how many are real?"

MLflow tracking is fail-open (like metrics.py): a dead tracking server
must never kill a training run that produced a good model.
"""

from __future__ import annotations

from dataclasses import dataclass

from fraud_lakehouse.common.exceptions import LakehouseError
from fraud_lakehouse.common.logger import get_logger
from fraud_lakehouse.ml.features import FEATURE_COLUMNS

logger = get_logger(__name__)


class ModelTrainingError(LakehouseError):
    """Failures in the Stage-4 model layer."""


@dataclass
class EvalReport:
    pr_auc: float
    precision_at_k: float
    k: int
    n_train: int
    n_test: int
    split_ts: str


def time_split(pdf, ts_col: str = "first_seen_ts", test_fraction: float = 0.3):
    """Split a pandas frame past/future on the ts quantile.

    Returns (train, test). GUARANTEE (unit-gated): max(train.ts) <= min(test.ts)
    -- zero temporal leakage.
    """
    try:
        pdf = pdf.sort_values(ts_col).reset_index(drop=True)
        cut = int(len(pdf) * (1 - test_fraction))
        if cut == 0 or cut == len(pdf):
            raise ModelTrainingError(
                f"time_split degenerate: n={len(pdf)}, test_fraction={test_fraction}"
            )
        return pdf.iloc[:cut].copy(), pdf.iloc[cut:].copy()
    except ModelTrainingError:
        raise
    except Exception as e:
        logger.error("time_split failed", exc_info=True)
        raise ModelTrainingError("time_split failed", e) from e


def precision_at_k(y_true, scores, k: int):
    """Of the k highest-scored wallets, what fraction are truly illicit?"""
    import numpy as np

    order = np.argsort(scores)[::-1][:k]
    return float(np.asarray(y_true)[order].mean())


def train_and_evaluate(features_pdf, k: int = 50, mlflow_experiment: str | None = None) -> tuple:
    """Time-split -> gradient boosting -> (model, EvalReport).

    Input: pandas frame with FEATURE_COLUMNS + label + first_seen_ts
    (i.e. build_wallet_features(...).toPandas() with labels attached).
    """
    try:
        from sklearn.ensemble import HistGradientBoostingClassifier
        from sklearn.metrics import average_precision_score

        labelled = features_pdf.dropna(subset=["label"]).copy()
        if labelled["label"].nunique() < 2:
            raise ModelTrainingError("need both classes present to train")

        train, test = time_split(labelled)
        k = min(k, len(test))

        model = HistGradientBoostingClassifier(max_iter=200, random_state=42)
        model.fit(train[FEATURE_COLUMNS], train["label"].astype(int))

        scores = model.predict_proba(test[FEATURE_COLUMNS])[:, 1]
        report = EvalReport(
            pr_auc=float(average_precision_score(test["label"].astype(int), scores)),
            precision_at_k=precision_at_k(test["label"].astype(int).values, scores, k),
            k=k,
            n_train=len(train),
            n_test=len(test),
            split_ts=str(train["first_seen_ts"].max()),
        )
        logger.info(
            "model evaluated | pr_auc=%.4f p@%d=%.4f train=%d test=%d",
            report.pr_auc,
            report.k,
            report.precision_at_k,
            report.n_train,
            report.n_test,
        )
        _log_mlflow(model, report, mlflow_experiment)
        return model, report
    except ModelTrainingError:
        raise
    except Exception as e:
        logger.error("train_and_evaluate failed", exc_info=True)
        raise ModelTrainingError("train_and_evaluate failed", e) from e


def _log_mlflow(model, report: EvalReport, experiment: str | None) -> None:
    """Fail-open tracking: log if MLflow is reachable, warn loudly if not."""
    if experiment is None:
        return
    try:
        import mlflow

        mlflow.set_experiment(experiment)
        with mlflow.start_run():
            mlflow.log_metrics(
                {"pr_auc": report.pr_auc, f"precision_at_{report.k}": report.precision_at_k}
            )
            mlflow.log_params(
                {"n_train": report.n_train, "n_test": report.n_test, "split_ts": report.split_ts}
            )
            mlflow.sklearn.log_model(model, "model")
        logger.info("mlflow run logged to experiment %s", experiment)
    except Exception:
        logger.error("mlflow logging failed (fail-open)", exc_info=True)
