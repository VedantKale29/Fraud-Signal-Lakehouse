"""Stage 4 -- score wallets and hand rows back for the Iceberg MERGE.
Scores are DATA: they land in gold.wallet_scores via the same idempotent
MERGE discipline as everything else (key: wallet_id + model_version)."""

from __future__ import annotations

from datetime import datetime, timezone

from fraud_lakehouse.common.logger import get_logger
from fraud_lakehouse.ml.features import FEATURE_COLUMNS
from fraud_lakehouse.ml.train import ModelTrainingError

logger = get_logger(__name__)


def score_wallets(model, features_pdf, model_version: str):
    """Return a pandas frame: wallet_id, fraud_score, model_version, scored_at."""
    try:
        out = features_pdf[["wallet_id"]].copy()
        out["fraud_score"] = model.predict_proba(features_pdf[FEATURE_COLUMNS])[:, 1]
        out["model_version"] = model_version
        out["scored_at"] = datetime.now(timezone.utc).isoformat()
        logger.info("scored %d wallets with %s", len(out), model_version)
        return out
    except Exception as e:
        logger.error("score_wallets failed", exc_info=True)
        raise ModelTrainingError("score_wallets failed", e) from e
