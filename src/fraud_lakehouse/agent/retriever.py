"""
Stage 4 -- evidence retrieval for the fraud analyst.

DESIGN CHOICE (defend this in interviews): for "why was wallet X flagged?"
retrieval is DETERMINISTIC -- that wallet's feature row, its SCD2 risk-tier
history, its score, its windowed alerts. Exact lookups over the governed
mart, not vector similarity. The LLM narrates facts; it never finds them.
Semantic search earns its place later for cross-case queries ("similar
past cases"), as an additive index -- never as the source of truth.

GOVERNANCE CARRIES INTO THE AI LAYER: the retriever exposes only the
fields an analyst may see (MASKED_FIELDS never leave this module), so the
agent cannot leak what Lake Formation hides -- the SS7.2 guardrail gate.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from fraud_lakehouse.common.exceptions import LakehouseError
from fraud_lakehouse.common.logger import get_logger

logger = get_logger(__name__)

MASKED_FIELDS = {"counterparty_id"}  # mirrors infra/terraform/lakeformation.tf


class AgentError(LakehouseError):
    """Failures in the Stage-4 agent layer."""


@dataclass
class WalletEvidence:
    wallet_id: str
    found: bool
    features: dict = field(default_factory=dict)
    risk_history: list = field(default_factory=list)  # SCD2 rows, oldest first
    score: dict = field(default_factory=dict)
    alerts: list = field(default_factory=list)

    def as_facts(self) -> list[str]:
        """Flatten to citable fact strings -- the ONLY things the LLM sees."""
        facts = []
        for k, v in self.features.items():
            facts.append(f"feature {k} = {v}")
        for h in self.risk_history:
            facts.append(
                f"risk_tier {h['risk_tier']} from {h['valid_from']} to "
                f"{h.get('valid_to') or 'now'}"
            )
        if self.score:
            facts.append(
                f"fraud_score = {self.score['fraud_score']:.3f} "
                f"(model {self.score['model_version']})"
            )
        for a in self.alerts:
            facts.append(
                f"alert: window {a['window_start']} tx_count={a['tx_count']} "
                f"total_value={a['total_value']}"
            )
        return facts


class EvidenceRetriever:
    """Backed by pandas frames locally / Athena or Spark tables in prod --
    same interface either way (constructor injection keeps it testable)."""

    def __init__(self, features_pdf, dim_wallet_pdf, scores_pdf=None, alerts_pdf=None):
        self.features = features_pdf
        self.dims = dim_wallet_pdf
        self.scores = scores_pdf
        self.alerts = alerts_pdf

    def retrieve(self, wallet_id: str) -> WalletEvidence:
        try:
            ev = WalletEvidence(wallet_id=wallet_id, found=False)

            frow = self.features[self.features["wallet_id"] == wallet_id]
            if len(frow):
                ev.found = True
                ev.features = {
                    k: v
                    for k, v in frow.iloc[0].to_dict().items()
                    if k not in MASKED_FIELDS | {"wallet_id", "label", "first_seen_ts"}
                }

            drows = self.dims[self.dims["wallet_id"] == wallet_id]
            if len(drows):
                ev.found = True
                ev.risk_history = (
                    drows.sort_values("valid_from")
                    .drop(columns=[c for c in MASKED_FIELDS if c in drows], errors="ignore")
                    .to_dict("records")
                )

            if self.scores is not None:
                srow = self.scores[self.scores["wallet_id"] == wallet_id]
                if len(srow):
                    ev.score = srow.iloc[0].to_dict()

            if self.alerts is not None:
                arows = self.alerts[self.alerts["wallet_id"] == wallet_id]
                ev.alerts = arows.to_dict("records")

            logger.info(
                "evidence retrieved | wallet=%s found=%s facts=%d",
                wallet_id,
                ev.found,
                len(ev.as_facts()),
            )
            return ev
        except Exception as e:
            logger.error("retrieve failed for %s", wallet_id, exc_info=True)
            raise AgentError(f"evidence retrieval failed for {wallet_id}", e) from e
