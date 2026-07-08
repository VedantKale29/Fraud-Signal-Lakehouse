"""Stage-4 agent gates (SS7.2): evidence exact, guardrail refuses,
masked fields NEVER surface, deterministic backend narrates only facts."""

import pandas as pd
import pytest

from fraud_lakehouse.agent.analyst import REFUSAL, FraudAnalystAgent, TemplateBackend
from fraud_lakehouse.agent.retriever import MASKED_FIELDS, EvidenceRetriever


@pytest.fixture
def retriever():
    features = pd.DataFrame(
        [
            {
                "wallet_id": "w-1",
                "tx_count": 42,
                "total_value": 9000.0,
                "counterparty_id": "SECRET-CP",
                "label": 1,
                "first_seen_ts": "2026-01-01",
            }
        ]
    )
    dims = pd.DataFrame(
        [
            {
                "wallet_id": "w-1",
                "risk_tier": "LOW",
                "valid_from": "2026-01-01",
                "valid_to": "2026-02-01",
                "is_current": False,
            },
            {
                "wallet_id": "w-1",
                "risk_tier": "HIGH",
                "valid_from": "2026-02-01",
                "valid_to": None,
                "is_current": True,
            },
        ]
    )
    scores = pd.DataFrame([{"wallet_id": "w-1", "fraud_score": 0.87, "model_version": "gbt-v1"}])
    alerts = pd.DataFrame(
        [
            {
                "wallet_id": "w-1",
                "window_start": "2026-02-03 10:00",
                "tx_count": 15,
                "total_value": 4000.0,
            }
        ]
    )
    return EvidenceRetriever(features, dims, scores, alerts)


def test_evidence_is_exact_and_ordered(retriever):
    ev = retriever.retrieve("w-1")
    assert ev.found
    assert ev.features["tx_count"] == 42
    assert [h["risk_tier"] for h in ev.risk_history] == ["LOW", "HIGH"]  # SCD2 order
    facts = ev.as_facts()
    assert any("fraud_score = 0.870" in f for f in facts)
    assert any("alert:" in f for f in facts)


def test_masked_fields_never_surface(retriever):
    """Governance carries into the AI layer -- the SS7.2 guardrail gate."""
    ev = retriever.retrieve("w-1")
    blob = " ".join(ev.as_facts()) + str(ev.features)
    assert "SECRET-CP" not in blob
    assert all(m not in ev.features for m in MASKED_FIELDS)


def test_unknown_wallet_refuses_not_guesses(retriever):
    agent = FraudAnalystAgent(retriever)
    assert agent.explain("w-does-not-exist") == REFUSAL


def test_answer_contains_only_retrieved_facts(retriever):
    agent = FraudAnalystAgent(retriever, backend=TemplateBackend())
    answer = agent.explain("w-1")
    assert "wallet w-1" in answer
    assert "fraud_score = 0.870" in answer
    assert "HIGH" in answer  # narrates the SCD2 history
    assert "SECRET-CP" not in answer  # masked stays masked, end to end
