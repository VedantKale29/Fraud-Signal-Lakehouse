"""Stage 4 -- fraud-ops dashboard (Streamlit). Run:  streamlit run dashboard/app.py

Offline demo mode: synthesises the mart with the SAME functions the
pipeline uses (train_and_evaluate + score_wallets), so what you demo is
what the pipeline computes -- and the agent panel uses the SAME
EvidenceRetriever + guardrails as prod, with the free TemplateBackend."""

import numpy as np
import pandas as pd
import streamlit as st

from fraud_lakehouse.agent.analyst import FraudAnalystAgent
from fraud_lakehouse.agent.retriever import EvidenceRetriever
from fraud_lakehouse.ml.score import score_wallets
from fraud_lakehouse.ml.train import train_and_evaluate


@st.cache_data
def demo_mart(n=600, seed=11):
    rng = np.random.default_rng(seed)
    y = rng.random(n) < 0.12
    f = pd.DataFrame({
        "wallet_id": [f"w-{i}" for i in range(n)],
        "tx_count": rng.poisson(5, n) + y * rng.poisson(28, n),
        "total_value": rng.exponential(500, n) + y * rng.exponential(9000, n),
        "distinct_counterparties": rng.poisson(3, n) + y * rng.poisson(18, n),
        "active_days": rng.integers(1, 30, n),
        "label": y.astype(int),
        "first_seen_ts": pd.date_range("2026-01-01", periods=n, freq="h"),
    })
    f["avg_value"] = f["total_value"] / f["tx_count"].clip(lower=1)
    f["max_value"] = f["total_value"] * 0.6
    model, report = train_and_evaluate(f, k=50)
    scores = score_wallets(model, f, "gbt-v1")
    dims = pd.DataFrame([
        {"wallet_id": w, "risk_tier": "HIGH" if s > 0.7 else "LOW",
         "valid_from": "2026-01-01", "valid_to": None, "is_current": True}
        for w, s in zip(scores["wallet_id"], scores["fraud_score"])
    ])
    return f, scores, dims, report


st.set_page_config(page_title="Fraud-Signal Lakehouse", layout="wide")
st.title("Fraud-Signal Lakehouse — Ops")

features, scores, dims, report = demo_mart()
c1, c2, c3 = st.columns(3)
c1.metric("PR-AUC (time-split)", f"{report.pr_auc:.3f}")
c2.metric(f"Precision@{report.k}", f"{report.precision_at_k:.2f}")
c3.metric("Wallets scored", len(scores))

st.subheader("Top risk wallets")
top = scores.sort_values("fraud_score", ascending=False).head(15)
st.dataframe(top, use_container_width=True)

st.subheader("Ask the fraud analyst")
agent = FraudAnalystAgent(EvidenceRetriever(features, dims, scores))
wallet = st.selectbox("Wallet", top["wallet_id"].tolist())
if st.button("Explain this alert"):
    st.code(agent.explain(wallet))
