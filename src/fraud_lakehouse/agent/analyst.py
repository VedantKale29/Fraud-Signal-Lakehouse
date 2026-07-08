"""
Stage 4 -- the fraud-analyst agent: guardrailed narration of evidence.

THE GUARDRAIL (SS7.2 gate): the agent answers ONLY from retrieved facts.
Unknown wallet -> explicit refusal, never a guess. Masked fields never
reach the prompt (enforced upstream in the retriever). The system prompt
additionally instructs the model to cite facts verbatim, and the local
TemplateBackend is fully deterministic -- which is what makes the agent
TESTABLE without an LLM in the loop.

Backends are pluggable: BedrockBackend in prod, TemplateBackend for
tests/demo/free local runs. Same agent, same guardrails.
"""

from __future__ import annotations

import json

from fraud_lakehouse.agent.retriever import AgentError, EvidenceRetriever
from fraud_lakehouse.common.logger import get_logger

logger = get_logger(__name__)

SYSTEM_PROMPT = (
    "You are a fraud analyst assistant. Answer the question using ONLY the "
    "numbered facts provided. Cite fact numbers like [1]. If the facts are "
    "insufficient, say exactly: 'Insufficient evidence in the lakehouse.' "
    "Never invent wallet attributes."
)

REFUSAL = "No evidence found for this wallet in the lakehouse. I can't speculate about it."


class TemplateBackend:
    """Deterministic local 'LLM': renders evidence into a readable answer.
    Zero cost, zero network -- used by tests and the offline demo."""

    def complete(self, system: str, prompt: str) -> str:
        payload = json.loads(prompt)
        lines = [f"Analysis for wallet {payload['wallet_id']}:"]
        lines += [f"  [{i+1}] {f}" for i, f in enumerate(payload["facts"])]
        lines.append(f"Question: {payload['question']}")
        lines.append("Assessment: based solely on the facts above.")
        return "\n".join(lines)


class BedrockBackend:
    """Prod backend -- Anthropic Claude via AWS Bedrock. Lazy boto3 import."""

    def __init__(
        self, model_id: str = "anthropic.claude-3-5-haiku-20241022-v1:0", region: str = "ap-south-1"
    ):
        self.model_id, self.region = model_id, region

    def complete(self, system: str, prompt: str) -> str:
        try:
            import boto3

            rt = boto3.client("bedrock-runtime", region_name=self.region)
            resp = rt.converse(
                modelId=self.model_id,
                system=[{"text": system}],
                messages=[{"role": "user", "content": [{"text": prompt}]}],
                inferenceConfig={"maxTokens": 800, "temperature": 0},
            )
            return resp["output"]["message"]["content"][0]["text"]
        except Exception as e:
            logger.error("bedrock call failed", exc_info=True)
            raise AgentError("Bedrock completion failed", e) from e


class FraudAnalystAgent:
    def __init__(self, retriever: EvidenceRetriever, backend=None):
        self.retriever = retriever
        self.backend = backend or TemplateBackend()

    def explain(self, wallet_id: str, question: str = "Why was this wallet flagged?") -> str:
        """Retrieve -> guardrail -> narrate. The only public entrypoint."""
        try:
            ev = self.retriever.retrieve(wallet_id)
            if not ev.found:
                logger.info("guardrail refusal | wallet=%s", wallet_id)
                return REFUSAL
            prompt = json.dumps(
                {"wallet_id": wallet_id, "question": question, "facts": ev.as_facts()}
            )
            answer = self.backend.complete(SYSTEM_PROMPT, prompt)
            logger.info("agent answered | wallet=%s facts=%d", wallet_id, len(ev.as_facts()))
            return answer
        except AgentError:
            raise
        except Exception as e:
            logger.error("agent.explain failed", exc_info=True)
            raise AgentError(f"agent failed for wallet {wallet_id}", e) from e
