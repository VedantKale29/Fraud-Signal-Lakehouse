"""
Stage 2 -- synthetic Kafka producer: chaos on demand.

Replays historical transactions into Kafka with configurable event-time
lag, duplicate rate, and burst factor, so the late-data / duplicate-replay
testing gates (Part 10 SS5.2) have something deterministic to bite on.
"""

from __future__ import annotations

import json
import random
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from fraud_lakehouse.common.config import AppConfig, load_config
from fraud_lakehouse.common.exceptions import StreamingError
from fraud_lakehouse.common.logger import get_logger

logger = get_logger(__name__)


@dataclass
class ChaosProfile:
    """Knobs the streaming tests turn."""

    late_fraction: float = 0.10      # share of events emitted late
    late_minutes_max: int = 20       # how late the stragglers can be
    duplicate_fraction: float = 0.05 # share of events sent twice
    burst_factor: int = 1            # multiply steady rate for burst tests


class TransactionProducer:
    def __init__(self, cfg: AppConfig, chaos: ChaosProfile | None = None):
        self.cfg = cfg
        self.chaos = chaos or ChaosProfile()
        self._producer = None

    def _connect(self):
        try:
            from kafka import KafkaProducer  # lazy import

            self._producer = KafkaProducer(
                bootstrap_servers=self.cfg.kafka.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks="all",
            )
            logger.info("Kafka producer connected -> %s", self.cfg.kafka.bootstrap_servers)
        except Exception as e:
            logger.error("Kafka connect failed", exc_info=True)
            raise StreamingError("Kafka producer connect failed", e) from e

    def make_event(self, seq: int) -> dict:
        """One synthetic on-chain transaction, possibly with injected lag."""
        now = datetime.now(timezone.utc)
        event_ts = now
        if random.random() < self.chaos.late_fraction:
            event_ts = now - timedelta(
                minutes=random.uniform(1, self.chaos.late_minutes_max)
            )
        return {
            "tx_id": f"tx-{seq}",
            "wallet_id": f"w-{random.randint(1, 500)}",
            "counterparty_id": f"w-{random.randint(1, 500)}",
            "value": round(random.expovariate(1 / 250), 2),
            "asset": "BTC",
            "event_ts": event_ts.isoformat(),
            "produced_ts": now.isoformat(),
        }

    def run(self, events_per_sec: float = 20.0, total: int = 1000) -> None:
        if self._producer is None:
            self._connect()
        topic = self.cfg.kafka.transactions_topic
        sent = 0
        try:
            for seq in range(total):
                evt = self.make_event(seq)
                self._producer.send(topic, evt)
                sent += 1
                if random.random() < self.chaos.duplicate_fraction:
                    self._producer.send(topic, evt)  # deliberate duplicate
                    sent += 1
                time.sleep(1.0 / (events_per_sec * self.chaos.burst_factor))
            self._producer.flush()
            logger.info("Producer done | topic=%s sent=%d", topic, sent)
        except Exception as e:
            logger.error("Producer failed after %d sends", sent, exc_info=True)
            raise StreamingError(f"Producer failed after {sent} sends", e) from e


if __name__ == "__main__":
    TransactionProducer(load_config()).run(total=100)
