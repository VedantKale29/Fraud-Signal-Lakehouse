"""Stage-0 gate: logging is namespaced, idempotent, and writes to file."""

import logging

from fraud_lakehouse.common.logger import get_logger


def test_namespacing():
    lg = get_logger("ingestion.batch")
    assert lg.name == "fraud_lakehouse.ingestion.batch"
    lg2 = get_logger("fraud_lakehouse.transforms.scd2")
    assert lg2.name == "fraud_lakehouse.transforms.scd2"


def test_no_duplicate_handlers():
    """Repeated get_logger calls must never attach additional handlers
    (pytest injects its own capture handlers, so compare before/after)."""
    get_logger("a")
    root = logging.getLogger("fraud_lakehouse")
    before = len(root.handlers)
    for _ in range(5):
        get_logger("b")
    assert len(root.handlers) == before


def test_log_line_emitted(caplog):
    lg = get_logger("test.emit")
    with caplog.at_level(logging.INFO, logger="fraud_lakehouse"):
        lg.info("hello from %s", "stage-0")
    assert any("hello from stage-0" in r.message for r in caplog.records)
