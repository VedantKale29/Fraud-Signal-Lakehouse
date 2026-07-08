"""
Central logging for the Fraud-Signal Lakehouse.

One call, everywhere:

    from fraud_lakehouse.common.logger import get_logger
    logger = get_logger(__name__)

Behaviour
---------
- Console handler (always) — human-readable, level from $LOG_LEVEL (default INFO).
- Rotating file handler -> logs/fraud_lakehouse_<YYYY_MM_DD>.log
  (5 MB x 5 backups) so long batch/stream runs never fill the disk.
- Idempotent: calling get_logger twice never duplicates handlers
  (important inside Airflow workers and Spark executors).
- LOG_TO_FILE=0 disables the file handler (useful in CI and containers
  where stdout is the log sink).
"""

from __future__ import annotations

import logging
import os
from datetime import datetime
from logging.handlers import RotatingFileHandler
from pathlib import Path

_LOG_DIR = Path(os.getenv("LOG_DIR", "logs"))
_FORMAT = (
    "[%(asctime)s] %(levelname)-8s %(name)s " "(%(module)s:%(funcName)s:%(lineno)d) - %(message)s"
)
_DATEFMT = "%Y-%m-%d %H:%M:%S"

_configured_root = False


def _configure_root() -> None:
    """Attach handlers to the package root logger exactly once."""
    global _configured_root
    if _configured_root:
        return

    root = logging.getLogger("fraud_lakehouse")
    root.setLevel(os.getenv("LOG_LEVEL", "INFO").upper())
    formatter = logging.Formatter(_FORMAT, datefmt=_DATEFMT)

    console = logging.StreamHandler()
    console.setFormatter(formatter)
    root.addHandler(console)

    if os.getenv("LOG_TO_FILE", "1") == "1":
        _LOG_DIR.mkdir(parents=True, exist_ok=True)
        logfile = _LOG_DIR / f"fraud_lakehouse_{datetime.now():%Y_%m_%d}.log"
        fileh = RotatingFileHandler(
            logfile, maxBytes=5 * 1024 * 1024, backupCount=5, encoding="utf-8"
        )
        fileh.setFormatter(formatter)
        root.addHandler(fileh)

    root.propagate = False
    _configured_root = True


def get_logger(name: str) -> logging.Logger:
    """Return a child logger under the ``fraud_lakehouse`` namespace.

    ``get_logger(__name__)`` inside src/fraud_lakehouse/... already yields a
    properly namespaced logger; any other name is nested under the package
    root so every module shares the same handlers and format.
    """
    _configure_root()
    if name.startswith("fraud_lakehouse"):
        return logging.getLogger(name)
    return logging.getLogger(f"fraud_lakehouse.{name}")
