"""
Stage 3 -- custom CloudWatch metrics: the pipeline's pulse.

Testing catches KNOWN failure modes; these metrics catch the UNKNOWN ones
(the Part-8 maturity ladder). Every DAG task ends with one line:

    emit_metric("silver_rows_written", silver.count())

The Terraform alarms (monitoring.tf) watch exactly these names -- code and
infra agree on the contract via METRIC_NAMES below.

Fail-open policy: metric emission failing must NEVER fail a pipeline that
just processed data correctly -- log loudly, swallow, move on. (This is a
deliberate exception to our raise-everything rule; observability is a
side-channel, not the payload.)
"""

from __future__ import annotations

import os

from fraud_lakehouse.common.logger import get_logger

logger = get_logger(__name__)

NAMESPACE = "FraudSignalLakehouse"

# The code<->terraform contract. Alarm names in monitoring.tf must match.
METRIC_NAMES = {
    "silver_rows_written",
    "quarantine_rows_written",
    "silver_age_hours",
    "gold_rows_written",
    "stream_batch_rows",
    "dag_task_duration_seconds",
}


def emit_metric(name: str, value: float, unit: str = "Count", env: str | None = None) -> bool:
    """Publish one metric. Returns True on success, False on (logged) failure."""
    if name not in METRIC_NAMES:
        # typo'd metric names would silently never alarm -- fail LOUDLY in dev
        logger.error("unknown metric name %r -- add it to METRIC_NAMES", name)
        return False
    try:
        import boto3

        boto3.client("cloudwatch").put_metric_data(
            Namespace=NAMESPACE,
            MetricData=[
                {
                    "MetricName": name,
                    "Value": float(value),
                    "Unit": unit,
                    "Dimensions": [{"Name": "env", "Value": env or os.getenv("FSL_ENV", "dev")}],
                }
            ],
        )
        logger.info("metric %s=%s emitted", name, value)
        return True
    except Exception:
        logger.error("metric emission failed for %s (fail-open)", name, exc_info=True)
        return False
