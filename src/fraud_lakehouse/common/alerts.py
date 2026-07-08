"""
Stage 3 -- failure alerting: Airflow task fails -> SNS -> your phone/email.

Wired via default_args["on_failure_callback"] = task_failure_alert.
The SS6.2 failure-drill gate: kill a task, alert arrives < 5 min, and the
runbook (docs/runbook.md) resolves it.

Same fail-open policy as metrics: a broken alert channel must not mask the
original task failure (Airflow already marked it failed).
"""

from __future__ import annotations

import os

from fraud_lakehouse.common.logger import get_logger

logger = get_logger(__name__)


def send_alert(subject: str, message: str, topic_arn: str | None = None) -> bool:
    topic_arn = topic_arn or os.getenv("FSL_ALERT_TOPIC_ARN")
    if not topic_arn:
        logger.error("FSL_ALERT_TOPIC_ARN not set -- alert NOT sent: %s", subject)
        return False
    try:
        import boto3

        boto3.client("sns").publish(
            TopicArn=topic_arn, Subject=subject[:100], Message=message
        )
        logger.info("alert sent: %s", subject)
        return True
    except Exception:
        logger.error("alert send failed (fail-open): %s", subject, exc_info=True)
        return False


def task_failure_alert(context: dict) -> None:
    """Airflow on_failure_callback. Context -> a message a 3am-you can act on."""
    ti = context.get("task_instance")
    subject = f"[FSL] task FAILED: {getattr(ti, 'task_id', '?')}"
    message = (
        f"dag={context.get('dag').dag_id if context.get('dag') else '?'}\n"
        f"task={getattr(ti, 'task_id', '?')}\n"
        f"logical_date={context.get('ds')}\n"
        f"try={getattr(ti, 'try_number', '?')}\n"
        f"log_url={getattr(ti, 'log_url', 'n/a')}\n"
        f"runbook=docs/runbook.md"
    )
    send_alert(subject, message)
