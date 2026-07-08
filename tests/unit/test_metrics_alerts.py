"""Stage-3 gate: metrics land in CloudWatch, alerts land in SNS (moto),
and both FAIL OPEN -- a broken side-channel never fails the pipeline."""

import boto3
from moto import mock_aws

from fraud_lakehouse.common.alerts import send_alert, task_failure_alert
from fraud_lakehouse.common.metrics import NAMESPACE, emit_metric


@mock_aws
def test_emit_metric_lands_in_cloudwatch(monkeypatch):
    monkeypatch.setenv("AWS_DEFAULT_REGION", "ap-south-1")
    assert emit_metric("silver_rows_written", 1234) is True
    stats = boto3.client("cloudwatch", region_name="ap-south-1").list_metrics(Namespace=NAMESPACE)
    names = {m["MetricName"] for m in stats["Metrics"]}
    assert "silver_rows_written" in names


def test_unknown_metric_name_rejected():
    # typo'd names would silently never alarm -- must be caught in dev
    assert emit_metric("silver_rowz_written", 1) is False


def test_emit_metric_fails_open_without_aws(monkeypatch):
    monkeypatch.setenv("AWS_DEFAULT_REGION", "")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "")
    # no mock, no credentials -> boto fails -> function returns False, no raise
    assert emit_metric("silver_rows_written", 1) is False


@mock_aws
def test_alert_publishes_to_sns(monkeypatch):
    monkeypatch.setenv("AWS_DEFAULT_REGION", "ap-south-1")
    topic = boto3.client("sns", region_name="ap-south-1").create_topic(Name="t")
    assert send_alert("subj", "body", topic_arn=topic["TopicArn"]) is True


def test_alert_fails_open_without_topic(monkeypatch):
    monkeypatch.delenv("FSL_ALERT_TOPIC_ARN", raising=False)
    assert send_alert("subj", "body") is False


@mock_aws
def test_failure_callback_builds_actionable_message(monkeypatch):
    monkeypatch.setenv("AWS_DEFAULT_REGION", "ap-south-1")
    topic = boto3.client("sns", region_name="ap-south-1").create_topic(Name="t")
    monkeypatch.setenv("FSL_ALERT_TOPIC_ARN", topic["TopicArn"])

    class FakeTI:
        task_id, try_number, log_url = "silver_transform", 2, "http://airflow/log"

    class FakeDag:
        dag_id = "fraud_lakehouse_batch"

    # must not raise, even with a partial context
    task_failure_alert({"task_instance": FakeTI(), "dag": FakeDag(), "ds": "2026-07-01"})
