"""
Stage 1 -- the single batch DAG (Part 10 SS4.1):

ingest -> gx_audit -> silver -> dbt_run -> dbt_test -> publish

Idempotent tasks (overwrite-partition / MERGE, never blind append),
retries with exponential backoff, failure alerts. Accepts a logical date
so any historical partition can be rebuilt (the backfill gate).
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

DEFAULT_ARGS = {
    "owner": "vedant",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "email_on_failure": False,  # TODO(Stage 3): Slack/SNS alert callback
}


def _ingest(**ctx):
    from datetime import date

    from fraud_lakehouse.common.config import load_config
    from fraud_lakehouse.ingestion.batch_ingest import BronzeIngestor

    logical_date = ctx["ds"]
    BronzeIngestor(load_config()).run(
        source=ctx["params"]["source_path"],
        logical_date=date.fromisoformat(logical_date),
    )


def _gx_audit(**ctx):
    # TODO(Stage 1): load bronze batch into Spark, call quality.gx_runner.audit_dataframe
    raise NotImplementedError("Stage 1 build task")


with DAG(
    dag_id="fraud_lakehouse_batch",
    description="Bronze->Silver->Gold with Write-Audit-Publish gates",
    schedule="@daily",
    start_date=datetime(2026, 7, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    params={"source_path": "/data/raw/transactions.parquet"},
    tags=["fraud-signal-lakehouse", "stage-1"],
) as dag:
    ingest = PythonOperator(task_id="ingest_bronze", python_callable=_ingest)
    audit = PythonOperator(task_id="gx_audit", python_callable=_gx_audit)
    # TODO(Stage 1): silver spark-submit, dbt run, dbt test, publish (atomic swap)
    ingest >> audit
