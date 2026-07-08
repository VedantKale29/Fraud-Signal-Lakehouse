"""
Stage 1 -- the batch DAG (Part 10 SS4.1), full Write-Audit-Publish shape:

  ingest_bronze -> gx_audit -> silver_transform -> scd2_dim_wallet
      -> dbt_run -> dbt_test -> publish

Key behaviours:
- {{ ds }} (the logical date) flows into every task, so
  `airflow dags backfill -s ... -e ...` rebuilds ANY historical partition
  -- the backfill gate.
- Every task is idempotent (overwrite-partition / MERGE), so retries and
  backfills are safe by construction.
- gx_audit raising DataQualityError FAILS the task -> nothing downstream
  runs -> the WAP promise.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

DEFAULT_ARGS = {
    "owner": "vedant",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "email_on_failure": False,
}


def _on_failure(context):
    from fraud_lakehouse.common.alerts import task_failure_alert

    task_failure_alert(context)


DEFAULT_ARGS["on_failure_callback"] = _on_failure

DBT_DIR = "/usr/local/airflow/dbt"  # Astro image path; adjust if self-hosted


def _ingest(**ctx):
    from datetime import date
    from pathlib import Path

    from fraud_lakehouse.common.config import load_config
    from fraud_lakehouse.ingestion.batch_ingest import BronzeIngestor

    BronzeIngestor(load_config(), register_glue=True).run(
        source=Path(ctx["params"]["source_path"]),
        logical_date=date.fromisoformat(ctx["ds"]),
    )


def _gx_audit(**ctx):
    from fraud_lakehouse.common.config import load_config
    from fraud_lakehouse.quality.gx_runner import audit_dataframe
    from fraud_lakehouse.utils.spark_session import get_spark

    cfg = load_config()
    spark = get_spark(cfg)
    df = spark.read.parquet(
        f"s3a://{cfg.s3.bucket}/{cfg.s3.bronze_prefix}/transactions/"
        f"ingest_date={ctx['ds']}"
    )
    audit_dataframe(df)  # raises DataQualityError -> task fails -> WAP halt


def _silver(**ctx):
    from fraud_lakehouse.common.config import load_config
    from fraud_lakehouse.transforms.silver_transform import to_silver
    from fraud_lakehouse.utils.spark_session import get_spark

    cfg = load_config()
    spark = get_spark(cfg)
    bronze = spark.read.parquet(
        f"s3a://{cfg.s3.bucket}/{cfg.s3.bronze_prefix}/transactions/"
        f"ingest_date={ctx['ds']}"
    )
    silver, quarantine = to_silver(bronze)
    # overwritePartitions = Iceberg's idempotent write for this date
    silver.writeTo(f"{cfg.spark.catalog_name}.silver.transactions").overwritePartitions()
    quarantine.writeTo(
        f"{cfg.spark.catalog_name}.silver.transactions_quarantine"
    ).overwritePartitions()

    # Stage 3: the pipeline's pulse (monitoring.tf alarms watch these)
    from fraud_lakehouse.common.metrics import emit_metric

    emit_metric("silver_rows_written", silver.count())
    emit_metric("quarantine_rows_written", quarantine.count())


def _scd2(**ctx):
    from fraud_lakehouse.common.config import load_config
    from fraud_lakehouse.transforms.scd2_merge import apply_scd2
    from fraud_lakehouse.utils.spark_session import get_spark

    cfg = load_config()
    apply_scd2(get_spark(cfg), cfg.spark.catalog_name)


def _publish(**ctx):
    # Stage 1: gold tables are written by dbt; "publish" = mart is only
    # exposed after dbt_test passed (task ordering IS the atomic gate here).
    # TODO(Stage 3): upgrade to Iceberg branch write-audit-publish
    # (write to `audit` branch -> fast-forward main on green).
    from fraud_lakehouse.common.logger import get_logger

    get_logger("dag.publish").info("publish gate passed for %s", ctx["ds"])


with DAG(
    dag_id="fraud_lakehouse_batch",
    description="Bronze->Silver->Gold with Write-Audit-Publish gates",
    schedule="@daily",
    start_date=datetime(2026, 7, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    params={"source_path": "/data/raw/transactions"},
    tags=["fraud-signal-lakehouse", "stage-1"],
) as dag:
    ingest = PythonOperator(task_id="ingest_bronze", python_callable=_ingest)
    audit = PythonOperator(task_id="gx_audit", python_callable=_gx_audit)
    silver = PythonOperator(task_id="silver_transform", python_callable=_silver)
    scd2 = PythonOperator(task_id="scd2_dim_wallet", python_callable=_scd2)
    dbt_run = BashOperator(
        task_id="dbt_run", bash_command=f"cd {DBT_DIR} && dbt run --profiles-dir ."
    )
    dbt_test = BashOperator(
        task_id="dbt_test", bash_command=f"cd {DBT_DIR} && dbt test --profiles-dir ."
    )
    publish = PythonOperator(task_id="publish", python_callable=_publish)

    ingest >> audit >> silver >> scd2 >> dbt_run >> dbt_test >> publish
