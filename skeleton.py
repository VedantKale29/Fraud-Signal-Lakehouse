"""
skeleton.py -- regenerate the full project structure with one command:

    python skeleton.py

Creates every directory and file in FILE_LIST if missing (never overwrites
existing work -- safe to re-run any time). Every package directory gets an
__init__.py automatically. This is the industry template.py pattern: the
repo's structure is code, not tribal knowledge.
"""

from __future__ import annotations

import logging
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)-8s skeleton - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("skeleton")

PROJECT = "fraud_lakehouse"

FILE_LIST = [
    # -- source package --------------------------------------------------
    f"src/{PROJECT}/__init__.py",
    f"src/{PROJECT}/common/__init__.py",
    f"src/{PROJECT}/common/logger.py",
    f"src/{PROJECT}/common/exceptions.py",
    f"src/{PROJECT}/common/config.py",
    f"src/{PROJECT}/common/metrics.py",
    f"src/{PROJECT}/common/alerts.py",
    f"src/{PROJECT}/ingestion/__init__.py",
    f"src/{PROJECT}/ingestion/batch_ingest.py",
    f"src/{PROJECT}/ingestion/elliptic_loader.py",
    f"src/{PROJECT}/transforms/__init__.py",
    f"src/{PROJECT}/transforms/silver_transform.py",
    f"src/{PROJECT}/transforms/scd2_merge.py",
    f"src/{PROJECT}/streaming/__init__.py",
    f"src/{PROJECT}/streaming/producer.py",
    f"src/{PROJECT}/streaming/stream_job.py",
    f"src/{PROJECT}/quality/__init__.py",
    f"src/{PROJECT}/quality/gx_runner.py",
    f"src/{PROJECT}/utils/__init__.py",
    f"src/{PROJECT}/utils/spark_session.py",
    f"src/{PROJECT}/utils/s3_utils.py",
    f"src/{PROJECT}/ml/__init__.py",
    f"src/{PROJECT}/ml/features.py",
    f"src/{PROJECT}/ml/train.py",
    f"src/{PROJECT}/ml/score.py",
    f"src/{PROJECT}/agent/__init__.py",
    f"src/{PROJECT}/agent/retriever.py",
    f"src/{PROJECT}/agent/analyst.py",
    "dashboard/app.py",
    # -- orchestration ----------------------------------------------------
    "dags/fraud_lakehouse_dag.py",
    # -- dbt ----------------------------------------------------------------
    "dbt/dbt_project.yml",
    "dbt/profiles.yml.example",
    "dbt/tests/assert_scd2_no_overlap.sql",
    "dbt/models/staging/sources.yml",
    "dbt/models/staging/stg_transactions.sql",
    "dbt/models/marts/fact_transaction.sql",
    "dbt/models/marts/schema.yml",
    # -- infrastructure -----------------------------------------------------
    "infra/terraform/backend.tf",
    "infra/terraform/variables.tf",
    "infra/terraform/main.tf",
    "infra/terraform/outputs.tf",
    "infra/terraform/emr_serverless.tf",
    "infra/terraform/iam.tf",
    "infra/terraform/msk.tf",
    "infra/terraform/lakeformation.tf",
    "infra/terraform/monitoring.tf",
    # -- config / docs / ops ------------------------------------------------
    "configs/config.yaml",
    "docs/data_contract.md",
    "docs/architecture.md",
    "docs/runbook.md",
    "docs/setup_windows.md",
    "scripts/local_up.sh",
    "docker-compose.yml",
    # -- tests ----------------------------------------------------------------
    "tests/__init__.py",
    "tests/conftest.py",
    "tests/unit/__init__.py",
    "tests/unit/test_logger.py",
    "tests/unit/test_exceptions.py",
    "tests/unit/test_config.py",
    "tests/unit/test_silver_transform.py",
    "tests/unit/test_scd2.py",
    "tests/unit/test_gx_runner.py",
    "tests/unit/test_batch_ingest.py",
    "tests/unit/test_elliptic_loader.py",
    "tests/unit/test_stream_job.py",
    "tests/unit/test_producer.py",
    "tests/unit/test_metrics_alerts.py",
    "tests/unit/test_ml.py",
    "tests/unit/test_agent.py",
    "tests/integration/__init__.py",
    "tests/integration/test_stream_kafka.py",
    "tests/e2e/__init__.py",
    "tests/e2e/README.md",
    # -- repo plumbing ---------------------------------------------------------
    ".github/workflows/ci.yml",
    ".github/workflows/cd.yml",
    "tests/e2e/smoke_test.py",
    ".gitignore",
    ".pre-commit-config.yaml",
    "requirements.txt",
    "pyproject.toml",
    "setup.py",
    "Makefile",
    "tasks.ps1",
    "README.md",
]


def create(paths: list[str]) -> None:
    made_dirs, made_files, skipped = 0, 0, 0
    for raw in paths:
        path = Path(raw)
        if path.parent != Path("."):
            if not path.parent.exists():
                path.parent.mkdir(parents=True, exist_ok=True)
                made_dirs += 1
                logger.info("dir  created : %s", path.parent)
        if path.exists() and path.stat().st_size > 0:
            skipped += 1
            continue
        path.touch(exist_ok=True)
        made_files += 1
        logger.info("file created : %s", path)
    logger.info(
        "done | dirs=%d new-files=%d untouched=%d", made_dirs, made_files, skipped
    )


if __name__ == "__main__":
    create(FILE_LIST)
