# Fraud-Signal Lakehouse

Production-grade batch + streaming lakehouse on AWS for on-chain fraud
signals -- the data-engineering backbone of my agentic-AI blockchain
fraud-detection research. Built stage-by-stage per the Part 10 playbook,
with a testing gate at every stage.

## Status
- [x] Stage 0 -- Foundations (this skeleton, logging/exceptions, CI, local stack)
- [ ] Stage 1 -- Batch core (bronze->silver->gold, SCD2, dbt, GX, Airflow)
- [ ] Stage 2 -- Streaming (Kafka -> windowed features -> exactly-once Iceberg)
- [ ] Stage 3 -- Hardening (full IaC, CI/CD gates, observability, governance)
- [ ] Stage 4 -- Differentiator (evaluated anomaly model + RAG fraud analyst)

## Quickstart
```bash
make install    # pip install -r requirements.txt (includes -e . editable install)
make test       # unit gates (logger, exceptions, config)
make up         # local Kafka (KRaft) + MinIO -- dev costs nothing
python skeleton.py   # regenerate any missing structure
```

## Layout
```
src/fraud_lakehouse/
  common/      logger.py exceptions.py config.py   <- used by EVERY module
  ingestion/   batch_ingest.py                     <- bronze landing
  transforms/  silver_transform.py scd2_merge.py   <- Spark + SCD2 MERGE
  streaming/   producer.py stream_job.py           <- chaos producer + stream
  quality/     gx_runner.py                        <- WAP audit gate
  utils/       spark_session.py s3_utils.py
dags/          Airflow DAG (ingest->audit->silver->dbt->publish)
dbt/           staging + marts (fact_transaction, dim_wallet SCD2)
infra/         Terraform (S3 lake, Glue DBs; Stage 3 adds the rest)
tests/         unit / integration / e2e chaos gates
```

## Conventions
- Every module: `logger = get_logger(__name__)`; every failure re-raised as
  a typed `LakehouseError` subclass with origin file/line captured.
- Idempotency everywhere: overwrite-partition or MERGE, never blind append.
- Nothing merges with a red CI; nothing publishes with a failed audit (WAP).
