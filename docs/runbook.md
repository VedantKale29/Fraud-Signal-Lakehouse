# Runbook -- Fraud-Signal Lakehouse

The SS6.2 failure-drill gate: every alert below has a first-response you
can execute half-asleep. Update this file every time an incident teaches
you something new.

## Alert: fsl-silver-freshness (silver_age_hours > 26)
1. Airflow UI -> dag `fraud_lakehouse_batch` -> which task is red?
2. `gx_audit` red -> a DataQualityError fired = the gate WORKED. Check the
   task log for the breach list; inspect quarantine table; fix upstream,
   then re-run the task (idempotent -- safe).
3. `ingest_bronze` red -> source extract missing/late. Confirm with the
   producer of the raw dump; re-run when it lands.
4. Spark task red -> open EMR Serverless job logs; OOM -> bump job config;
   transient -> Airflow already retried twice, re-run manually.

## Alert: fsl-silver-zero-rows
Pipeline "succeeded" but wrote nothing = upstream sent an empty extract or
a filter ate everything. Check bronze row count first, then quarantine
count (all rows quarantined = contract drift -> update contract + suite
deliberately, never loosen silently).

## Alert: budget > 80%
1. `terraform destroy -var env=demo` if the demo env is up (biggest spender).
2. Cost Explorer -> service breakdown. Usual suspects: MSK left up (set
   msk_enabled=false and apply), EMR app without auto-stop, S3 versioning
   bloat on the checkpoint prefix.

## Backfill procedure
`airflow dags backfill fraud_lakehouse_batch -s 2026-07-01 -e 2026-07-05`
Safe by construction: every task overwrites its partition / MERGEs.
After backfill: `dbt test` must be green (SCD2 no-overlap especially).
