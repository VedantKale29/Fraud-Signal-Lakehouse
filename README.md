# Fraud-Signal Lakehouse

**A production-grade batch + streaming lakehouse on AWS for on-chain fraud
signals — the data-engineering backbone of my agentic-AI blockchain
fraud-detection research.**

Apache Iceberg tables on S3 fed by two paths — daily batch (Spark on EMR
Serverless) and real-time Kafka streams (Structured Streaming, verified
exactly-once) — modelled into a star-schema fraud mart with SCD Type 2
wallet history, transformed by dbt, gated by data-quality audits, fully
Terraform-provisioned and CI/CD-deployed. On top: an anomaly-scoring layer
evaluated on the labelled Elliptic dataset and a RAG "fraud analyst" agent
that explains any alert from lakehouse evidence.

**Why it exists:** my research area is agentic-AI for blockchain fraud
detection. Research needs trustworthy features; this platform is that
trust, engineered — every claim below is backed by an automated test.

---

## Architecture

```
 INGEST                PROCESS                 MODEL & STORE           SERVE
 ------                -------                 -------------           -----
 on-chain events  -->  Spark Structured   -->  Iceberg lakehouse  -->  Athena SQL
   Kafka / MSK         Streaming               bronze/silver/gold      fraud-ops
                       window + watermark      star schema             dashboard
 batch tx dumps   -->  Spark on EMR       -->  SCD2 dims via      -->  anomaly
   S3 raw              Serverless + dbt        two-pass MERGE          alerts
                                                                  -->  RAG fraud
                                                                       analyst
 ---------------------------- CROSS-CUTTING -----------------------------------
 Airflow orchestration (WAP DAG)  |  dbt tests + native GX-style audit gates
 Terraform IaC (full estate)      |  GitHub Actions CI/CD (OIDC, no keys)
 CloudWatch metrics + alarms      |  Lake Formation column masking | runbook
```

## What makes it production-grade (not a tutorial pipeline)

| Claim | Proof |
|---|---|
| **Idempotent everywhere** — retries/backfills can never duplicate data | delete-prefix-then-write bronze (moto test: run twice -> same 2 objects); Iceberg `overwritePartitions`; MERGE-only writes |
| **SCD Type 2 done correctly** | pure-function spec + two-pass Iceberg MERGE; gate: wallet changes tier twice -> exactly 3 rows, contiguous ranges, one `is_current`; dbt singular test for range overlaps |
| **Exactly-once streaming, proven not claimed** | replayable Kafka + S3 checkpoint + `foreachBatch` MERGE on (wallet, window); chaos producer injects late/duplicate events on demand |
| **Bad data can't reach users** | Write-Audit-Publish: single-pass audit (schema, uniqueness, negatives, future timestamps, volume bounds) halts the DAG before silver; bad rows quarantined, never dropped |
| **Observable & governed** | custom CloudWatch metrics with `treat_missing_data=breaching` alarms, SNS failure callbacks with runbook links, OIDC-based CD, Lake Formation column masking for analyst roles |
| **Typed failures, diagnosable at 3am** | custom exception hierarchy auto-captures origin file/line + root cause; central rotating-file logger in every module |

**Test suite: 50 unit gates** (pytest + chispa + moto, real local Spark)
plus integration gates against dockerised Kafka and a post-deploy smoke
test wired into CD. Every stage of the build had a named testing gate that
blocked progression.

## Tech stack (pinned, with reasons)

Spark 3.5.x (matches EMR 7.x / Glue 5.0 runtimes) · Apache Iceberg ·
Kafka / MSK Serverless · dbt-core on Athena · Airflow · Terraform ·
GitHub Actions (OIDC) · S3 + Glue Catalog + Lake Formation · CloudWatch ·
Python 3.10/3.11 · pytest / chispa / moto

## Data

- **Elliptic dataset** (labelled Bitcoin tx graph) — real temporal ordering
  and illicit/licit labels preserved; anonymised gaps (amounts, wallets)
  synthesised deterministically with documented rules
- **Synthetic chaos producer** — replayable event stream with controllable
  event-time lag, duplicate rate, and burst factor, so streaming
  correctness is tested against manufactured chaos

## Build roadmap (stage-gated)

- [x] **Stage 0 — Foundations**: repo skeleton (`skeleton.py` regenerates it), logging/exceptions, CI, dockerised dev stack
- [x] **Stage 1 — Batch core**: bronze->silver->gold medallion, star schema + SCD2, dbt models + tests, WAP audit, Airflow DAG *(unit gates green; AWS demo run pending)*
- [x] **Stage 2 — Streaming**: chaos producer, windowed velocity features, exactly-once Iceberg sink *(unit gates green; Kafka integration gates run locally)*
- [x] **Stage 3 — Hardening**: full Terraform estate (EMR/MSK/LF/monitoring), OIDC CD + smoke test, metrics/alerts, runbook *(drills on real AWS pending)*
- [x] **Stage 4 — Differentiator**: fraud model (time-split PR-AUC + precision@k, MLflow), guardrailed RAG fraud-analyst (deterministic evidence retrieval, masked fields never surface), Streamlit ops dashboard *(gates green; Elliptic full run + Bedrock demo pending)*

## Getting started

```powershell
# Windows (full guide + error decoder: docs/setup_windows.md)
.\tasks.ps1 install
.\tasks.ps1 test        # 40 passed
.\tasks.ps1 up          # local Kafka + MinIO (free dev)
```
```bash
# Linux / WSL2 / CI
make install && make test && make up
```

## Repo map

```
src/fraud_lakehouse/   common/ ingestion/ transforms/ streaming/ quality/ utils/
dags/                  Airflow WAP DAG
dbt/                   staging + marts + SCD2 singular test
infra/terraform/       full AWS estate, destroy-safe
tests/                 unit / integration / e2e chaos gates
docs/                  data contract · runbook · Windows setup · architecture
skeleton.py            regenerates the entire structure, idempotent
```

## Docs worth reading

[Data contract](docs/data_contract.md) · [Runbook](docs/runbook.md) ·
[Windows setup](docs/setup_windows.md) · [Architecture](docs/architecture.md)

---

*Vedant Kale — Computer Engineering, SFIT · research: agentic-AI for
blockchain fraud detection*
