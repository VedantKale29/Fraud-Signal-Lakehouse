# Data Contract -- raw on-chain transactions (v1)

The GX suite in `quality/gx_runner.py` is written against THIS file, not vibes.

| Field | Type | Rules |
|---|---|---|
| tx_id | string | required, globally unique (dedupe key) |
| wallet_id | string | required |
| counterparty_id | string | nullable |
| event_ts | timestamp (UTC, ISO-8601) | required, never in the future, event-time field for all windows |
| value | decimal | required, >= 0 (negatives -> quarantine) |
| asset | string | required, accepted: BTC, ETH |

Volumes: ~50k rows/day batch; producer default 20 events/sec streaming.
Uniqueness: tx_id. Late-data tolerance: watermark 15 min (p99 lag ~12 min).
Breach behaviour: SchemaContractError -> DAG halts before silver (WAP).
