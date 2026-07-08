-- Grain: ONE ROW PER ON-CHAIN TRANSACTION (say it in one sentence -- Part 4)
select
    t.tx_id,
    t.wallet_id,
    t.counterparty_id,
    t.event_ts,
    t.value,
    t.asset
from {{ ref('stg_transactions') }} t
