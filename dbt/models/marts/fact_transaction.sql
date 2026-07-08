-- Grain: ONE ROW PER ON-CHAIN TRANSACTION (say it in one sentence -- Part 4)
-- Joins each tx to the dim_wallet version VALID AT EVENT TIME (not just
-- current) -- the point of keeping SCD2 history at all.
select
    t.tx_id,
    t.wallet_id,
    d.risk_tier as wallet_risk_tier_at_event,
    t.counterparty_id,
    t.event_ts,
    t.value,
    t.asset
from {{ ref('stg_transactions') }} t
left join {{ source('gold', 'dim_wallet') }} d
    on  d.wallet_id = t.wallet_id
    and t.event_ts >= d.valid_from
    and (d.valid_to is null or t.event_ts < d.valid_to)
