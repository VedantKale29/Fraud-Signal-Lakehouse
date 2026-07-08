-- Stage 1: staging view over the silver Iceberg table (source of the mart)
select
    tx_id,
    wallet_id,
    counterparty_id,
    cast(event_ts as timestamp) as event_ts,
    value,
    asset
from {{ source('silver', 'transactions') }}
