-- SCD2 correctness gate (Part 10 SS4.2 "backfill test"):
-- for any wallet, validity ranges must never overlap and there must be
-- at most ONE current row. Any row returned = test FAILURE.

with ordered as (
    select
        wallet_id,
        valid_from,
        valid_to,
        is_current,
        lead(valid_from) over (partition by wallet_id order by valid_from) as next_from
    from {{ source('gold', 'dim_wallet') }}
),

overlaps as (
    select wallet_id, 'overlapping_range' as breach
    from ordered
    where valid_to is not null and next_from is not null and valid_to > next_from
),

multi_current as (
    select wallet_id, 'multiple_current_rows' as breach
    from {{ source('gold', 'dim_wallet') }}
    where is_current
    group by wallet_id
    having count(*) > 1
)

select * from overlaps
union all
select * from multi_current
