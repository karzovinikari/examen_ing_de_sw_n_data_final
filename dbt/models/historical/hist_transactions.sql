
with source as (
    select *
    from {{ ref('stg_transactions') }}
)

select
    transaction_id,
    customer_id,
    amount,
    status,
    transaction_ts,
    transaction_date
from source
