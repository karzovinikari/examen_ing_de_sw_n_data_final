
with base as (
    select * from {{ ref('hist_transactions') }}
)

, aggregates as (
    select
        customer_id,
        count(*) as transaction_count,
        sum(case when status = 'completed' then amount else 0 end) as total_amount_completed,
        sum(amount) as total_amount_all
    from base
    group by customer_id
)

select
    customer_id,
    transaction_count,
    total_amount_completed,
    total_amount_all
from aggregates
