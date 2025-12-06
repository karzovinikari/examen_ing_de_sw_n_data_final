select
    customer_id,
    total_amount_completed,
    total_amount_all
from {{ ref('fct_customer_transactions') }}
where total_amount_completed > total_amount_all
