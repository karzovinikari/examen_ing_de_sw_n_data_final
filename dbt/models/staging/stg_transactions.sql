{% set clean_dir = var('clean_dir') %}
{% set ds_nodash = var('ds_nodash') %}

with source as (
    select *
    from read_parquet(
        '{{ clean_dir }}/transactions_{{ ds_nodash }}_clean.parquet'
    )
)

select
    cast(transaction_id as varchar) as transaction_id,
    cast(customer_id as varchar) as customer_id,
    cast(amount as double) as amount,
    lower(trim(status)) as status,
    cast(transaction_ts as timestamp) as transaction_ts,
    cast(transaction_date as date) as transaction_date
from source
where transaction_id is not null
  and customer_id is not null
  and transaction_ts is not null
