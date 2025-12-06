
with base as (
    select * from {{ ref('stg_transactions') }}
)

-- TODO: Completar el modelo para que cree la tabla fct_customer_transactions con las metricas en schema.yml.
