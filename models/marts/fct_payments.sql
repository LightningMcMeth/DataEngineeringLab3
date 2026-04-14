{{ config(materialized='incremental', unique_key='payment_id') }}

with payments as (
    select *
    from {{ ref('stg_payments') }}
),

orders as (
    select
        order_id,
        customer_id,
        order_date,
        order_month,
        order_status,
        order_updated_at
    from {{ ref('stg_orders') }}
)

select
    pay.payment_id,
    pay.order_id,
    orders.customer_id,
    orders.order_date,
    orders.order_month,
    orders.order_status,
    pay.payment_date,
    pay.payment_method,
    pay.payment_status,
    pay.payment_amount,
    greatest(pay.payment_updated_at, orders.order_updated_at) as record_updated_at
from payments as pay
inner join orders
    on pay.order_id = orders.order_id
{% if is_incremental() %}
where greatest(pay.payment_updated_at, orders.order_updated_at) >= (
    select coalesce(max(record_updated_at), timestamp '1900-01-01 00:00:00')
    from {{ this }}
)
{% endif %}

