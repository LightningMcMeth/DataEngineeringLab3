{{ config(materialized='incremental', unique_key='order_item_sk', on_schema_change='sync_all_columns') }}

with order_items as (
    select *
    from {{ ref('stg_order_items') }}
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
    {{ build_surrogate_key(['o_itms.order_item_id', 'o_itms.order_id']) }} as order_item_sk,
    o_itms.order_item_id,
    o_itms.order_id,

    orders.customer_id,
    o_itms.product_id,

    orders.order_date,
    orders.order_month,
    orders.order_status,

    o_itms.quantity,
    o_itms.unit_price,
    o_itms.unit_cost,

    o_itms.item_discount_amount,
    o_itms.gross_item_amount,
    o_itms.net_item_amount,
    o_itms.item_cost_amount,
    o_itms.item_profit_amount,
    
    greatest(o_itms.order_item_updated_at, orders.order_updated_at) as record_updated_at
from order_items as o_itms
inner join orders
    on o_itms.order_id = orders.order_id

{% if is_incremental() %}
where greatest(o_itms.order_item_updated_at, orders.order_updated_at) >= (
    select coalesce(max(record_updated_at), timestamp '1900-01-01 00:00:00')
    from {{ this }}
)
{% endif %}
