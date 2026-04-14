with order_items as (
    select *
    from {{ ref('raw_order_items') }}
)

select
    order_item_id,
    order_id,
    product_id,

    quantity,
    unit_price,
    unit_cost,
    discount_amount as item_discount_amount,

    quantity * unit_price as gross_item_amount,
    (quantity * unit_price) - discount_amount as net_item_amount,
    quantity * unit_cost as item_cost_amount,
    ((quantity * unit_price) - discount_amount) - (quantity * unit_cost) as item_profit_amount,
    
    updated_at as order_item_updated_at
from order_items