with source_data as (
    select *
    from {{ source('ecommerce_seed', 'order_items') }}
)

select
    cast(order_item_id as integer) as order_item_id,
    cast(order_id as integer) as order_id,
    cast(product_id as integer) as product_id,

    cast(quantity as integer) as quantity,

    cast(unit_price as numeric(10, 2)) as unit_price,
    cast(unit_cost as numeric(10, 2)) as unit_cost,
    cast(discount_amount as numeric(10, 2)) as discount_amount,

    
    cast(updated_at as timestamp) as updated_at
from source_data