with source_data as (
    select *
    from {{ source('ecommerce_seed', 'orders') }}
)

select
    cast(order_id as integer) as order_id,
    cast(customer_id as integer) as customer_id,

    cast(order_date as timestamp) as order_date,

    cast(order_status as varchar) as order_status,
    cast(shipping_method as varchar) as shipping_method,
    cast(shipping_fee as numeric(10, 2)) as shipping_fee,

    cast(discount_amount as numeric(10, 2)) as discount_amount,

    
    cast(updated_at as timestamp) as updated_at
from source_data