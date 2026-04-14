with orders as (
    select *
    from {{ ref('raw_orders') }}
)

select
    order_id,
    customer_id,

    order_date as order_ts,
    cast(order_date as date) as order_date,
    cast(date_trunc('month', order_date) as date) as order_month,
    lower({{ normalize_text('order_status') }}) as order_status,

    lower({{ normalize_text('shipping_method') }}) as shipping_method,
    shipping_fee,

    discount_amount as order_discount_amount,

    case when lower({{ normalize_text('order_status') }}) = 'cancelled' then true else false end as is_cancelled_order,
    case when lower({{ normalize_text('order_status') }}) in ('processing', 'shipped') then true else false end as is_open_order,
    
    updated_at as order_updated_at
from orders