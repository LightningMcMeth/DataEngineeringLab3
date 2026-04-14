with source_data as (
    select *
    from {{ source('ecommerce_seed', 'payments') }}
)

select
    cast(payment_id as integer) as payment_id,
    cast(order_id as integer) as order_id,

    cast(payment_date as timestamp) as payment_date,
    cast(payment_method as varchar) as payment_method,
    cast(payment_status as varchar) as payment_status,
    
    cast(amount as numeric(10, 2)) as amount,


    cast(updated_at as timestamp) as updated_at
from source_data