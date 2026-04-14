with source_data as (
    select *
    from {{ source('ecommerce_seed', 'returns') }}
)

select
    cast(return_id as integer) as return_id,
    cast(order_item_id as integer) as order_item_id,

    cast(return_request_date as timestamp) as return_request_date,
    cast(return_reason as varchar) as return_reason,
    cast(return_status as varchar) as return_status,

    cast(refund_amount as numeric(10, 2)) as refund_amount,

    
    cast(updated_at as timestamp) as updated_at
from source_data
