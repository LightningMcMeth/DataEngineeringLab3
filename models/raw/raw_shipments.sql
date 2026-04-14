with source_data as (
    select *
    from {{ source('ecommerce_seed', 'shipments') }}
)

select
    cast(shipment_id as integer) as shipment_id,
    cast(order_id as integer) as order_id,

    cast(carrier as varchar) as carrier,

    cast(shipped_at as timestamp) as shipped_at,
    cast(estimated_delivery_at as timestamp) as estimated_delivery_at,
    cast(delivered_at as timestamp) as delivered_at,
    
    cast(shipment_status as varchar) as shipment_status,
    cast(shipping_region as varchar) as shipping_region,


    cast(updated_at as timestamp) as updated_at
from source_data