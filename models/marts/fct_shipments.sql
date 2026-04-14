{{ config(materialized='incremental', unique_key='shipment_id') }}

with shipments as (
    select *
    from {{ ref('stg_shipments') }}
),

orders as (
    select
        order_id,
        customer_id,
        order_status,
        order_updated_at
    from {{ ref('stg_orders') }}
)

select
    ship.shipment_id,
    ship.order_id,
    orders.customer_id,

    orders.order_status,
    ship.carrier,
    ship.shipping_region,
    ship.shipped_at,

    ship.estimated_delivery_at,
    ship.delivered_at,
    ship.shipment_status as delivery_status,
    case
        when ship.shipped_at is not null and ship.delivered_at is not null
            then date_diff('day', cast(ship.shipped_at as date), cast(ship.delivered_at as date))
        else null
    end as actual_delivery_days,
    s.is_delivered_late,
    greatest(ship.shipment_updated_at, orders.order_updated_at) as record_updated_at

from shipments as ship
inner join orders
    on ship.order_id = orders.order_id
    
{% if is_incremental() %}
where greatest(ship.shipment_updated_at, orders.order_updated_at) >= (
    select coalesce(max(record_updated_at), timestamp '1900-01-01 00:00:00')
    from {{ this }}
)
{% endif %}

