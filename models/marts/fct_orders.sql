{{ config(materialized='incremental', unique_key='order_id') }}

with orders as (
    select *
    from {{ ref('stg_orders') }}
),

order_items as (
    select
        order_id,
        sum(quantity) as total_items,
        sum(net_item_amount) as order_subtotal_amount,
        sum(item_profit_amount) as item_profit_amount,
        max(order_item_updated_at) as latest_order_item_updated_at
    from {{ ref('stg_order_items') }}
    group by 1
),

payments as (
    select
        order_id,
        max(payment_id) as payment_id,
        max(payment_date) as latest_payment_date,
        max(payment_method) as payment_method,
        max(payment_status) as payment_status,
        max(payment_amount) as payment_amount,
        max(payment_updated_at) as latest_payment_updated_at
    from {{ ref('stg_payments') }}
    group by 1
),

shipments as (
    select
        order_id,
        max(shipment_id) as shipment_id,
        max(carrier) as carrier,
        max(shipping_region) as shipping_region,
        max(shipment_status) as shipment_status,
        max(case when is_delivered_late then 1 else 0 end) as is_delivered_late_flag,
        max(shipment_updated_at) as latest_shipment_updated_at
    from {{ ref('stg_shipments') }}
    group by 1
),

final as (
    select
        orders.order_id,
        orders.customer_id,

        orders.order_ts,
        orders.order_date,
        orders.order_month,
        
        orders.order_status,
        orders.shipping_method,
        orders.shipping_fee,

        orders.order_discount_amount,
        coalesce(o_itms.total_items, 0) as total_items,
        coalesce(o_itms.order_subtotal_amount, 0) as order_subtotal_amount,
        (coalesce(o_itms.order_subtotal_amount, 0) + orders.shipping_fee) - orders.order_discount_amount as order_total_amount,
        coalesce(o_itms.item_profit_amount, 0) as item_profit_amount,

        pay.payment_id,
        pay.latest_payment_date as payment_date,
        pay.payment_method,
        coalesce(p.payment_status, 'unpaid') as payment_status,
        coalesce(p.payment_amount, 0) as payment_amount,

        ship.shipment_id,
        ship.carrier,
        ship.shipping_region,
        coalesce(ship.shipment_status, 'not_shipped') as shipment_status,

        case when coalesce(ship.is_delivered_late_flag, 0) = 1 then true else false end as is_delivered_late,
        case
            when coalesce(pay.payment_status, 'unpaid') = 'paid' and orders.order_status != 'cancelled'
                then (coalesce(o_itms.order_subtotal_amount, 0) + orders.shipping_fee) - orders.order_discount_amount
            else 0
        end as recognized_revenue_amount,
        greatest(
            orders.order_updated_at,
            coalesce(o_itms.latest_order_item_updated_at, timestamp '1900-01-01 00:00:00'),
            coalesce(pay.latest_payment_updated_at, timestamp '1900-01-01 00:00:00'),
            coalesce(ship.latest_shipment_updated_at, timestamp '1900-01-01 00:00:00')
        ) as record_updated_at
    from orders
    left join order_items as o_itms
        on orders.order_id = o_itms.order_id
    left join payments as pay
        on orders.order_id = pay.order_id
    left join shipments as ship
        on orders.order_id = ship.order_id
)

select *
from final
{% if is_incremental() %}
where record_updated_at >= (
    select coalesce(max(record_updated_at), timestamp '1900-01-01 00:00:00') - interval '7 days'
    from {{ this }}
)
{% endif %}