{{ config(materialized='incremental', unique_key='order_id', on_schema_change='sync_all_columns') }}

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
        count(*) as payment_event_count,
        max(payment_date) as latest_payment_date,
        sum(case when payment_status = 'paid' then payment_amount else 0 end) as total_paid_amount,
        sum(case when payment_status = 'refunded' then payment_amount else 0 end) as total_refunded_amount,
        case
            when max(case when payment_status = 'refunded' then 1 else 0 end) = 1 then 'refunded'
            when max(case when payment_status = 'paid' then 1 else 0 end) = 1 then 'paid'
            when max(case when payment_status = 'pending' then 1 else 0 end) = 1 then 'pending'
            when max(case when payment_status = 'failed' then 1 else 0 end) = 1 then 'failed'
            else 'unpaid'
        end as payment_status,
        max(payment_updated_at) as latest_payment_updated_at
    from {{ ref('stg_payments') }}
    group by 1
),

shipments as (
    select
        order_id,
        count(*) as shipment_event_count,
        count(distinct carrier) as shipment_carrier_count,
        max(shipped_at) as latest_shipped_at,
        max(estimated_delivery_at) as latest_estimated_delivery_at,
        max(delivered_at) as latest_delivered_at,
        max(case when is_delivered_late then 1 else 0 end) as is_delivered_late_flag,
        case
            when count(*) = 0 then 'not_shipped'
            when max(case when shipment_status = 'delivered' then 1 else 0 end) = 1 then 'delivered'
            else 'in_transit'
        end as shipment_status,
        max(shipment_updated_at) as latest_shipment_updated_at
    from {{ ref('stg_shipments') }}
    group by 1
),

final_table as (
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

        coalesce(pay.payment_event_count, 0) as payment_event_count,
        pay.latest_payment_date as payment_date,
        coalesce(pay.payment_status, 'unpaid') as payment_status,
        coalesce(pay.total_paid_amount, 0) as total_paid_amount,
        coalesce(pay.total_refunded_amount, 0) as total_refunded_amount,

        coalesce(ship.shipment_event_count, 0) as shipment_event_count,
        coalesce(ship.shipment_carrier_count, 0) as shipment_carrier_count,
        ship.latest_shipped_at as shipped_at,
        ship.latest_estimated_delivery_at as estimated_delivery_at,
        ship.latest_delivered_at as delivered_at,
        coalesce(ship.shipment_status, 'not_shipped') as shipment_status,
        case when coalesce(ship.is_delivered_late_flag, 0) = 1 then true else false end as is_delivered_late,

        case
            when coalesce(pay.payment_status, 'unpaid') = 'paid'
             and orders.order_status != 'cancelled'
                then (coalesce(o_itms.order_subtotal_amount, 0) + orders.shipping_fee) - orders.order_discount_amount
            else 0
        end as recognized_revenue_amount,

        greatest(
            orders.order_updated_at,
            coalesce(o_itms.latest_order_item_updated_at, timestamp '1901-01-01 00:00:00'),
            coalesce(pay.latest_payment_updated_at, timestamp '1902-01-01 00:00:00'),
            coalesce(ship.latest_shipment_updated_at, timestamp '1903-01-01 00:00:00')
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
from final_table

{% if is_incremental() %}
where record_updated_at >= (
    select coalesce(max(record_updated_at), timestamp '1901-01-01 00:00:00') - interval '7 days'
    from {{ this }}
)
{% endif %}
