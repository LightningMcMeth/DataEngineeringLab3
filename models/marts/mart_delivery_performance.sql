with shipments as (
    select *
    from {{ ref('fct_shipments') }}
),

shipment_rollup as (
    select
        carrier,
        shipping_region,

        count(*) as shipment_count,
        
        sum(case when delivery_status = 'delivered' then 1 else 0 end) as delivered_shipments,
        sum(case when is_delivered_late then 1 else 0 end) as late_shipments,
        avg(case when delivery_status = 'delivered' then actual_delivery_days end) as average_delivery_days
    from shipments
    group by 1, 2
)

select
    carrier,
    shipping_region,
    shipment_count,
    delivered_shipments,
    late_shipments,
    
    round(average_delivery_days, 2) as average_delivery_days,
    round(case
            when delivered_shipments = 0 then null
            else late_shipments * 1.0 / delivered_shipments
          end, 4) as late_delivery_rate
from shipment_rollup
