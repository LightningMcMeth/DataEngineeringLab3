{{ config(materialized='incremental', unique_key='customer_id') }}

with customers as (
    select *
    from {{ ref('stg_customers') }}
),

order_rollup as (
    select
        customer_id,
        min(order_date) as first_order_date,
        max(order_date) as most_recent_order_date,
        sum(case when order_status != 'cancelled' then 1 else 0 end) as total_non_cancelled_orders,
        max(order_updated_at) as latest_order_updated_at
    from {{ ref('stg_orders') }}
    group by 1
),

final as (
    select
        c.customer_sk,
        c.customer_id,

        c.first_name,
        c.last_name,
        c.first_name || ' ' || c.last_name as customer_name,

        c.email,
        c.phone,
        c.city,
        c.region,

        c.signup_date,
        c.customer_tier,
        c.customer_segment,

        c.is_active,
        o_rollup.first_order_date,
        o_rollup.most_recent_order_date,
        coalesce(o_rollup.total_non_cancelled_orders, 0) as total_non_cancelled_orders,
        case
            when coalesce(o_rollup.total_non_cancelled_orders, 0) >= 3 then 'repeat_customer'
            when coalesce(o_rollup.total_non_cancelled_orders, 0) = 2 then 'growing_customer'
            when coalesce(o_rollup.total_non_cancelled_orders, 0) = 1 then 'new_customer'
            else 'prospect_customer'
        end as lifecycle_stage,
        greatest(
            c.customer_updated_at,
            coalesce(o_rollup.latest_order_updated_at, timestamp '1900-01-01 00:00:00')
        ) as record_updated_at
        
    from customers as c
    left join order_rollup as o_rollup
        on c.customer_id = o_rollup.customer_id
)

select *
from final
{% if is_incremental() %}
where record_updated_at >= (
    select coalesce(max(record_updated_at), timestamp '1900-01-01 00:00:00')
    from {{ this }}
)
{% endif %}
