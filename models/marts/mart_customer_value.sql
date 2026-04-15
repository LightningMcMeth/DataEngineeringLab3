with customer_dimension as (
    select *
    from {{ ref('dim_customers') }}
),

customer_orders as (
    select *
    from {{ ref('int_customer_order_sequence') }}
),

customer_rollup as (
    select
        customer_id,
        
        count(*) as total_orders,
        sum(recognized_revenue_amount) as lifetime_revenue,
        avg(order_total_amount) as average_order_value,
        min(order_date) as first_order_date,
        max(order_date) as most_recent_order_date,
        min(case when customer_order_number = 2 then days_since_previous_order end) as days_to_second_order
    from customer_orders
    group by 1
)

select
    cust_dim.customer_id,
    cust_dim.customer_name,

    cust_dim.email,
    cust_dim.city,
    cust_dim.region,

    cust_dim.customer_tier,
    cust_dim.customer_segment,

    coalesce(cust_rollup.total_orders, 0) as total_orders,
    round(coalesce(cust_rollup.lifetime_revenue, 0), 2) as lifetime_revenue,
    round(coalesce(cust_rollup.average_order_value, 0), 2) as average_order_value,

    cust_rollup.first_order_date,
    cust_rollup.most_recent_order_date,
    cust_rollup.days_to_second_order,
    case when coalesce(cust_rollup.total_orders, 0) > 1 then true else false end as is_repeat_customer,
    case
        when coalesce(cust_rollup.lifetime_revenue, 0) >= 300 then 'vip'
        when coalesce(cust_rollup.lifetime_revenue, 0) >= 150 then 'loyal'
        when coalesce(cust_rollup.lifetime_revenue, 0) > 0 then 'active'
        else 'inactive'
    end as customer_value_segment
from customer_dimension as cust_dim
left join customer_rollup as cust_rollup
    on cust_dim.customer_id = cust_rollup.customer_id
