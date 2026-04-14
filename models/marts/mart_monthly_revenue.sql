with monthly_rollup as (
    select
        order_month,
        count(*) as total_orders,
        sum(recognized_revenue_amount) as monthly_revenue,
        avg(order_total_amount) as average_order_value
    from {{ ref('fct_orders') }}
    where order_status != 'cancelled'
    group by 1
)

select
    order_month,
    total_orders,
    round(monthly_revenue, 2) as monthly_revenue,
    round(average_order_value, 2) as average_order_value,
    round(lag(monthly_revenue) over (order by order_month), 2) as previous_month_revenue,
    round(monthly_revenue - coalesce(lag(monthly_revenue) over (order by order_month), 0),2) 
    as month_over_month_change,
    round(sum(monthly_revenue) over ( order by order_month rows between unbounded preceding and current row), 2) 
    as running_revenue
from monthly_rollup
