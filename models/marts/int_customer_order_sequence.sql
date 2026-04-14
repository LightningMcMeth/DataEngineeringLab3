with eligible_orders as (
    select
        order_id,
        customer_id,
        order_date,
        order_month,
        order_status,
        payment_status,
        order_total_amount,
        recognized_revenue_amount
    from {{ ref('fct_orders') }}
    where order_status != 'cancelled'
),

sequenced_orders as (
    select
        order_id,
        customer_id,
        order_date,
        order_month,
        order_status,
        payment_status,
        order_total_amount,
        recognized_revenue_amount,
        row_number() over (
            partition by customer_id
            order by order_date, order_id
        ) as customer_order_number,
        lag(order_date) over (
            partition by customer_id
            order by order_date, order_id
        ) as previous_order_date
    from eligible_orders
)

select
    order_id,
    customer_id,
    order_date,
    order_month,
    order_status,
    payment_status,
    order_total_amount,
    recognized_revenue_amount,
    customer_order_number,
    previous_order_date,
    case
        when previous_order_date is not null
            then date_diff('day', previous_order_date, order_date)
        else null
    end as days_since_previous_order
from sequenced_orders
