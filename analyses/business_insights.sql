select *
from {{ ref('mart_customer_value') }}
order by lifetime_revenue desc
limit 5;

select *
from {{ ref('mart_category_performance') }}
where category_revenue_rank = 1
order by order_month;

select *
from {{ ref('mart_monthly_revenue') }}
order by order_month;

select *
from {{ ref('mart_delivery_performance') }}
order by late_delivery_rate desc, shipment_count desc;