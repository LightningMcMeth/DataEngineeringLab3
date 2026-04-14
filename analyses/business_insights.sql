-- Top customers by lifetime revenue
select *
from {{ ref('mart_customer_value') }}
order by lifetime_revenue desc
limit 5;

-- Best categories by monthly revenue
select *
from {{ ref('mart_category_performance') }}
where category_revenue_rank = 1
order by order_month;

-- Monthly revenue trend with running total
select *
from {{ ref('mart_monthly_revenue') }}
order by order_month;

-- Worst delivery performers
select *
from {{ ref('mart_delivery_performance') }}
order by late_delivery_rate desc, shipment_count desc;

-- Products that need quality review
select *
from {{ ref('mart_product_quality') }}
where product_quality_flag = 'watchlist'
order by return_rate desc, average_rating asc;

