with order_items as (
    select *
    from {{ ref('fct_order_items') }}
    where order_status != 'cancelled'
),

products as (
    select *
    from {{ ref('dim_products') }}
),

review_rollup as (
    select
        o_itms.product_id,
        count(r.review_id) as review_count,
        avg(r.rating) as average_rating
    from order_items as o_itms
    left join {{ ref('stg_reviews') }} as r
        on o_itms.order_item_id = r.order_item_id
    group by 1
),

return_rollup as (
    select
        o_itms.product_id,
        count(rt.return_id) as return_count,
        sum(case when rt.return_status = 'approved' then rt.refund_amount else 0 end) as approved_refund_amount
    from order_items as o_itms
    left join {{ ref('stg_returns') }} as rt
        on o_itms.order_item_id = rt.order_item_id
    group by 1
),

sales_rollup as (
    select
        product_id,
        sum(quantity) as units_sold,
        sum(net_item_amount) as product_revenue,
        sum(item_profit_amount) as product_profit
    from order_items
    group by 1
)

select
    prod.product_id,
    prod.product_name,
    prod.category_name,
    prod.brand_name,
    coalesce(sales_rollup.units_sold, 0) as units_sold,
    round(coalesce(sales_rollup.product_revenue, 0), 2) as product_revenue,
    round(coalesce(sales_rollup.product_profit, 0), 2) as product_profit,
    coalesce(rev_rollup.review_count, 0) as review_count,
    round(coalesce(rev_rollup.average_rating, 0), 2) as average_rating,
    coalesce(return_rollup.return_count, 0) as return_count,
    round(coalesce(return_rollup.approved_refund_amount, 0), 2) as approved_refund_amount,
    round(
        case
            when coalesce(sales_rollup.units_sold, 0) = 0 then 0
            else coalesce(return_rollup.return_count, 0) * 1.0 / sales_rollup.units_sold
        end,
        4
    ) as return_rate,
    case
        when coalesce(return_rollup.return_count, 0) >= 1 then 'watchlist'
        when coalesce(rev_rollup.review_count, 0) >= 1 and coalesce(rev_rollup.average_rating, 0) < 3 then 'watchlist'
        when coalesce(rev_rollup.review_count, 0) >= 1 and coalesce(rev_rollup.average_rating, 0) >= 4.5 and coalesce(return_rollup.return_count, 0) = 0 then 'top_rated'
        else 'stable'
    end as product_quality_flag
from products as prod
left join sales_rollup as sales_rollup
    on prod.product_id = sales_rollup.product_id
left join review_rollup as rev_rollup
    on prod.product_id = rev_rollup.product_id
left join return_rollup as return_rollup
    on prod.product_id = return_rollup.product_id
