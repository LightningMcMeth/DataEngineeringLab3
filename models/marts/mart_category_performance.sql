with order_items as (
    select *
    from {{ ref('fct_order_items') }}
    where order_status != 'cancelled'
),

products as (
    select
        product_id,
        category_id,
        category_name
    from {{ ref('dim_products') }}
),

review_rollup as (
    select
        prod.category_id,

        avg(rev.rating) as average_rating

    from {{ ref('stg_reviews') }} as rev
    inner join {{ ref('fct_order_items') }} as o_itms
        on rev.order_item_id = o_itms.order_item_id
    inner join products as prod
        on o_itms.product_id = prod.product_id
    group by 1
),

category_month_rollup as (
    select
        o_itms.order_month,
        prod.category_id,
        prod.category_name,

        sum(o_itms.net_item_amount) as category_revenue,
        sum(o_itms.item_profit_amount) as category_profit,
        sum(o_itms.quantity) as units_sold,
        count(distinct o_itms.order_id) as order_count

    from order_items as o_itms
    inner join products as prod
        on o_itms.product_id = prod.product_id
    group by 1, 2, 3
)

select
    cat_rollup.order_month,
    cat_rollup.category_id,

    cat_rollup.category_name,

    round(cat_rollup.category_revenue, 2) as category_revenue,
    round(cat_rollup.category_profit, 2) as category_profit,

    cat_rollup.units_sold,
    cat_rollup.order_count,
    
    round(r_rollup.average_rating, 2) as average_rating,
    dense_rank() over (
        partition by cat_rollup.order_month
        order by cat_rollup.category_revenue desc
    ) as category_revenue_rank

from category_month_rollup as cat_rollup
left join review_rollup as r_rollup
    on cat_rollup.category_id = r_rollup.category_id
