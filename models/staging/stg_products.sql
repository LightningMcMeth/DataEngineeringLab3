with products as (
    select *
    from {{ ref('raw_products') }}
)

select
    product_id,
    category_id,

    {{ normalize_text('product_name') }} as product_name,
    {{ normalize_text('brand_name') }} as brand_name,

    list_price,
    standard_cost,
    round(case
            when list_price = 0 then null
            else ((list_price - standard_cost) / list_price) * 100 end, 2
         ) as margin_pct,
         
    is_active,
    cast(created_at as date) as product_created_date,
    updated_at as product_updated_at
from products
