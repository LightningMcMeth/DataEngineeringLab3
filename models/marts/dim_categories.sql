with categories as (
    select *
    from {{ ref('stg_categories') }}
),

product_rollup as (
    select
        category_id,

        count(*) as active_product_count,
        max(product_updated_at) as latest_product_updated_at
    from {{ ref('stg_products') }}
    where is_active
    group by 1
)

select
    {{ build_surrogate_key(['cat.category_id']) }} as category_sk,
    cat.category_id,
    cat.category_name,
    cat.department_name,

    coalesce(p_rollup.active_product_count, 0) as active_product_count,
    greatest(
        cat.category_updated_at,
        coalesce(p_rollup.latest_product_updated_at, timestamp '1900-01-01 00:00:00')
    ) as record_updated_at
    
from categories as cat
left join product_rollup as p_rollup
    on cat.category_id = p_rollup.category_id
