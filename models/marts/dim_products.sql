{{ config(materialized='incremental', unique_key='product_id', on_schema_change='sync_all_columns') }}

with products as (
    select *
    from {{ ref('stg_products') }}
),

categories as (
    select *
    from {{ ref('stg_categories') }}
)

select
    {{ build_surrogate_key(['prod.product_id', 'prod.category_id']) }} as product_sk,
    prod.product_id,
    prod.category_id,

    cat.category_name,
    cat.department_name,

    prod.product_name,
    prod.brand_name,

    prod.list_price,
    prod.standard_cost,
    prod.margin_pct,

    prod.is_active,
    prod.product_created_date,
    prod.product_updated_at as record_updated_at
    
from products as prod
left join categories as cat
    on prod.category_id = cat.category_id

{% if is_incremental() %}
where prod.product_updated_at >= (
    select coalesce(max(record_updated_at), timestamp '1932-01-01 00:00:00')
    from {{ this }}
)
{% endif %}
