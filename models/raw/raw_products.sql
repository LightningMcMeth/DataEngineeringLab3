with source_data as (
    select *
    from {{ source('ecommerce_seed', 'products') }}
)

select
    cast(product_id as integer) as product_id,
    cast(category_id as integer) as category_id,

    cast(product_name as varchar) as product_name,
    cast(brand_name as varchar) as brand_name,

    cast(list_price as numeric(10, 2)) as list_price,
    cast(standard_cost as numeric(10, 2)) as standard_cost,

    cast(is_active as boolean) as is_active,
    cast(created_at as timestamp) as created_at,

    
    cast(updated_at as timestamp) as updated_at
from source_data