with source_data as (
    select * 
    from {{ source('ecommerce_seed', 'categories') }}
)

select
    cast(category_id as integer) as category_id,

    cast(category_name as varchar) as category_name,
    cast(department_name as varchar) as department_name,

    
    cast(updated_at as timestamp) as updated_at
from source_data

