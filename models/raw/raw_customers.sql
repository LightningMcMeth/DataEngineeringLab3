with source_data as (
    select *
    from {{ source('ecommerce_seed', 'customers') }}
)

select
    cast(customer_id as integer) as customer_id,

    cast(first_name as varchar) as first_name,
    cast(last_name as varchar) as last_name,
    cast(email as varchar) as email,
    cast(phone as varchar) as phone,
    cast(city as varchar) as city,
    cast(region as varchar) as region,

    cast(signup_date as date) as signup_date,
    cast(customer_tier as varchar) as customer_tier,
    cast(is_active as boolean) as is_active,


    cast(updated_at as timestamp) as updated_at
from source_data