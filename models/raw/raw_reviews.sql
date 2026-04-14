with source_data as (
    select *
    from {{ source('ecommerce_seed', 'reviews') }}
)

select
    cast(review_id as integer) as review_id,
    cast(order_item_id as integer) as order_item_id,
    cast(customer_id as integer) as customer_id,
    
    cast(review_date as timestamp) as review_date,
    
    cast(rating as integer) as rating,
    cast(review_text as varchar) as review_text,


    cast(updated_at as timestamp) as updated_at
from source_data