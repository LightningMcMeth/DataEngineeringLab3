with reviews as (
    select *
    from {{ ref('raw_reviews') }}
)

select
    review_id,
    order_item_id,
    customer_id,

    cast(review_date as date) as review_date,

    rating,
    {{ normalize_text('review_text') }} as review_text,
    
    updated_at as review_updated_at
from reviews

