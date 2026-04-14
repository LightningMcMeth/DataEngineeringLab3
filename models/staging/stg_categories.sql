with categories as (
    select *
    from {{ ref('raw_categories') }}
)

select
    category_id,

    case lower({{ normalize_text('category_name') }})
        when 'electronics' then 'Electronics'
        when 'home & kitchen' then 'Home & Kitchen'
        when 'fashion' then 'Fashion'
        when 'beauty' then 'Beauty'
        else {{ normalize_text('category_name') }}
    end as category_name,

    {{ normalize_text('department_name') }} as department_name,
    
    updated_at as category_updated_at
from categories