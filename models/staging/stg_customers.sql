with customers as (
    select *
    from {{ ref('raw_customers') }}
)

select
    customer_id,

    {{ normalize_text('first_name') }} as first_name,
    {{ normalize_text('last_name') }} as last_name,
    lower({{ normalize_text('email') }}) as email,
    {{ normalize_text('phone') }} as phone,
    {{ normalize_text('city') }} as city,
    upper({{ normalize_text('region') }}) as region,

    signup_date,
    lower({{ normalize_text('customer_tier') }}) as customer_tier,
    case
        when lower({{ normalize_text('customer_tier') }}) = 'gold' then 'high_value'
        when lower({{ normalize_text('customer_tier') }}) = 'silver' then 'mid_value'
        else 'developing'
    end as customer_segment,
    is_active,
    updated_at as customer_updated_at,
    
    {{ build_surrogate_key(['customer_id', 'email']) }} as customer_sk
from customers