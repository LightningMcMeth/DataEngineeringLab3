with returns as (
    select *
    from {{ ref('raw_returns') }}
)

select
    return_id,
    order_item_id,

    cast(return_request_date as date) as return_request_date,

    lower({{ normalize_text('return_reason') }}) as return_reason,
    lower({{ normalize_text('return_status') }}) as return_status,

    refund_amount,
    
    updated_at as return_updated_at
from returns

