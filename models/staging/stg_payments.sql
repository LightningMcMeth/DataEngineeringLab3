with payments as (
    select *
    from {{ ref('raw_payments') }}
)

select
    payment_id,
    order_id,

    payment_date,
    cast(payment_date as date) as payment_date_day,
    cast(date_trunc('month', payment_date) as date) as payment_month,
    
    lower({{ normalize_text('payment_method') }}) as payment_method,
    lower({{ normalize_text('payment_status') }}) as payment_status,
    amount as payment_amount,

    updated_at as payment_updated_at
from payments

