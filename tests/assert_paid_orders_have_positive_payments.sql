select
    payment_id
from {{ ref('fct_payments') }}
where payment_status = 'paid'
  and payment_amount <= 0
