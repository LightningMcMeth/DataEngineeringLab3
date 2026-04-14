select
    o.order_id
from {{ ref('fct_orders') }} as o
left join {{ ref('fct_order_items') }} as oi
    on o.order_id = oi.order_id
where o.order_status != 'cancelled'
group by o.order_id
having count(oi.order_item_sk) = 0

