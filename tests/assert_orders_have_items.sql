select
    o.order_id
from {{ ref('fct_orders') }} as o
left join {{ ref('fct_order_items') }} as o_itms
    on o.order_id = o_itms.order_id
where o.order_status != 'cancelled'
group by o.order_id
having count(o_itms.order_item_sk) = 0