with shipments as (
    select *
    from {{ ref('raw_shipments') }}
)

select
    shipment_id,
    order_id,

    {{ normalize_text('carrier') }} as carrier,

    shipped_at,
    estimated_delivery_at,
    delivered_at,

    lower({{ normalize_text('shipment_status') }}) as shipment_status,
    upper({{ normalize_text('shipping_region') }}) as shipping_region,
    case
        when delivered_at is not null and delivered_at > estimated_delivery_at then true
        else false
    end as is_delivered_late,

    updated_at as shipment_updated_at
from shipments