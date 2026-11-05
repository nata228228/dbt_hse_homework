{{ config(distributed_by='order_id',orientation='column') }}




select
    soi.order_id::text || '-' || soi.product_id::text as order_item_id,--не указано, но надо
    soi.order_id,--аналогично

    soi.product_id,
    o.customer_id,
    o.order_date,
    soi.quantity,
    soi.unit_price,
    soi.discount_amount,
    soi.line_amount,
    o.is_confirmed
from {{ ref('stg_shop__order_items') }} as soi
join {{ ref('fct_orders') }} as o
    on soi.order_id = o.order_id