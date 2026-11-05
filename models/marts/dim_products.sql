{{ config(distributed_replicated=true) }}

select
    p.product_id,
    p.product_name,
    p.brand,
    t.category_name,
    t.parent_category_name,
    t.root_category_name
from {{ ref('stg_shop__products') }} as p
left join {{ ref('int_category_tree') }} as t
    on t.category_name = p.category