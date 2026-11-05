with recursive tree as (
    select
        category_id,
        category_name,
        parent_category_id,
        null::text as parent_category_name,
        category_name as root_category_name,
        1 as depth
    from {{ ref('stg_shop__product_categories') }}
    where parent_category_id is null

    union all

    select
        c.category_id,
        c.category_name,
        c.parent_category_id,
        t.category_name,
        t.root_category_name,
        t.depth + 1
    from {{ ref('stg_shop__product_categories') }} as c
    join tree as t on c.parent_category_id = t.category_id
)
select * from tree
