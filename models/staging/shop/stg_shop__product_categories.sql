select
    category_id,
    category_name,
    parent_category_id
from {{ source('shop', 'product_categories') }}
