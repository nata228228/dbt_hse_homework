{{ config(distributed_replicated=true) }}

with customers as (
    select * from {{ ref('stg_shop__customers') }}
),

confirmed_orders as (
    select
        order_id,
        customer_id,
        ordered_at,
        order_amount,
        row_number() over (
            partition by customer_id
            order by ordered_at, order_id
        ) as order_rank
    from {{ ref('fct_orders') }}
    where is_confirmed
),

customer_orders as (
    select
        customer_id,
        max(order_id) filter (where order_rank = 1) as first_confirmed_order_id,
        max(ordered_at) filter (where order_rank = 1) as first_confirmed_at,
        count(*) as confirmed_orders_count,
        sum(order_amount) as confirmed_revenue
    from confirmed_orders
    group by customer_id
)

select
    c.customer_id,
    c.registered_at,
    c.registration_date,
    c.city,
    c.acquisition_channel,
    co.first_confirmed_order_id,
    co.first_confirmed_at,
    coalesce(co.confirmed_orders_count, 0) as confirmed_orders_count,
    coalesce(co.confirmed_revenue, 0) as confirmed_revenue,
    coalesce(co.confirmed_orders_count, 0) > 0 as is_buyer
from customers as c
left join customer_orders as co
    on c.customer_id = co.customer_id