with orders as (
    select * from {{ ref('stg_cafe_orders') }}
)
select
    customer_id,
    store_location,
    count(*) as total_orders,
    sum(amount) as total_revenue,
    avg(amount) as avg_order_value
from orders
where status = 'completed'
group by 1, 2