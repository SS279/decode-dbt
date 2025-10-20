-- Join orders with customer information
select
    o.order_id,
    o.customer_id,
    c.customer_name,
    c.customer_region,
    o.total_amount
from {{ ref('raw_orders') }} as o
left join {{ ref('customers') }} as c
on o.customer_id = c.customer_id