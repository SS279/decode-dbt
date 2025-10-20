-- Aggregate sales by status
select
    order_status,
    sum(total_amount) as total_amount
from {{ ref('raw_orders') }}
group by order_status