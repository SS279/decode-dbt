-- Select all completed orders
select *
from {{ ref('raw_orders') }}
where order_status = 'completed'