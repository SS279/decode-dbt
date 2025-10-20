-- Filter completed orders and rename columns
select
    order_id as id,
    customer_id as cust_id,
    total_amount as amount
from {{ ref('raw_orders') }}
where order_status = 'completed'