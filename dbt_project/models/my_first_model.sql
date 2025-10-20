-- models/my_first_model.sql
SELECT
    id,
    customer_name,
    amount,
    order_date
FROM {{ ref('raw_orders') }}
WHERE amount > 0