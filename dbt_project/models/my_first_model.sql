{{ config(schema="main") }}

SELECT
    id,
    customer_name,
    amount,
    order_date
FROM {{ ref('raw_orders') }}
WHERE amount > 0