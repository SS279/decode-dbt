{{ config(materialized='table') }}

-- Step 3: Aggregate results into a sales summary
SELECT
    customer_id
    , customer_name
    , COUNT(order_id) AS total_orders
    , SUM(amount) AS total_spent
FROM {{ ref('refined_orders') }}
GROUP BY customer_id, customer_name