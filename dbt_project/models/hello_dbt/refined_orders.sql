{{ config(materialized='table') }}

-- Step 2: Refine data to remove incomplete or refunded orders
SELECT
    order_id
    , customer_id
    , customer_name
    , order_date
    , amount
FROM {{ ref('staging_orders') }}
WHERE status = 'completed'