{{ config(materialized='view') }}

-- Step 1: Clean and standardize raw orders
SELECT
    order_id
    , customer_name
    , customer_id
    , CAST(order_date AS DATE) AS order_date
    , amount
    , LOWER(status) AS status
FROM {{ ref('raw_orders') }}
WHERE amount IS NOT NULL