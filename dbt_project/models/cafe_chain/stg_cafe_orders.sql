with source as (
    select * from {{ ref('raw_cafe_orders') }}
),
cleaned as (
    select
        order_id,
        customer_id,
        cast(order_date as date) as order_date,
        lower(store_location) as store_location,
        cast(amount as double) as amount,
        status
    from source
)
select * from cleaned