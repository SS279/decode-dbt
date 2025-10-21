with source as (
    select * from {{ ref('raw_cafe_customers') }}
),
cleaned as (
    select
        customer_id,
        customer_name,
        region,
        cast(join_date as date) as join_date,
        loyalty_points
    from source
)
select * from cleaned