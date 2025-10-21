select
    c.customer_id,
    c.customer_name,
    c.region,
    c.loyalty_points,
    sum(f.total_revenue) as total_spent
from {{ ref('stg_cafe_customers') }} c
left join {{ ref('fact_sales') }} f
on c.customer_id = f.customer_id
group by 1,2,3,4