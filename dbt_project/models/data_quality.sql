-- Simple data quality check: identify rows with null total_amount
select *
from {{ ref('raw_orders') }}
where total_amount is null
