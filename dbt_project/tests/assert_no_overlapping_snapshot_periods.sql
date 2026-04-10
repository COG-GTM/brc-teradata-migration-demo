-- Snapshot validity test: No overlapping validity periods
-- For SCD Type 2, each customer_id should have at most ONE active record
-- (where dbt_valid_to IS NULL). Overlapping periods indicate a snapshot bug.

select
    customer_id,
    count(*) as active_record_count
from {{ ref('snap_customer_risk_rating') }}
where dbt_valid_to is null
group by customer_id
having count(*) > 1
