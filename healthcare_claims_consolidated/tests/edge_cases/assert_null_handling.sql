-- =============================================================================
-- Edge Case: Null Handling Validation
-- Ensures critical fields are never null in mart-level models.
-- =============================================================================

-- Check transaction mart for null critical fields
select
    'fct_daily_transactions' as model_name,
    'transaction_id' as field_name,
    count(*) as null_count
from {{ ref('fct_daily_transactions') }}
where transaction_id is null

union all

select
    'fct_daily_transactions',
    'transaction_date',
    count(*)
from {{ ref('fct_daily_transactions') }}
where transaction_date is null

union all

select
    'fct_credit_risk_scores',
    'customer_id',
    count(*)
from {{ ref('fct_credit_risk_scores') }}
where customer_id is null

union all

select
    'fct_credit_risk_scores',
    'risk_rating',
    count(*)
from {{ ref('fct_credit_risk_scores') }}
where risk_rating is null

union all

select
    'fct_aml_alerts',
    'alert_id',
    count(*)
from {{ ref('fct_aml_alerts') }}
where alert_id is null

having count(*) > 0  -- Only return rows with nulls found
