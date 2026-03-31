-- KYC compliance status for regulatory reporting.
-- Tracks customer verification status, risk rating, and account activity.

with customers as (

    select * from {{ ref('stg_customers') }}

),

account_summary as (

    select
        customer_id,
        count(distinct account_id) as total_accounts,
        sum(case when is_open then 1 else 0 end) as active_accounts,
        min(open_date) as earliest_account_date,
        max(open_date) as latest_account_date

    from {{ ref('stg_accounts') }}
    group by customer_id

),

risk_factors as (

    select
        customer_id,
        derived_risk_rating,
        total_transactions,
        total_transaction_volume

    from {{ ref('int_customer_risk_factors') }}

)

select
    c.customer_id,
    c.first_name,
    c.last_name,
    c.nationality,
    c.kyc_status,
    c.risk_rating,
    c.segment,
    c.onboarding_date,

    -- KYC review urgency
    case
        when c.kyc_status = 'EXPIRED' then 'OVERDUE'
        when c.kyc_status = 'FAILED' then 'REMEDIATION_REQUIRED'
        when c.kyc_status = 'PENDING' then 'IN_PROGRESS'
        when c.kyc_status = 'VERIFIED' then 'COMPLIANT'
        else 'UNKNOWN'
    end as compliance_status,

    -- Enhanced due diligence flag
    case
        when c.risk_rating in ('D', 'E') then true
        when rf.derived_risk_rating in ('D', 'E') then true
        when c.nationality not in ('GB', 'US', 'DE', 'FR', 'JP', 'CA', 'AU') then true
        else false
    end as requires_edd,

    coalesce(a.total_accounts, 0) as total_accounts,
    coalesce(a.active_accounts, 0) as active_accounts,
    coalesce(rf.total_transactions, 0) as total_transactions,
    coalesce(rf.total_transaction_volume, 0) as total_transaction_volume,
    rf.derived_risk_rating,

    current_timestamp as etl_loaded_ts

from customers c
left join account_summary a
    on c.customer_id = a.customer_id
left join risk_factors rf
    on c.customer_id = rf.customer_id
