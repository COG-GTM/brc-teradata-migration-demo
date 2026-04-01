-- Migrated from: teradata/ddl/04_warehouse_tables.sql (DIM_CUSTOMER)
-- Customer dimension with current state and derived attributes.
-- Teradata constructs replaced:
--   GENERATED ALWAYS AS IDENTITY -> generate_surrogate_key
--   PERIOD(DATE) validity_period -> valid_from / valid_to columns
--   SCD Type 2 cursor loop       -> dbt snapshot (snap_customer_risk_rating)

with customers as (

    select * from {{ ref('stg_customers') }}

),

risk_factors as (

    select
        customer_id,
        derived_risk_rating,
        total_transactions,
        total_transaction_volume,
        customer_age,
        tenure_years
    from {{ ref('int_customer_risk_factors') }}

),

accounts as (

    select
        customer_id,
        count(distinct account_id) as total_accounts,
        sum(case when is_open then 1 else 0 end) as active_accounts,
        min(open_date) as first_account_date

    from {{ ref('stg_accounts') }}
    group by customer_id

)

select
    {{ dbt_utils.generate_surrogate_key(['c.customer_id']) }} as customer_sk,
    c.customer_id,
    c.first_name,
    c.last_name,
    c.first_name || ' ' || c.last_name as full_name,
    c.date_of_birth,
    c.nationality,
    c.kyc_status,
    c.risk_rating,
    c.segment,
    c.onboarding_date,

    -- Risk metrics from intermediate layer
    rf.derived_risk_rating,
    rf.customer_age,
    rf.tenure_years,
    rf.total_transactions,
    rf.total_transaction_volume,

    -- Account summary
    coalesce(a.total_accounts, 0) as total_accounts,
    coalesce(a.active_accounts, 0) as active_accounts,
    a.first_account_date,

    -- Customer lifecycle band
    case
        when rf.tenure_years < 1 then 'NEW'
        when rf.tenure_years < 5 then 'ESTABLISHED'
        else 'LOYAL'
    end as lifecycle_band,

    current_timestamp as etl_loaded_ts

from customers c
left join risk_factors rf
    on c.customer_id = rf.customer_id
left join accounts a
    on c.customer_id = a.customer_id
