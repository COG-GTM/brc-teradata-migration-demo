-- =============================================================================
-- Migrated from: teradata/ddl/04_warehouse_tables.sql (DIM_ACCOUNT)
--   and dbt_project/models/marts/finance/dim_accounts.sql
-- Account dimension with current state and derived attributes.
-- Now operates on unified cross-platform data.
-- =============================================================================

with accounts as (

    select * from {{ ref('int_unified_account') }}

),

customers as (

    select * from {{ ref('int_unified_customer') }}

)

select
    a.account_id,
    a.customer_id,
    c.first_name || ' ' || c.last_name as customer_name,
    c.segment as customer_segment,
    a.account_type,
    a.currency,
    a.branch_code,
    a.status,
    case when a.close_date is null then true else false end as is_open,
    a.open_date,
    a.close_date,
    a.days_since_opening,

    -- Account age band
    case
        when a.days_since_opening < 365 then 'NEW'
        when a.days_since_opening < 1825 then 'ESTABLISHED'
        else 'MATURE'
    end as account_age_band,

    a.credit_limit,
    a.overdraft_limit,
    a.source_platform,
    current_timestamp as etl_loaded_ts

from accounts a
inner join customers c
    on a.customer_id = c.customer_id
