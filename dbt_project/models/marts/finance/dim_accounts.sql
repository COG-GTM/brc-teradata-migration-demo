-- Migrated from: teradata/ddl/04_warehouse_tables.sql (DIM_ACCOUNT)
-- Account dimension with current state and derived attributes.

with accounts as (

    select * from {{ ref('stg_accounts') }}

),

customers as (

    select * from {{ ref('stg_customers') }}

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
    a.is_open,
    a.open_date,
    a.close_date,
    a.days_since_opening,

    -- Account age band
    case
        when a.days_since_opening < 365 then 'NEW'
        when a.days_since_opening < 1825 then 'ESTABLISHED'
        else 'MATURE'
    end as account_age_band,

    current_timestamp as etl_loaded_ts

from accounts a
inner join customers c
    on a.customer_id = c.customer_id
