-- Migrated from: teradata/ddl/03_staging_views.sql (V_ACCOUNT_CURRENT)
-- Teradata constructs replaced:
--   ZEROIFNULL(x)         -> coalesce(x, 0)
--   date - date = integer -> datediff('day', date1, date2)
--   LOCK ROW FOR ACCESS   -> removed

with source as (

    select * from {{ source('barclays_raw', 'account') }}

),

cleaned as (

    select
        account_id,
        customer_id,
        upper(trim(account_type)) as account_type,
        upper(trim(currency)) as currency,
        branch_code,
        upper(trim(status)) as status,
        open_date,
        close_date,

        -- Derived: is the account currently open?
        case
            when upper(trim(status)) in ('ACTIVE', 'DORMANT') then true
            else false
        end as is_open,

        -- Derived: days since account opened
        -- Teradata: CURRENT_DATE - open_date (returns integer)
        -- Snowflake/Databricks: datediff
        {{ datediff('open_date', 'current_date', 'day') }} as days_since_opening

    from source
    where upper(trim(status)) != 'CLOSED'
       or close_date is null

)

select * from cleaned
