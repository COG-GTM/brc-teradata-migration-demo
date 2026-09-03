-- Migrated from: teradata/ddl/03_staging_views.sql (V_ACCOUNT_CURRENT)
-- Teradata constructs replaced:
--   ZEROIFNULL(x)         -> zeroifnull() compat macro
--   date - date = integer -> datediff() cross-database macro
--   LOCK ROW FOR ACCESS   -> removed
--
-- Databricks notes:
--   * `current_date` is used without parentheses so the same SQL parses on
--     Databricks, Snowflake and Postgres.
--   * dates are cast explicitly because Databricks infers string for
--     CSV/CTAS-loaded landing columns.

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
        cast(open_date as date) as open_date,
        cast(close_date as date) as close_date,

        -- Derived: is the account currently open?
        case
            when upper(trim(status)) in ('ACTIVE', 'DORMANT') then true
            else false
        end as is_open,

        -- Derived: days since account opened
        -- Teradata: CURRENT_DATE - open_date (returns integer)
        {{ datediff('cast(open_date as date)', 'current_date', 'day') }} as days_since_opening

    from source
    where upper(trim(status)) != 'CLOSED'
       or close_date is null

)

select * from cleaned
