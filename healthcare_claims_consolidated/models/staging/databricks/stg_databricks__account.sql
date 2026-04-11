-- =============================================================================
-- Source: Databricks / healthcare_raw / account (Delta table)
-- Migration: PySpark withColumn operations -> dbt SQL
--            is_active boolean -> status VARCHAR (standardized to match Teradata)
--            currency_code -> currency (column name drift resolution)
--            Delta partition by open_year -> removed
-- Date: 2026-04-02
-- =============================================================================

with source as (

    select * from {{ source('databricks_raw', 'account') }}

),

standardized as (

    select
        account_id,
        customer_id,
        upper(trim(account_type)) as account_type,
        -- Column naming drift: Databricks currency_code -> currency
        upper(trim(currency_code)) as currency,
        branch_code,
        sort_code,
        -- Structural drift: Databricks boolean is_active -> Teradata-style status
        case
            when is_active = true and close_date is null then 'ACTIVE'
            when is_active = false and close_date is not null then 'CLOSED'
            when is_active = false then 'DORMANT'
            else 'ACTIVE'
        end as status,
        open_date,
        close_date,
        coalesce(credit_limit, 0) as credit_limit,
        coalesce(overdraft_limit, 0) as overdraft_limit,
        case
            when close_date is not null then 'CLOSED'
            when is_active = false then 'DORMANT'
            when datediff('day', open_date, current_date) < 90 then 'NEW'
            else 'ACTIVE'
        end as derived_status,
        datediff('day', open_date, current_date) as days_since_opening,
        row_number() over (
            partition by account_id
            order by open_date desc
        ) as row_num

    from source

)

select
    account_id,
    customer_id,
    account_type,
    currency,
    branch_code,
    sort_code,
    status,
    open_date,
    close_date,
    credit_limit,
    overdraft_limit,
    derived_status,
    days_since_opening

from standardized
where row_num = 1
