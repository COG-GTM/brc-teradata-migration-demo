-- =============================================================================
-- Source: Databricks / healthcare_raw / transaction (Delta table)
-- Migration: PySpark DataFrame operations -> dbt SQL
--            transaction_timestamp (full timestamp) -> split to date + time
--            currency_code -> currency (column name drift)
--            Delta partition by transaction_month -> removed
--            amount as double -> cast to numeric for precision
-- Date: 2026-04-02
-- =============================================================================

with source as (

    select * from {{ source('databricks_raw', 'transaction') }}

)

select
    transaction_id,
    account_id,
    transaction_date,
    -- Structural drift: Databricks has full timestamp; extract time component
    cast(transaction_timestamp as time) as transaction_time,
    cast(amount as numeric(15,2)) as amount,
    -- Column naming drift: currency_code -> currency
    upper(trim(currency_code)) as currency,
    upper(trim(transaction_type)) as transaction_type,
    counterparty_id,
    description,
    upper(trim(channel)) as channel,
    reference_number,
    coalesce(cast(balance_after as numeric(15,2)), 0) as balance_after,
    -- Value band classification (consistent with Teradata staging)
    case
        when cast(amount as numeric(15,2)) > 10000.00 then 'HIGH_VALUE'
        when cast(amount as numeric(15,2)) > 1000.00 then 'MEDIUM_VALUE'
        else 'STANDARD'
    end as value_band,
    -- Signed amount (consistent with Teradata staging)
    case
        when upper(trim(transaction_type)) in ('DEBIT', 'FEE') then cast(amount as numeric(15,2)) * -1
        else cast(amount as numeric(15,2))
    end as signed_amount

from source
