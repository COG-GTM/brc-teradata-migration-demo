-- =============================================================================
-- Source: Snowflake / HEALTHCARE_RAW / transaction
-- Migration: txn_id -> transaction_id (column name drift)
--            txn_date -> transaction_date (column name drift)
--            txn_time -> transaction_time (column name drift)
--            txn_type -> transaction_type (column name drift)
--            ccy -> currency (column name drift)
--            narrative -> description (column name drift)
--            ref_number -> reference_number (column name drift)
--            running_balance -> balance_after (column name drift)
--            txn_metadata VARIANT -> ignored (Snowflake-only extension)
-- Date: 2026-04-02
-- =============================================================================

with source as (

    select * from {{ source('snowflake_raw', 'transaction') }}

)

select
    -- Column naming drift: txn_id -> transaction_id
    txn_id as transaction_id,
    account_id,
    -- Column naming drift: txn_date -> transaction_date
    txn_date as transaction_date,
    -- Column naming drift: txn_time -> transaction_time
    txn_time as transaction_time,
    amount,
    -- Column naming drift: ccy -> currency
    upper(trim(ccy)) as currency,
    -- Column naming drift: txn_type -> transaction_type
    upper(trim(txn_type)) as transaction_type,
    counterparty_id,
    -- Column naming drift: narrative -> description
    narrative as description,
    upper(trim(channel)) as channel,
    -- Column naming drift: ref_number -> reference_number
    ref_number as reference_number,
    -- Column naming drift: running_balance -> balance_after
    coalesce(running_balance, 0) as balance_after,
    -- Value band classification (consistent across all platforms)
    case
        when amount > 10000.00 then 'HIGH_VALUE'
        when amount > 1000.00 then 'MEDIUM_VALUE'
        else 'STANDARD'
    end as value_band,
    -- Signed amount (consistent across all platforms)
    case
        when upper(trim(txn_type)) in ('DEBIT', 'FEE') then amount * -1
        else amount
    end as signed_amount

from source
