-- =============================================================================
-- Source: Teradata / BARCLAYS_RAW / TRANSACTION (MULTISET table)
-- Migration: ZEROIFNULL(balance_after) -> coalesce(balance_after, 0)
--            NOT CASESPECIFIC           -> removed
--            RANGE_N PPI (monthly)      -> removed (Snowflake micro-partitions)
--            value_band / signed_amount -> derived columns (from V_TRANSACTION_ENRICHED)
-- Date: 2026-04-02
-- =============================================================================

with source as (

    select * from {{ source('teradata_raw', 'transaction') }}

)

select
    transaction_id,
    account_id,
    transaction_date,
    transaction_time,
    amount,
    upper(trim(currency)) as currency,
    upper(trim(transaction_type)) as transaction_type,
    counterparty_id,
    description,
    upper(trim(channel)) as channel,
    reference_number,
    coalesce(balance_after, 0) as balance_after,
    -- Value band classification (from Teradata V_TRANSACTION_ENRICHED)
    case
        when amount > 10000.00 then 'HIGH_VALUE'
        when amount > 1000.00 then 'MEDIUM_VALUE'
        else 'STANDARD'
    end as value_band,
    -- Signed amount (from Teradata V_TRANSACTION_ENRICHED)
    case
        when upper(trim(transaction_type)) in ('DEBIT', 'FEE') then amount * -1
        else amount
    end as signed_amount

from source
