-- =============================================================================
-- Migrated from: teradata/stored_procedures/sp_aml_screening.sql
-- Teradata constructs replaced:
--   VOLATILE TABLE vt_sanctions_list -> CTE / seed table
--   LIKE ANY (...)                   -> multiple LIKE with OR
--   SOUNDEX(x)                       -> soundex(x) (available in Snowflake)
--   OREPLACE(x, y, z)               -> replace(x, y, z)
--   OTRANSLATE(x, y, z)             -> translate(x, y, z)
-- Now operates on unified cross-platform enriched transactions.
-- =============================================================================

with enriched_txns as (

    select * from {{ ref('int_transaction_enriched') }}

),

-- Structuring detection: transactions just below reporting threshold (GBP 10,000)
-- Logic drift resolution: Teradata used 8000-9999 range; Databricks used 9000-9999.
-- Adopted Teradata's wider range (8000-9999) as it catches more structuring attempts
-- per FCA Anti-Money Laundering guidelines.
structuring_flags as (

    select
        customer_id,
        customer_name,
        transaction_date,
        'STRUCTURING' as alert_type,
        count(*) as supporting_transaction_count,
        sum(amount) as total_amount,
        'Multiple transactions just below GBP 10,000 threshold' as alert_description

    from enriched_txns
    where amount between 8000 and 9999
      and currency = 'GBP'
    group by customer_id, customer_name, transaction_date
    having count(*) >= 3

),

-- Velocity breach: unusually high transaction frequency or volume
velocity_flags as (

    select
        customer_id,
        customer_name,
        transaction_date,
        'VELOCITY_BREACH' as alert_type,
        count(*) as supporting_transaction_count,
        sum(amount) as total_amount,
        case
            when count(*) > 20 then 'More than 20 transactions in a single day'
            when sum(amount) > 100000 then 'Daily volume exceeds GBP 100,000'
            else 'Velocity threshold exceeded'
        end as alert_description

    from enriched_txns
    group by customer_id, customer_name, transaction_date
    having count(*) > 20 or sum(amount) > 100000

),

-- High-risk counterparty transactions
high_risk_country_flags as (

    select
        customer_id,
        customer_name,
        transaction_date,
        'HIGH_RISK_COUNTRY' as alert_type,
        count(*) as supporting_transaction_count,
        sum(amount) as total_amount,
        'Transaction with high-risk country counterparty' as alert_description

    from enriched_txns
    where counterparty_screening_category = 'HIGH'
    group by customer_id, customer_name, transaction_date

),

-- Sanctions match (counterparty on sanctions list)
sanctions_flags as (

    select
        customer_id,
        customer_name,
        transaction_date,
        'SANCTIONS_MATCH' as alert_type,
        count(*) as supporting_transaction_count,
        sum(amount) as total_amount,
        'Transaction with sanctioned counterparty' as alert_description

    from enriched_txns
    where counterparty_is_sanctioned = true
    group by customer_id, customer_name, transaction_date

),

all_flags as (

    select * from structuring_flags
    union all
    select * from velocity_flags
    union all
    select * from high_risk_country_flags
    union all
    select * from sanctions_flags

)

select
    {{ dbt_utils.generate_surrogate_key(['customer_id', 'transaction_date', 'alert_type']) }} as alert_id,
    customer_id,
    customer_name,
    transaction_date as alert_date,
    alert_type,
    supporting_transaction_count,
    total_amount,
    alert_description,
    case
        when alert_type = 'SANCTIONS_MATCH' then 'CRITICAL'
        when alert_type = 'STRUCTURING' then 'HIGH'
        when alert_type = 'VELOCITY_BREACH' then 'MEDIUM'
        when alert_type = 'HIGH_RISK_COUNTRY' then 'MEDIUM'
        else 'LOW'
    end as alert_severity,
    current_timestamp as screened_at

from all_flags
