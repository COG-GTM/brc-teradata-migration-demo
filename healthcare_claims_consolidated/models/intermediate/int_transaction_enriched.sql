-- =============================================================================
-- Enriched transactions joined with unified account and counterparty data.
-- Migrated from: teradata/stored_procedures/sp_daily_transaction_load.sql
--   and dbt_project/models/intermediate/int_transaction_enriched.sql
-- This provides the denormalized transaction view used by all downstream marts.
-- =============================================================================

with transactions as (

    select * from {{ ref('int_unified_transaction') }}

),

accounts as (

    select * from {{ ref('int_unified_account') }}

),

counterparties as (

    select * from {{ ref('int_unified_counterparty') }}

),

customers as (

    select * from {{ ref('int_unified_customer') }}

),

enriched as (

    select
        t.transaction_id,
        t.account_id,
        t.transaction_date,
        t.transaction_time,
        t.amount,
        t.signed_amount,
        t.currency,
        t.transaction_type,
        t.description,
        t.channel,
        t.value_band,
        t.reference_number,
        t.balance_after,

        -- Account context
        a.customer_id,
        a.account_type,
        a.branch_code,
        a.status as account_status,

        -- Customer context
        c.first_name || ' ' || c.last_name as customer_name,
        c.segment as customer_segment,
        c.risk_rating as customer_risk_rating,
        c.kyc_status as customer_kyc_status,

        -- Counterparty context
        t.counterparty_id,
        cp.counterparty_name,
        cp.counterparty_type,
        cp.country_code as counterparty_country,
        cp.screening_category as counterparty_screening_category,
        coalesce(cp.is_sanctions_listed, false) as counterparty_is_sanctioned,

        -- Derived flags
        case
            when cp.screening_category = 'HIGH' then true
            else false
        end as is_high_risk_counterparty,

        case
            when t.amount >= 10000 then true
            else false
        end as is_reportable_transaction,

        -- Source platform tracking
        t.source_platform as transaction_source_platform

    from transactions t
    inner join accounts a
        on t.account_id = a.account_id
    inner join customers c
        on a.customer_id = c.customer_id
    left join counterparties cp
        on t.counterparty_id = cp.counterparty_id

)

select * from enriched
