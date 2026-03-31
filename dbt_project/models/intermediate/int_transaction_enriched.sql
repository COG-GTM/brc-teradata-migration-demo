-- Enriched transactions joined with account and counterparty data.
-- This model provides the denormalized transaction view used by downstream marts.

with transactions as (

    select * from {{ ref('stg_transactions') }}

),

accounts as (

    select * from {{ ref('stg_accounts') }}

),

counterparties as (

    select * from {{ ref('stg_counterparties') }}

),

customers as (

    select * from {{ ref('stg_customers') }}

),

enriched as (

    select
        t.transaction_id,
        t.account_id,
        t.transaction_date,
        t.amount,
        t.signed_amount,
        t.currency,
        t.transaction_type,
        t.description,
        t.channel,
        t.value_band,

        -- Account context
        a.customer_id,
        a.account_type,
        a.branch_code,
        a.status as account_status,

        -- Customer context
        c.first_name || ' ' || c.last_name as customer_name,
        c.segment as customer_segment,
        c.risk_rating as customer_risk_rating,

        -- Counterparty context
        t.counterparty_id,
        cp.counterparty_name,
        cp.counterparty_type,
        cp.country_code as counterparty_country,
        cp.screening_category as counterparty_screening_category,
        coalesce(cp.is_sanctions_listed, false) as counterparty_is_sanctioned,

        -- Derived flags
        case
            when cp.screening_category = 'HIGH_RISK' then true
            else false
        end as is_high_risk_counterparty,

        case
            when t.amount >= 10000 then true
            else false
        end as is_reportable_transaction

    from transactions t
    inner join accounts a
        on t.account_id = a.account_id
    inner join customers c
        on a.customer_id = c.customer_id
    left join counterparties cp
        on t.counterparty_id = cp.counterparty_id

)

select * from enriched
