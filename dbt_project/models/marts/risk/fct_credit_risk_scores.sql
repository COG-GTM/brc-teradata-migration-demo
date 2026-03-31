-- Migrated from: teradata/stored_procedures/sp_customer_risk_scoring.sql
--   and teradata/ddl/05_mart_tables.sql (MART_CREDIT_RISK)
-- Teradata constructs replaced:
--   VOLATILE TABLE         -> CTE
--   ZEROIFNULL / NULLIFZERO -> coalesce / nullif
--   HASHROW / HASHBUCKET    -> hash()

with risk_factors as (

    select * from {{ ref('int_customer_risk_factors') }}

),

risk_ratings as (

    select * from {{ ref('risk_rating_codes') }}

),

scored as (

    select
        rf.customer_id,
        rf.kyc_status,
        rf.current_risk_rating,
        rf.derived_risk_rating as risk_rating,
        rf.segment,
        rf.customer_age,
        rf.tenure_years,
        rf.account_count,
        rf.active_account_count,
        rf.total_transactions,
        rf.total_transaction_volume,
        rf.avg_transaction_amount,
        rf.large_transaction_count,

        -- PD (Probability of Default) based on risk rating
        case rf.derived_risk_rating
            when 'A' then 0.002
            when 'B' then 0.010
            when 'C' then 0.030
            when 'D' then 0.080
            when 'E' then 0.150
            else 0.100
        end as probability_of_default,

        -- LGD (Loss Given Default)
        case
            when rf.segment = 'RETAIL' then 0.45
            when rf.segment = 'WEALTH' then 0.35
            when rf.segment = 'CORPORATE' then 0.40
            else 0.45
        end as loss_given_default,

        -- EAD (Exposure At Default) - simplified as total transaction volume
        rf.total_transaction_volume as exposure_at_default

    from risk_factors rf

)

select
    customer_id,
    kyc_status,
    current_risk_rating,
    risk_rating,
    segment,
    customer_age,
    tenure_years,
    account_count,
    active_account_count,
    total_transactions,
    total_transaction_volume,
    avg_transaction_amount,
    large_transaction_count,
    probability_of_default,
    loss_given_default,
    exposure_at_default,

    -- RWA (Risk-Weighted Assets) = EAD * Risk Weight
    -- Simplified Basel III SA: RW = 12.5 * LGD * PD correlation factor
    exposure_at_default * probability_of_default * loss_given_default * 12.5 as risk_weighted_assets,

    -- Expected Loss = PD * LGD * EAD
    probability_of_default * loss_given_default * exposure_at_default as expected_loss,

    current_date as assessment_date,
    current_timestamp as etl_loaded_ts

from scored
