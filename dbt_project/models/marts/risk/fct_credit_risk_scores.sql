-- Migrated from: teradata/stored_procedures/sp_customer_risk_scoring.sql
--   and teradata/ddl/05_mart_tables.sql (MART_CREDIT_RISK)
-- Teradata constructs replaced:
--   VOLATILE TABLE         -> CTE
--   ZEROIFNULL / NULLIFZERO -> coalesce / nullif
--   HASHROW / HASHBUCKET    -> hash()
--
-- Databricks notes:
--   * Every PD/LGD/EAD operand carries an explicit DECIMAL precision so the
--     Basel III products stay inside Spark's precision-38 ceiling. Without
--     this, Spark silently truncates the result scale (or returns NULL when
--     spark.sql.decimal.operations.allowPrecisionLoss=false).
--   * Numeric literals are cast rather than left bare: Spark types 12.5 as
--     DECIMAL(3,1) and 0.002 as DECIMAL(4,3), which makes the width of the
--     product depend on the literal that happens to be written.
--   * The model is merge-incremental on Databricks (Delta) and a plain table
--     elsewhere, so the Postgres CI path is unchanged.

{%- set is_databricks = target.type == 'databricks' -%}

{% if is_databricks %}
{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key=['customer_id', 'assessment_date'],
        file_format='delta',
        partition_by=['assessment_date'],
        tblproperties={
            'delta.autoOptimize.optimizeWrite': 'true',
            'delta.autoOptimize.autoCompact': 'true'
        },
        post_hook=[
            "optimize {{ this }} zorder by (customer_id, risk_rating)"
        ]
    )
}}
{% endif %}

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
        cast(
            case rf.derived_risk_rating
                when 'A' then 0.002
                when 'B' then 0.010
                when 'C' then 0.030
                when 'D' then 0.080
                when 'E' then 0.150
                else 0.100
            end
            as decimal(9, 6)
        ) as probability_of_default,

        -- LGD (Loss Given Default)
        cast(
            case
                when rf.segment = 'RETAIL' then 0.45
                when rf.segment = 'WEALTH' then 0.35
                when rf.segment = 'CORPORATE' then 0.40
                else 0.45
            end
            as decimal(9, 6)
        ) as loss_given_default,

        -- EAD (Exposure At Default) - simplified as total transaction volume
        cast(rf.total_transaction_volume as decimal(18, 2)) as exposure_at_default

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
    -- Risk weight is folded first (max decimal(24,13)) and narrowed before
    -- being applied to EAD, keeping the widest intermediate at decimal(37,10).
    cast(
        cast(
            probability_of_default * loss_given_default * cast(12.5 as decimal(4, 1))
            as decimal(18, 8)
        ) * exposure_at_default
        as decimal(38, 6)
    ) as risk_weighted_assets,

    -- Expected Loss = PD * LGD * EAD
    cast(
        cast(probability_of_default * loss_given_default as decimal(18, 10))
        * exposure_at_default
        as decimal(38, 6)
    ) as expected_loss,

    cast(current_date as date) as assessment_date,
    {{ dbt.current_timestamp() }} as etl_loaded_ts

from scored
