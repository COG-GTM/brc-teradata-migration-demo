-- =============================================================================
-- Migrated from: teradata/ddl/04_warehouse_tables.sql (FCT_TRANSACTION)
--   and dbt_project/models/marts/finance/fct_daily_transactions.sql
-- Teradata constructs replaced:
--   Surrogate key joins via IDENTITY columns -> dbt_utils.generate_surrogate_key
--   PPI by RANGE_N on date_key              -> Snowflake clustering
-- Now operates on unified cross-platform enriched transactions.
-- =============================================================================

{{
    config(
        materialized='incremental',
        unique_key='transaction_id',
        cluster_by=['transaction_date']
    )
}}

with enriched as (

    select * from {{ ref('int_transaction_enriched') }}

    {% if is_incremental() %}
    where transaction_date > (select max(transaction_date) from {{ this }})
    {% endif %}

)

select
    transaction_id,
    account_id,
    customer_id,
    transaction_date,
    amount,
    signed_amount,
    currency,
    transaction_type,
    description,
    channel,
    value_band,
    account_type,
    branch_code,
    customer_segment,
    customer_risk_rating,
    counterparty_id,
    counterparty_name,
    counterparty_country,
    is_high_risk_counterparty,
    is_reportable_transaction,
    transaction_source_platform,
    current_timestamp as etl_loaded_ts

from enriched
