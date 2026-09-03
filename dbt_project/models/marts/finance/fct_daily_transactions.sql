-- Migrated from: teradata/ddl/04_warehouse_tables.sql (FCT_TRANSACTION)
-- Teradata constructs replaced:
--   Surrogate key joins via IDENTITY columns -> dbt_utils.generate_surrogate_key
--   PPI by RANGE_N on date_key              -> Snowflake clustering / Databricks partitioning
--
-- Databricks physical design:
--   Delta file format with MERGE incremental strategy on transaction_id, which
--   makes late-arriving corrections idempotent instead of duplicating rows.
--   partition_by transaction_date mirrors the Teradata PPI (RANGE_N over the
--   date key): every downstream finance query filters on a date range, and the
--   daily batch rewrites whole date partitions.
--   ZORDER BY (account_id, counterparty_id) after each run: both are
--   high-cardinality columns used for per-account statements and counterparty
--   exposure lookups, so file skipping within a date partition matters.
--   Snowflake keeps the equivalent automatic clustering key on transaction_date;
--   Postgres CI ignores the physical-design configs and uses delete+insert,
--   since MERGE is not available on every supported Postgres version.

{%- set is_databricks = target.type == 'databricks' -%}

{{
    config(
        materialized='incremental',
        unique_key='transaction_id',
        incremental_strategy='merge' if target.type in ['databricks', 'snowflake'] else 'delete+insert',
        file_format='delta' if is_databricks else none,
        partition_by=['transaction_date'] if is_databricks else none,
        cluster_by=['transaction_date'] if target.type == 'snowflake' else none,
        post_hook=[
            'optimize {{ this }} zorder by (account_id, counterparty_id)'
        ] if is_databricks else []
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
    current_timestamp as etl_loaded_ts

from enriched
