-- =============================================================================
-- SCD Type 2 snapshot for account status changes.
-- Migrated from: teradata/stored_procedures/sp_daily_transaction_load.sql
--   (MERGE INTO DIM_ACCOUNT section)
-- Teradata MERGE replaced with dbt snapshot check strategy.
-- =============================================================================

{% snapshot snap_account_status %}

{{
    config(
        target_schema='snapshots',
        unique_key='account_id',
        strategy='check',
        check_cols=['status', 'credit_limit', 'overdraft_limit']
    )
}}

select
    account_id,
    customer_id,
    account_type,
    currency,
    branch_code,
    status,
    open_date,
    close_date,
    credit_limit,
    overdraft_limit

from {{ ref('int_unified_account') }}

{% endsnapshot %}
