-- =============================================================================
-- SCD Type 2 snapshot for customer risk rating changes.
-- Migrated from: teradata/stored_procedures/sp_daily_transaction_load.sql
--   (SCD2 cursor-based logic for DIM_CUSTOMER)
-- Teradata constructs replaced:
--   Cursor loop with UPDATE/INSERT -> dbt snapshot with check strategy
--   PERIOD data type               -> dbt_valid_from / dbt_valid_to
--   is_current flag                 -> derived from dbt_valid_to IS NULL
-- Now operates on unified cross-platform customer data.
-- =============================================================================

{% snapshot snap_customer_risk_rating %}

{{
    config(
        target_schema='snapshots',
        unique_key='customer_id',
        strategy='check',
        check_cols=['risk_rating', 'kyc_status', 'segment']
    )
}}

select
    customer_id,
    first_name,
    last_name,
    date_of_birth,
    nationality,
    kyc_status,
    risk_rating,
    segment,
    onboarding_date

from {{ ref('int_unified_customer') }}

{% endsnapshot %}
