-- SCD Type 2 snapshot for customer risk rating changes.
-- Migrated from: teradata/stored_procedures/sp_daily_transaction_load.sql
--   (SCD2 cursor-based logic for DIM_CUSTOMER).
-- Teradata constructs replaced:
--   Cursor loop with UPDATE/INSERT -> dbt snapshot with check strategy
--   PERIOD data type               -> dbt_valid_from / dbt_valid_to
--   is_current flag                 -> derived from dbt_valid_to IS NULL
--
-- Platform notes:
--   Databricks snapshots require a transactional file format; dbt-databricks
--   only supports the snapshot merge on Delta, so file_format='delta' is set
--   explicitly (the adapter default can be parquet on non-Unity workspaces).
--   invalidate_hard_deletes closes out rows removed from the source; on
--   Databricks this also runs through the Delta merge, so it is enabled on
--   every target to keep the SCD2 semantics identical across platforms.

{% snapshot snap_customer_risk_rating %}

{% if target.type == 'databricks' %}
    {{
        config(
            target_schema='snapshots',
            unique_key='customer_id',
            strategy='check',
            check_cols=['risk_rating', 'kyc_status', 'segment'],
            invalidate_hard_deletes=True,
            file_format='delta',
            tblproperties={
                'delta.autoOptimize.optimizeWrite': 'true',
                'delta.autoOptimize.autoCompact': 'true'
            }
        )
    }}
{% else %}
    {{
        config(
            target_schema='snapshots',
            unique_key='customer_id',
            strategy='check',
            check_cols=['risk_rating', 'kyc_status', 'segment'],
            invalidate_hard_deletes=True
        )
    }}
{% endif %}

select
    customer_id,
    first_name,
    last_name,
    date_of_birth,
    upper(trim(nationality)) as nationality,
    upper(trim(kyc_status)) as kyc_status,
    upper(trim(risk_rating)) as risk_rating,
    upper(trim(segment)) as segment,
    onboarding_date

from {{ ref('stg_customers') }}

{% endsnapshot %}
