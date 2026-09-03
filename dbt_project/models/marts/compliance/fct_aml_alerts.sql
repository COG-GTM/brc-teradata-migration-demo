-- Migrated from: teradata/stored_procedures/sp_aml_screening.sql
--   and teradata/ddl/05_mart_tables.sql (MART_AML_ALERTS)
-- This mart materializes the AML screening flags for compliance reporting.
--
-- Physical design:
--   Teradata: PPI on alert_date, PI on alert_id
--   Databricks: Delta table, liquid clustering on (alert_date, customer_id),
--               merge incremental strategy on alert_id
--   Other targets: dialect-neutral delete+insert incremental

{% if target.type == 'databricks' %}
    {{
        config(
            materialized='incremental',
            unique_key='alert_id',
            incremental_strategy='merge',
            file_format='delta',
            liquid_clustered_by=['alert_date', 'customer_id'],
            tblproperties={
                'delta.autoOptimize.optimizeWrite': 'true',
                'delta.autoOptimize.autoCompact': 'true'
            }
        )
    }}
{% else %}
    {{
        config(
            materialized='incremental',
            unique_key='alert_id',
            incremental_strategy='delete+insert'
        )
    }}
{% endif %}

with screening_flags as (

    select * from {{ ref('int_aml_screening_flags') }}

    {% if is_incremental() %}
    where alert_date > (select max(alert_date) from {{ this }})
    {% endif %}

),

normalised as (

    -- Sanctions / PEP style codes are normalised to upper case here rather than
    -- relying on the engine's collation: Snowflake string comparison and
    -- Databricks (Spark) string comparison are both case- and whitespace-
    -- sensitive, while Teradata's default NOT CASESPECIFIC collation is not.
    select
        alert_id,
        customer_id,
        customer_name,
        alert_date,
        upper(trim(alert_type)) as alert_type,
        upper(trim(alert_severity)) as alert_severity,
        supporting_transaction_count,
        total_amount,
        alert_description,
        screened_at

    from screening_flags

)

select
    alert_id,
    customer_id,
    customer_name,
    alert_date,
    alert_type,
    alert_severity,
    supporting_transaction_count,
    total_amount,
    alert_description,
    'OPEN' as alert_status,
    screened_at,
    current_timestamp as etl_loaded_ts

from normalised
