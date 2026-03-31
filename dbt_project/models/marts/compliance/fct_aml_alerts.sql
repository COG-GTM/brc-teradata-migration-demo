-- Migrated from: teradata/stored_procedures/sp_aml_screening.sql
--   and teradata/ddl/05_mart_tables.sql (MART_AML_ALERTS)
-- This mart materializes the AML screening flags for compliance reporting.

{{
    config(
        materialized='incremental',
        unique_key='alert_id'
    )
}}

with screening_flags as (

    select * from {{ ref('int_aml_screening_flags') }}

    {% if is_incremental() %}
    where alert_date > (select max(alert_date) from {{ this }})
    {% endif %}

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

from screening_flags
