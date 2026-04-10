/*
 * Mart Model: Encounter Summary
 *
 * Summarizes encounters by type and month for utilization management
 * and quality measure reporting.
 *
 * Uses the corrected gap-and-island encounter grouping (DRIFT-002 resolution).
 *
 * Migration source:
 *   - teradata/claims/ddl/04_warehouse_tables.sql (CLAIMS_MART.MART_ENCOUNTER_SUMMARY)
 *   - databricks/ddl/04_warehouse_tables.sql (claims_mart.mart_encounter_summary)
 *   - snowflake/ddl/04_warehouse_tables.sql (CLAIMS_DW.MART.MART_ENCOUNTER_SUMMARY)
 */

{{ config(
    materialized='table',
    tags=['marts', 'encounters']
) }}

with encounters as (
    select * from {{ ref('int_encounter_grouped') }}
),

summary as (
    select
        date_trunc('month', encounter_start_date)           as encounter_month,
        encounter_type,
        count(*)                                            as encounter_count,
        count(distinct person_id)                           as member_count,
        sum(total_claim_lines)                              as total_claim_lines,
        sum(total_charge_amount)                            as total_charge_amount,
        sum(total_allowed_amount)                           as total_allowed_amount,
        sum(total_paid_amount)                              as total_paid_amount,
        sum(total_member_liability)                         as total_member_liability,
        avg(total_paid_amount)                              as avg_paid_per_encounter,
        avg(length_of_stay)                                 as avg_length_of_stay,
        max(length_of_stay)                                 as max_length_of_stay,
        avg(total_claim_lines)                              as avg_claims_per_encounter,
        data_source
    from encounters
    group by 1, 2, data_source
)

select
    encounter_month,
    encounter_type,
    encounter_count,
    member_count,
    total_claim_lines,
    total_charge_amount,
    total_allowed_amount,
    total_paid_amount,
    total_member_liability,
    avg_paid_per_encounter,
    avg_length_of_stay,
    max_length_of_stay,
    avg_claims_per_encounter,
    data_source,
    current_timestamp()                                     as etl_load_timestamp
from summary
