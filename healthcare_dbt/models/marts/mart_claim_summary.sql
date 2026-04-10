/*
 * Mart Model: Claims Summary by Type and Month
 *
 * Aggregates medical and pharmacy claims by type and month for
 * executive dashboards and financial reporting.
 *
 * Migration source:
 *   - teradata/claims/ddl/04_warehouse_tables.sql (CLAIMS_MART.MART_CLAIM_SUMMARY)
 *   - databricks/ddl/04_warehouse_tables.sql (claims_mart.mart_claim_summary)
 *   - snowflake/ddl/04_warehouse_tables.sql (CLAIMS_DW.MART.MART_CLAIM_SUMMARY)
 */

{{ config(
    materialized='table',
    tags=['marts', 'financial', 'claims_summary']
) }}

with medical_claims as (
    select
        date_trunc('month', claim_start_date)               as claim_month,
        claim_type,
        claim_status,
        'medical'                                           as claim_category,
        count(*)                                            as claim_line_count,
        count(distinct claim_id)                            as claim_count,
        count(distinct person_id)                           as member_count,
        sum(coalesce(charge_amount, 0))                     as total_charge_amount,
        sum(coalesce(allowed_amount, 0))                    as total_allowed_amount,
        sum(coalesce(paid_amount, 0))                       as total_paid_amount,
        sum(coalesce(copayment_amount, 0))                  as total_copayment,
        sum(coalesce(coinsurance_amount, 0))                as total_coinsurance,
        sum(coalesce(deductible_amount, 0))                 as total_deductible,
        sum(coalesce(copayment_amount, 0))
            + sum(coalesce(coinsurance_amount, 0))
            + sum(coalesce(deductible_amount, 0))           as total_member_liability,
        data_source
    from {{ ref('int_medical_claim_adr_deduped') }}
    group by 1, 2, 3, data_source
),

pharmacy_claims as (
    select
        date_trunc('month', dispensing_date)                as claim_month,
        'pharmacy'                                          as claim_type,
        claim_status,
        'pharmacy'                                          as claim_category,
        count(*)                                            as claim_line_count,
        count(distinct claim_id)                            as claim_count,
        count(distinct person_id)                           as member_count,
        sum(coalesce(charge_amount, 0))                     as total_charge_amount,
        sum(coalesce(allowed_amount, 0))                    as total_allowed_amount,
        sum(coalesce(paid_amount, 0))                       as total_paid_amount,
        sum(coalesce(copayment_amount, 0))                  as total_copayment,
        sum(coalesce(coinsurance_amount, 0))                as total_coinsurance,
        sum(coalesce(deductible_amount, 0))                 as total_deductible,
        sum(coalesce(copayment_amount, 0))
            + sum(coalesce(coinsurance_amount, 0))
            + sum(coalesce(deductible_amount, 0))           as total_member_liability,
        data_source
    from {{ ref('int_pharmacy_claim_adr_deduped') }}
    group by 1, 2, 3, data_source
),

combined as (
    select * from medical_claims
    union all
    select * from pharmacy_claims
)

select
    claim_month,
    claim_type,
    claim_category,
    claim_status,
    claim_line_count,
    claim_count,
    member_count,
    total_charge_amount,
    total_allowed_amount,
    total_paid_amount,
    total_copayment,
    total_coinsurance,
    total_deductible,
    total_member_liability,
    data_source,
    current_timestamp()                                     as etl_load_timestamp
from combined
