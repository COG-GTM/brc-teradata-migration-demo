/*
 * Intermediate Model: ADR-Deduped Medical Claims (Unified)
 *
 * Applies the CMS-correct ADR deduplication priority across all source
 * platforms, resolving DRIFT-001.
 *
 * Priority: PAID=1, ADJUSTED=2, DENIED=3, REVERSED=4
 *
 * This model unions all platform staging models and applies a single,
 * consistent dedup logic. When the same claim exists across multiple
 * platforms, the primary_source_platform var controls which is preferred.
 *
 * Migration source:
 *   - teradata/claims/stored_procedures/sp_claims_adr_dedup.sql
 *   - databricks/notebooks/02_adr_deduplication.py
 *   - snowflake/stored_procedures/sp_claims_adr_dedup.sql
 */

{{ config(
    materialized='table',
    tags=['intermediate', 'adr_dedup', 'medical_claim']
) }}

with all_platform_claims as (
    -- Union all platform staging models into a single stream
    select *, 1 as platform_priority from {{ ref('stg_snowflake__medical_claim') }}
    union all
    select
        claim_id, claim_line_number, person_id, claim_type,
        claim_start_date, claim_end_date, admission_date, discharge_date,
        claim_status, rendering_npi, billing_npi, facility_npi,
        place_of_service_code, bill_type_code, ms_drg_code,
        revenue_center_code, hcpcs_code, hcpcs_modifier_1, hcpcs_modifier_2,
        diagnosis_code_1, diagnosis_code_2, diagnosis_code_3,
        diagnosis_code_4, diagnosis_code_5, diagnosis_code_6,
        diagnosis_code_7, diagnosis_code_8, diagnosis_code_9,
        diagnosis_code_10, diagnosis_code_11, diagnosis_code_12,
        diagnosis_code_13, diagnosis_code_14, diagnosis_code_15,
        diagnosis_code_16, diagnosis_code_17, diagnosis_code_18,
        diagnosis_code_19, diagnosis_code_20, diagnosis_code_21,
        diagnosis_code_22, diagnosis_code_23, diagnosis_code_24,
        diagnosis_code_25, diagnosis_code_type,
        charge_amount, allowed_amount, paid_amount,
        coinsurance_amount, copayment_amount, deductible_amount,
        original_claim_id, adjustment_reason_code,
        data_source, source_platform, source_load_timestamp,
        -- Snowflake-only columns: default to null for other platforms
        cast(null as number(18,2)) as net_paid_amount,
        cast(null as number(18,2)) as cob_amount,
        cast(null as number(18,2)) as withhold_amount,
        cast(null as date) as adjudication_date,
        2 as platform_priority
    from {{ ref('stg_databricks__medical_claim') }}
    union all
    select
        claim_id, claim_line_number, person_id, claim_type,
        claim_start_date, claim_end_date, admission_date, discharge_date,
        claim_status, rendering_npi, billing_npi, facility_npi,
        place_of_service_code, bill_type_code, ms_drg_code,
        revenue_center_code, hcpcs_code, hcpcs_modifier_1, hcpcs_modifier_2,
        diagnosis_code_1, diagnosis_code_2, diagnosis_code_3,
        diagnosis_code_4, diagnosis_code_5, diagnosis_code_6,
        diagnosis_code_7, diagnosis_code_8, diagnosis_code_9,
        diagnosis_code_10, diagnosis_code_11, diagnosis_code_12,
        diagnosis_code_13, diagnosis_code_14, diagnosis_code_15,
        diagnosis_code_16, diagnosis_code_17, diagnosis_code_18,
        diagnosis_code_19, diagnosis_code_20, diagnosis_code_21,
        diagnosis_code_22, diagnosis_code_23, diagnosis_code_24,
        diagnosis_code_25, diagnosis_code_type,
        charge_amount, allowed_amount, paid_amount,
        coinsurance_amount, copayment_amount, deductible_amount,
        original_claim_id, adjustment_reason_code,
        data_source, source_platform, source_load_timestamp,
        cast(null as number(18,2)) as net_paid_amount,
        cast(null as number(18,2)) as cob_amount,
        cast(null as number(18,2)) as withhold_amount,
        cast(null as date) as adjudication_date,
        3 as platform_priority
    from {{ ref('stg_teradata__medical_claim') }}
),

with_adr_priority as (
    select
        *,
        -- Apply CMS-correct ADR priority (DRIFT-001 resolution)
        {{ adr_priority('claim_status') }}                  as adr_priority_score,
        -- Dedup within each platform first
        row_number() over (
            partition by
                source_platform,
                coalesce(original_claim_id, claim_id),
                claim_line_number
            order by
                {{ adr_priority('claim_status') }} asc,
                source_load_timestamp desc
        )                                                   as platform_dedup_rn
    from all_platform_claims
),

platform_deduped as (
    -- Keep only the highest-priority claim version per platform
    select * from with_adr_priority
    where platform_dedup_rn = 1
),

cross_platform_deduped as (
    -- When the same claim exists across platforms, prefer the primary platform
    select
        *,
        row_number() over (
            partition by
                coalesce(original_claim_id, claim_id),
                claim_line_number
            order by
                platform_priority asc,
                source_load_timestamp desc
        )                                                   as cross_platform_rn
    from platform_deduped
)

select
    claim_id,
    claim_line_number,
    person_id,
    claim_type,
    claim_start_date,
    claim_end_date,
    admission_date,
    discharge_date,
    claim_status,
    adr_priority_score,
    rendering_npi,
    billing_npi,
    facility_npi,
    place_of_service_code,
    bill_type_code,
    ms_drg_code,
    revenue_center_code,
    hcpcs_code,
    hcpcs_modifier_1,
    hcpcs_modifier_2,
    diagnosis_code_1,
    diagnosis_code_2,
    diagnosis_code_3,
    diagnosis_code_4,
    diagnosis_code_5,
    diagnosis_code_6,
    diagnosis_code_7,
    diagnosis_code_8,
    diagnosis_code_9,
    diagnosis_code_10,
    diagnosis_code_11,
    diagnosis_code_12,
    diagnosis_code_13,
    diagnosis_code_14,
    diagnosis_code_15,
    diagnosis_code_16,
    diagnosis_code_17,
    diagnosis_code_18,
    diagnosis_code_19,
    diagnosis_code_20,
    diagnosis_code_21,
    diagnosis_code_22,
    diagnosis_code_23,
    diagnosis_code_24,
    diagnosis_code_25,
    diagnosis_code_type,
    charge_amount,
    allowed_amount,
    paid_amount,
    coinsurance_amount,
    copayment_amount,
    deductible_amount,
    net_paid_amount,
    cob_amount,
    withhold_amount,
    original_claim_id,
    adjustment_reason_code,
    adjudication_date,
    data_source,
    source_platform,
    source_load_timestamp
from cross_platform_deduped
where cross_platform_rn = 1
