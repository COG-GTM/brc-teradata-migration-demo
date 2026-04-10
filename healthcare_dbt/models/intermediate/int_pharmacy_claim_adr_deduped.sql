/*
 * Intermediate Model: ADR-Deduped Pharmacy Claims (Unified)
 *
 * Same ADR dedup logic as medical claims (DRIFT-001 resolution).
 * Priority: PAID=1, ADJUSTED=2, DENIED=3, REVERSED=4
 *
 * Migration source:
 *   - teradata/claims/stored_procedures/sp_claims_adr_dedup.sql (pharmacy branch)
 *   - databricks/notebooks/02_adr_deduplication.py (pharmacy branch)
 *   - snowflake/stored_procedures/sp_claims_adr_dedup.sql (PHARMACY category)
 */

{{ config(
    materialized='table',
    tags=['intermediate', 'adr_dedup', 'pharmacy_claim']
) }}

with all_platform_rx as (
    select *, 1 as platform_priority from {{ ref('stg_snowflake__pharmacy_claim') }}
    union all
    select *, 2 as platform_priority from {{ ref('stg_databricks__pharmacy_claim') }}
    union all
    select *, 3 as platform_priority from {{ ref('stg_teradata__pharmacy_claim') }}
),

with_adr_priority as (
    select
        *,
        {{ adr_priority('claim_status') }}                  as adr_priority_score,
        row_number() over (
            partition by
                source_platform,
                coalesce(original_claim_id, claim_id)
            order by
                {{ adr_priority('claim_status') }} asc,
                source_load_timestamp desc
        )                                                   as platform_dedup_rn
    from all_platform_rx
),

platform_deduped as (
    select * from with_adr_priority
    where platform_dedup_rn = 1
),

cross_platform_deduped as (
    select
        *,
        row_number() over (
            partition by coalesce(original_claim_id, claim_id)
            order by
                platform_priority asc,
                source_load_timestamp desc
        )                                                   as cross_platform_rn
    from platform_deduped
)

select
    claim_id,
    person_id,
    dispensing_date,
    ndc_code,
    quantity,
    days_supply,
    refill_number,
    claim_status,
    adr_priority_score,
    prescribing_provider_npi,
    dispensing_provider_npi,
    drug_name,
    generic_name,
    therapeutic_class,
    daw_code,
    paid_amount,
    allowed_amount,
    charge_amount,
    copayment_amount,
    coinsurance_amount,
    deductible_amount,
    ingredient_cost,
    dispensing_fee,
    plan_paid_amount,
    diagnosis_code,
    diagnosis_code_type,
    original_claim_id,
    data_source,
    source_platform,
    source_load_timestamp
from cross_platform_deduped
where cross_platform_rn = 1
