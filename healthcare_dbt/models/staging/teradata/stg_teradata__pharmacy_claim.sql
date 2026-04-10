/*
 * Staging Model: Teradata Pharmacy Claims → Tuva Input Layer
 *
 * Source: teradata/claims/ddl/02_raw_tables.sql (CLAIMS_RAW.RAW_PHARMACY_CLAIM)
 * Target: Tuva input_layer.pharmacy_claim
 *
 * Key drift resolutions:
 *   - dispensing_date → dispensing_date (Tuva naming, matches TD)
 *   - member_id → person_id (Tuva naming)
 *   - DECIMAL(18,2) → NUMBER(18,2) (compatible)
 *   - SMALLINT → INTEGER (widening)
 */

with source as (
    select * from {{ source('teradata_raw', 'raw_pharmacy_claim') }}
),

renamed as (
    select
        -- === Tuva pharmacy_claim required fields ===
        cast(claim_id as varchar)                           as claim_id,
        cast(member_id as varchar)                          as person_id,
        dispensing_date                                     as dispensing_date,
        cast(ndc_code as varchar)                           as ndc_code,
        cast(quantity as number(10,3))                      as quantity,
        cast(days_supply as integer)                        as days_supply,
        cast(refill_number as integer)                      as refill_number,

        -- Claim status
        cast(claim_status as varchar)                       as claim_status,

        -- Provider NPIs (naming drift)
        cast(prescribing_npi as varchar)                    as prescribing_provider_npi,
        cast(dispensing_npi as varchar)                     as dispensing_provider_npi,

        -- Drug information
        cast(drug_name as varchar)                          as drug_name,
        cast(generic_name as varchar)                       as generic_name,
        cast(therapeutic_class as varchar)                   as therapeutic_class,
        cast(daw_code as varchar)                           as daw_code,

        -- Financial amounts
        cast(paid_amount as number(18,2))                   as paid_amount,
        cast(allowed_amount as number(18,2))                as allowed_amount,
        cast(charge_amount as number(18,2))                 as charge_amount,
        cast(copay as number(18,2))                         as copayment_amount,
        cast(coinsurance as number(18,2))                   as coinsurance_amount,
        cast(deductible as number(18,2))                    as deductible_amount,
        cast(ingredient_cost as number(18,2))               as ingredient_cost,
        cast(dispensing_fee as number(18,2))                as dispensing_fee,
        cast(plan_paid as number(18,2))                     as plan_paid_amount,

        -- Diagnosis code
        cast(icd_diagnosis_code as varchar)                 as diagnosis_code,
        'icd-10-cm'                                         as diagnosis_code_type,

        -- Adjustment tracking
        cast(original_claim_id as varchar)                  as original_claim_id,

        -- === Source tracking ===
        'teradata'                                          as data_source,
        cast('teradata' as varchar)                         as source_platform,
        load_ts                                             as source_load_timestamp

    from source
)

select * from renamed
