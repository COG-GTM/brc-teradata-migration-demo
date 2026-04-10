/*
 * Staging Model: Teradata Medical Claims → Tuva Input Layer
 *
 * Source: teradata/claims/ddl/02_raw_tables.sql (CLAIMS_RAW.RAW_MEDICAL_CLAIM)
 * Target: Tuva input_layer.medical_claim
 *
 * Key drift resolutions:
 *   - service_date_from → claim_start_date (naming)
 *   - service_date_to → claim_end_date (naming)
 *   - claim_status → claim_status (no change, but value encoding differs)
 *   - 25 individual icd_diagnosis_code_N columns → diagnosis_code_1..25
 *   - DECIMAL(18,2) → NUMBER(18,2) (compatible)
 *   - charge_amount → charge_amount (Tuva naming)
 *   - ADR dedup: Teradata has WRONG priority (DRIFT-001). Staging exposes
 *     raw data; dedup is applied in intermediate layer with correct priority.
 */

with source as (
    select * from {{ source('teradata_raw', 'raw_medical_claim') }}
),

renamed as (
    select
        -- === Tuva medical_claim required fields ===
        cast(claim_id as varchar)                           as claim_id,
        cast(claim_line_number as integer)                  as claim_line_number,
        cast(member_id as varchar)                          as person_id,

        -- Claim type normalization (DRIFT-007: I/P/O → Tuva standard)
        case upper(claim_type)
            when 'I' then 'institutional'
            when 'P' then 'professional'
            when 'O' then 'professional'  -- Outpatient mapped to professional per CMS
            else lower(claim_type)
        end                                                 as claim_type,

        -- Date columns (naming drift resolution)
        service_date_from                                   as claim_start_date,
        service_date_to                                     as claim_end_date,
        admission_date                                      as admission_date,
        discharge_date                                      as discharge_date,

        -- Claim status (raw - dedup applied in intermediate)
        cast(claim_status as varchar)                       as claim_status,

        -- Provider NPIs
        cast(rendering_npi as varchar)                      as rendering_npi,
        cast(billing_npi as varchar)                        as billing_npi,
        cast(facility_npi as varchar)                       as facility_npi,

        -- Procedure/service codes
        cast(place_of_service as varchar)                   as place_of_service_code,
        cast(bill_type as varchar)                          as bill_type_code,
        cast(ms_drg as varchar)                             as ms_drg_code,
        cast(revenue_center_code as varchar)                as revenue_center_code,
        cast(hcpcs_code as varchar)                         as hcpcs_code,
        cast(cpt_modifier_1 as varchar)                     as hcpcs_modifier_1,
        cast(cpt_modifier_2 as varchar)                     as hcpcs_modifier_2,

        -- Diagnosis codes (25 individual columns → Tuva diagnosis_code_1..25)
        cast(icd_diagnosis_code_1 as varchar)               as diagnosis_code_1,
        cast(icd_diagnosis_code_2 as varchar)               as diagnosis_code_2,
        cast(icd_diagnosis_code_3 as varchar)               as diagnosis_code_3,
        cast(icd_diagnosis_code_4 as varchar)               as diagnosis_code_4,
        cast(icd_diagnosis_code_5 as varchar)               as diagnosis_code_5,
        cast(icd_diagnosis_code_6 as varchar)               as diagnosis_code_6,
        cast(icd_diagnosis_code_7 as varchar)               as diagnosis_code_7,
        cast(icd_diagnosis_code_8 as varchar)               as diagnosis_code_8,
        cast(icd_diagnosis_code_9 as varchar)               as diagnosis_code_9,
        cast(icd_diagnosis_code_10 as varchar)              as diagnosis_code_10,
        cast(icd_diagnosis_code_11 as varchar)              as diagnosis_code_11,
        cast(icd_diagnosis_code_12 as varchar)              as diagnosis_code_12,
        cast(icd_diagnosis_code_13 as varchar)              as diagnosis_code_13,
        cast(icd_diagnosis_code_14 as varchar)              as diagnosis_code_14,
        cast(icd_diagnosis_code_15 as varchar)              as diagnosis_code_15,
        cast(icd_diagnosis_code_16 as varchar)              as diagnosis_code_16,
        cast(icd_diagnosis_code_17 as varchar)              as diagnosis_code_17,
        cast(icd_diagnosis_code_18 as varchar)              as diagnosis_code_18,
        cast(icd_diagnosis_code_19 as varchar)              as diagnosis_code_19,
        cast(icd_diagnosis_code_20 as varchar)              as diagnosis_code_20,
        cast(icd_diagnosis_code_21 as varchar)              as diagnosis_code_21,
        cast(icd_diagnosis_code_22 as varchar)              as diagnosis_code_22,
        cast(icd_diagnosis_code_23 as varchar)              as diagnosis_code_23,
        cast(icd_diagnosis_code_24 as varchar)              as diagnosis_code_24,
        cast(icd_diagnosis_code_25 as varchar)              as diagnosis_code_25,
        'icd-10-cm'                                         as diagnosis_code_type,

        -- Financial amounts (DECIMAL(18,2) → NUMBER(18,2), compatible)
        cast(charge_amount as number(18,2))                 as charge_amount,
        cast(allowed_amount as number(18,2))                as allowed_amount,
        cast(paid_amount as number(18,2))                   as paid_amount,
        cast(coinsurance as number(18,2))                   as coinsurance_amount,
        cast(copay as number(18,2))                         as copayment_amount,
        cast(deductible as number(18,2))                    as deductible_amount,

        -- Adjustment tracking
        cast(original_claim_id as varchar)                  as original_claim_id,
        cast(adjustment_type as varchar)                    as adjustment_reason_code,

        -- === Source tracking ===
        'teradata'                                          as data_source,
        cast('teradata' as varchar)                         as source_platform,
        load_ts                                             as source_load_timestamp

    from source
)

select * from renamed
