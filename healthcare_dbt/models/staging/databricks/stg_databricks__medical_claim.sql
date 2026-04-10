/*
 * Staging Model: Databricks Medical Claims → Tuva Input Layer
 *
 * Source: databricks/ddl/02_raw_tables.sql (claims_raw.raw_medical_claim)
 * Target: Tuva input_layer.medical_claim
 *
 * Key drift resolutions:
 *   - patient_id → person_id (DRIFT: Databricks uses patient_id instead of member_id)
 *   - claim_start_date / claim_end_date → claim_start_date / claim_end_date (matches Tuva)
 *   - diagnosis_codes ARRAY<STRING> → individual columns via LATERAL FLATTEN
 *   - DOUBLE → NUMBER(18,2) for financial amounts (DRIFT-006)
 *   - claim_type full text → Tuva standard lowercase (DRIFT-007)
 */

with source as (
    select * from {{ source('databricks_raw', 'raw_medical_claim') }}
),

renamed as (
    select
        -- === Tuva medical_claim required fields ===
        cast(claim_id as varchar)                           as claim_id,
        cast(claim_line_number as integer)                  as claim_line_number,
        cast(patient_id as varchar)                         as person_id,

        -- Claim type normalization (DRIFT-007: full text → Tuva standard)
        case upper(claim_type)
            when 'INSTITUTIONAL' then 'institutional'
            when 'PROFESSIONAL' then 'professional'
            when 'OUTPATIENT' then 'professional'
            else lower(claim_type)
        end                                                 as claim_type,

        -- Date columns (Databricks naming already close to Tuva)
        claim_start_date                                    as claim_start_date,
        claim_end_date                                      as claim_end_date,
        admission_date                                      as admission_date,
        discharge_date                                      as discharge_date,

        -- Claim status
        cast(claim_status as varchar)                       as claim_status,

        -- Provider NPIs (naming drift)
        cast(rendering_provider_npi as varchar)             as rendering_npi,
        cast(billing_provider_npi as varchar)               as billing_npi,
        cast(facility_npi as varchar)                       as facility_npi,

        -- Procedure/service codes (naming drift)
        cast(place_of_service_code as varchar)              as place_of_service_code,
        cast(type_of_bill_code as varchar)                  as bill_type_code,
        cast(drg_code as varchar)                           as ms_drg_code,
        cast(revenue_code as varchar)                       as revenue_center_code,
        cast(procedure_code as varchar)                     as hcpcs_code,
        cast(procedure_modifier_1 as varchar)               as hcpcs_modifier_1,
        cast(procedure_modifier_2 as varchar)               as hcpcs_modifier_2,

        -- Diagnosis codes: ARRAY<STRING> → individual columns
        -- Databricks stores as VARIANT (migrated from ARRAY<STRING>)
        -- Use array indexing to extract individual codes
        cast(diagnosis_codes[0]::varchar as varchar)        as diagnosis_code_1,
        cast(diagnosis_codes[1]::varchar as varchar)        as diagnosis_code_2,
        cast(diagnosis_codes[2]::varchar as varchar)        as diagnosis_code_3,
        cast(diagnosis_codes[3]::varchar as varchar)        as diagnosis_code_4,
        cast(diagnosis_codes[4]::varchar as varchar)        as diagnosis_code_5,
        cast(diagnosis_codes[5]::varchar as varchar)        as diagnosis_code_6,
        cast(diagnosis_codes[6]::varchar as varchar)        as diagnosis_code_7,
        cast(diagnosis_codes[7]::varchar as varchar)        as diagnosis_code_8,
        cast(diagnosis_codes[8]::varchar as varchar)        as diagnosis_code_9,
        cast(diagnosis_codes[9]::varchar as varchar)        as diagnosis_code_10,
        cast(diagnosis_codes[10]::varchar as varchar)       as diagnosis_code_11,
        cast(diagnosis_codes[11]::varchar as varchar)       as diagnosis_code_12,
        cast(diagnosis_codes[12]::varchar as varchar)       as diagnosis_code_13,
        cast(diagnosis_codes[13]::varchar as varchar)       as diagnosis_code_14,
        cast(diagnosis_codes[14]::varchar as varchar)       as diagnosis_code_15,
        cast(diagnosis_codes[15]::varchar as varchar)       as diagnosis_code_16,
        cast(diagnosis_codes[16]::varchar as varchar)       as diagnosis_code_17,
        cast(diagnosis_codes[17]::varchar as varchar)       as diagnosis_code_18,
        cast(diagnosis_codes[18]::varchar as varchar)       as diagnosis_code_19,
        cast(diagnosis_codes[19]::varchar as varchar)       as diagnosis_code_20,
        cast(diagnosis_codes[20]::varchar as varchar)       as diagnosis_code_21,
        cast(diagnosis_codes[21]::varchar as varchar)       as diagnosis_code_22,
        cast(diagnosis_codes[22]::varchar as varchar)       as diagnosis_code_23,
        cast(diagnosis_codes[23]::varchar as varchar)       as diagnosis_code_24,
        cast(diagnosis_codes[24]::varchar as varchar)       as diagnosis_code_25,
        coalesce(cast(diagnosis_code_type as varchar), 'icd-10-cm')
                                                            as diagnosis_code_type,

        -- Financial amounts (DRIFT-006: DOUBLE → NUMBER(18,2) for precision)
        cast(billed_amount as number(18,2))                 as charge_amount,
        cast(allowed_amount as number(18,2))                as allowed_amount,
        cast(paid_amount as number(18,2))                   as paid_amount,
        cast(coinsurance_amount as number(18,2))            as coinsurance_amount,
        cast(copay_amount as number(18,2))                  as copayment_amount,
        cast(deductible_amount as number(18,2))             as deductible_amount,

        -- Adjustment tracking
        cast(original_claim_id as varchar)                  as original_claim_id,
        cast(null as varchar)                               as adjustment_reason_code,

        -- === Source tracking ===
        'databricks'                                        as data_source,
        cast('databricks' as varchar)                       as source_platform,
        load_timestamp                                      as source_load_timestamp

    from source
)

select * from renamed
