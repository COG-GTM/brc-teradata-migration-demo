/*
 * Staging Model: Snowflake Medical Claims → Tuva Input Layer
 *
 * Source: snowflake/ddl/02_raw_tables.sql (CLAIMS_DW.RAW.RAW_MEDICAL_CLAIM)
 * Target: Tuva input_layer.medical_claim
 *
 * Key drift resolutions:
 *   - start_date / end_date → claim_start_date / claim_end_date (naming)
 *   - status_code → claim_status (naming)
 *   - Hybrid diagnosis: COALESCE individual columns with VARIANT array extraction
 *   - Only 10 individual columns (vs 25 in Teradata) - fill 11-25 from VARIANT
 *   - net_paid_amount, cob_amount, withhold_amount are Snowflake-only extras
 *   - ADR priority PAID>ADJUSTED>REVERSED>DENIED (DRIFT-001: wrong order for
 *     REVERSED/DENIED; dedup with correct order applied in intermediate layer)
 */

with source as (
    select * from {{ source('snowflake_raw', 'raw_medical_claim') }}
),

renamed as (
    select
        -- === Tuva medical_claim required fields ===
        cast(claim_id as varchar)                           as claim_id,
        cast(claim_line_number as integer)                  as claim_line_number,
        cast(member_id as varchar)                          as person_id,

        -- Claim type normalization (DRIFT-007)
        case upper(claim_type)
            when 'I' then 'institutional'
            when 'P' then 'professional'
            else lower(claim_type)
        end                                                 as claim_type,

        -- Date columns (naming drift: start_date → claim_start_date)
        start_date                                          as claim_start_date,
        end_date                                            as claim_end_date,
        admission_date                                      as admission_date,
        discharge_date                                      as discharge_date,

        -- Claim status (naming drift: status_code → claim_status)
        cast(status_code as varchar)                        as claim_status,

        -- Provider NPIs
        cast(rendering_provider_npi as varchar)             as rendering_npi,
        cast(billing_provider_npi as varchar)               as billing_npi,
        cast(null as varchar)                               as facility_npi,

        -- Procedure/service codes
        cast(place_of_service as varchar)                   as place_of_service_code,
        cast(bill_type as varchar)                          as bill_type_code,
        cast(drg_code as varchar)                           as ms_drg_code,
        cast(revenue_code as varchar)                       as revenue_center_code,
        cast(cpt_code as varchar)                           as hcpcs_code,
        cast(cpt_modifier_1 as varchar)                     as hcpcs_modifier_1,
        cast(cpt_modifier_2 as varchar)                     as hcpcs_modifier_2,

        -- Diagnosis codes: Hybrid approach (DRIFT-003)
        -- Use individual columns for 1-10, then VARIANT for 11-25
        coalesce(cast(icd_diagnosis_code_1 as varchar),
                 cast(diagnosis_codes[0]::varchar as varchar))   as diagnosis_code_1,
        coalesce(cast(icd_diagnosis_code_2 as varchar),
                 cast(diagnosis_codes[1]::varchar as varchar))   as diagnosis_code_2,
        coalesce(cast(icd_diagnosis_code_3 as varchar),
                 cast(diagnosis_codes[2]::varchar as varchar))   as diagnosis_code_3,
        coalesce(cast(icd_diagnosis_code_4 as varchar),
                 cast(diagnosis_codes[3]::varchar as varchar))   as diagnosis_code_4,
        coalesce(cast(icd_diagnosis_code_5 as varchar),
                 cast(diagnosis_codes[4]::varchar as varchar))   as diagnosis_code_5,
        coalesce(cast(icd_diagnosis_code_6 as varchar),
                 cast(diagnosis_codes[5]::varchar as varchar))   as diagnosis_code_6,
        coalesce(cast(icd_diagnosis_code_7 as varchar),
                 cast(diagnosis_codes[6]::varchar as varchar))   as diagnosis_code_7,
        coalesce(cast(icd_diagnosis_code_8 as varchar),
                 cast(diagnosis_codes[7]::varchar as varchar))   as diagnosis_code_8,
        coalesce(cast(icd_diagnosis_code_9 as varchar),
                 cast(diagnosis_codes[8]::varchar as varchar))   as diagnosis_code_9,
        coalesce(cast(icd_diagnosis_code_10 as varchar),
                 cast(diagnosis_codes[9]::varchar as varchar))   as diagnosis_code_10,
        -- Positions 11-25: only available from VARIANT array
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
        'icd-10-cm'                                         as diagnosis_code_type,

        -- Financial amounts (NUMBER(18,2) - already correct precision)
        cast(billed_amount as number(18,2))                 as charge_amount,
        cast(allowed_amount as number(18,2))                as allowed_amount,
        cast(paid_amount as number(18,2))                   as paid_amount,
        cast(coinsurance_amount as number(18,2))            as coinsurance_amount,
        cast(copay_amount as number(18,2))                  as copayment_amount,
        cast(deductible_amount as number(18,2))             as deductible_amount,

        -- Snowflake-only extra financial columns (preserved for completeness)
        cast(net_paid_amount as number(18,2))               as net_paid_amount,
        cast(cob_amount as number(18,2))                    as cob_amount,
        cast(withhold_amount as number(18,2))               as withhold_amount,

        -- Adjustment tracking
        cast(original_claim_id as varchar)                  as original_claim_id,
        cast(adjustment_reason_code as varchar)             as adjustment_reason_code,
        adjudication_date                                   as adjudication_date,

        -- === Source tracking ===
        'snowflake'                                         as data_source,
        cast('snowflake' as varchar)                        as source_platform,
        load_timestamp                                      as source_load_timestamp

    from source
)

select * from renamed
