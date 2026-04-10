/*
 * Staging Model: Databricks Pharmacy Claims → Tuva Input Layer
 *
 * Source: databricks/ddl/02_raw_tables.sql (claims_raw.raw_pharmacy_claim)
 * Target: Tuva input_layer.pharmacy_claim
 *
 * Key drift resolutions:
 *   - patient_id → person_id (naming drift)
 *   - fill_date → dispensing_date (Tuva naming)
 *   - DOUBLE → NUMBER(18,2) for financial amounts (DRIFT-006)
 *   - prescriber_npi → prescribing_provider_npi (naming)
 */

with source as (
    select * from {{ source('databricks_raw', 'raw_pharmacy_claim') }}
),

renamed as (
    select
        -- === Tuva pharmacy_claim required fields ===
        cast(claim_id as varchar)                           as claim_id,
        cast(patient_id as varchar)                         as person_id,
        fill_date                                           as dispensing_date,
        cast(ndc_code as varchar)                           as ndc_code,
        cast(quantity_dispensed as number(10,3))             as quantity,
        cast(days_supply as integer)                        as days_supply,
        cast(refill_number as integer)                      as refill_number,

        -- Claim status
        cast(claim_status as varchar)                       as claim_status,

        -- Provider NPIs (naming drift)
        cast(prescriber_npi as varchar)                     as prescribing_provider_npi,
        cast(pharmacy_npi as varchar)                       as dispensing_provider_npi,

        -- Drug information
        cast(drug_name as varchar)                          as drug_name,
        cast(generic_name as varchar)                       as generic_name,
        cast(therapeutic_class as varchar)                   as therapeutic_class,
        cast(daw_code as varchar)                           as daw_code,

        -- Financial amounts (DRIFT-006: DOUBLE → NUMBER(18,2))
        cast(paid_amount as number(18,2))                   as paid_amount,
        cast(allowed_amount as number(18,2))                as allowed_amount,
        cast(billed_amount as number(18,2))                 as charge_amount,
        cast(copay_amount as number(18,2))                  as copayment_amount,
        cast(coinsurance_amount as number(18,2))            as coinsurance_amount,
        cast(deductible_amount as number(18,2))             as deductible_amount,
        cast(ingredient_cost as number(18,2))               as ingredient_cost,
        cast(dispensing_fee as number(18,2))                as dispensing_fee,
        cast(null as number(18,2))                          as plan_paid_amount,

        -- Diagnosis code
        cast(icd_diagnosis_code as varchar)                 as diagnosis_code,
        'icd-10-cm'                                         as diagnosis_code_type,

        -- Adjustment tracking
        cast(original_claim_id as varchar)                  as original_claim_id,

        -- === Source tracking ===
        'databricks'                                        as data_source,
        cast('databricks' as varchar)                       as source_platform,
        load_timestamp                                      as source_load_timestamp

    from source
)

select * from renamed
