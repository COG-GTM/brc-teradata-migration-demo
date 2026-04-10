/*
 * Tuva Input Layer: Medical Claim
 *
 * Maps the unified intermediate medical claim model to the Tuva
 * input_layer.medical_claim schema. ADR dedup has been applied
 * with the CMS-correct priority order (DRIFT-001 resolved).
 *
 * Tuva reference: https://thetuvaproject.com/data-dictionaries/input-layer
 */

{{ config(
    materialized='table',
    schema='tuva_input',
    tags=['tuva_input', 'medical_claim']
) }}

select
    claim_id,
    claim_line_number,
    person_id,
    claim_type,
    claim_start_date,
    claim_end_date,
    admission_date,
    discharge_date,
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
    data_source
from {{ ref('int_medical_claim_adr_deduped') }}
