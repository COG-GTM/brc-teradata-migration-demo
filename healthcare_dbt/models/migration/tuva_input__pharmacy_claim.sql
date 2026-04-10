/*
 * Tuva Input Layer: Pharmacy Claim
 *
 * Maps the unified intermediate pharmacy claim model to the Tuva
 * input_layer.pharmacy_claim schema.
 *
 * Tuva reference: https://thetuvaproject.com/data-dictionaries/input-layer
 */

{{ config(
    materialized='table',
    schema='tuva_input',
    tags=['tuva_input', 'pharmacy_claim']
) }}

select
    claim_id,
    person_id,
    dispensing_date,
    ndc_code,
    quantity,
    days_supply,
    refill_number,
    prescribing_provider_npi,
    dispensing_provider_npi,
    paid_amount,
    allowed_amount,
    charge_amount,
    copayment_amount,
    coinsurance_amount,
    deductible_amount,
    ingredient_cost,
    dispensing_fee,
    diagnosis_code,
    diagnosis_code_type,
    data_source
from {{ ref('int_pharmacy_claim_adr_deduped') }}
