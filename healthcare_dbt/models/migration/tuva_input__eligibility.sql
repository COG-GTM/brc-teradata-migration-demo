/*
 * Tuva Input Layer: Eligibility
 *
 * Maps the unified intermediate eligibility model to the Tuva
 * input_layer.eligibility schema. This is the interface between
 * the consolidated legacy data and the Tuva data model.
 *
 * Tuva reference: https://thetuvaproject.com/data-dictionaries/input-layer
 */

{{ config(
    materialized='table',
    schema='tuva_input',
    tags=['tuva_input', 'eligibility']
) }}

select
    person_id,
    gender,
    birth_date,
    race,
    zip_code,
    state,
    enrollment_start_date,
    enrollment_end_date,
    payer,
    plan,
    original_reason_entitlement_code,
    dual_status_code,
    medicare_status_code,
    data_source
from {{ ref('int_eligibility_deduped') }}
