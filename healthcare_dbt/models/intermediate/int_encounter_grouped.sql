/*
 * Intermediate Model: Encounter Grouping (Gap-and-Island)
 *
 * Groups overlapping or contiguous medical claims into encounters
 * using the correct gap-and-island overlap detection algorithm.
 * This resolves DRIFT-002.
 *
 * Algorithm: Adopted from Databricks/Snowflake (correct implementation).
 * Teradata's naive GROUP BY approach is deprecated.
 *
 * Migration source:
 *   - teradata/claims/stored_procedures/sp_encounter_grouping.sql (NAIVE - deprecated)
 *   - databricks/notebooks/03_encounter_grouping.py (CORRECT - adopted)
 *   - snowflake/stored_procedures/sp_encounter_grouping.sql (CORRECT - adopted)
 */

{{ config(
    materialized='table',
    tags=['intermediate', 'encounter_grouping']
) }}

with paid_claims as (
    -- Only group PAID and ADJUSTED claims into encounters
    select
        claim_id,
        claim_line_number,
        person_id,
        claim_start_date,
        coalesce(claim_end_date, claim_start_date)          as claim_end_date,
        claim_type,
        place_of_service_code,
        ms_drg_code,
        rendering_npi,
        facility_npi,
        diagnosis_code_1,
        charge_amount,
        allowed_amount,
        paid_amount,
        copayment_amount,
        coinsurance_amount,
        deductible_amount,
        data_source
    from {{ ref('int_medical_claim_adr_deduped') }}
    where upper(claim_status) in ('PAID', 'ADJUSTED')
),

with_prev_end as (
    -- Step 1: Track running max end_date for each member
    select
        *,
        {{ gap_and_island_encounter_groups('person_id', 'claim_start_date', 'claim_end_date') }}
    from paid_claims
),

island_starts as (
    -- Step 2: Detect new encounter islands
    select
        *,
        {{ is_new_encounter_island('claim_start_date', 'prev_max_end_date') }}
                                                            as is_new_island
    from with_prev_end
),

island_groups as (
    -- Step 3: Assign encounter group IDs
    select
        *,
        {{ encounter_group_id('person_id', 'claim_start_date', 'claim_end_date', 'is_new_island') }}
                                                            as encounter_group_id
    from island_starts
),

encounters as (
    -- Step 4: Aggregate claims into encounters
    select
        person_id || '-' ||
            to_char(min(claim_start_date), 'YYYYMMDD') || '-' ||
            lpad(encounter_group_id::varchar, 4, '0')       as encounter_id,
        person_id,
        min(claim_start_date)                               as encounter_start_date,
        max(claim_end_date)                                 as encounter_end_date,

        -- Encounter type classification
        case
            when max(case when claim_type = 'institutional'
                          and place_of_service_code = '21' then 1 else 0 end) = 1
                then 'inpatient'
            when max(case when place_of_service_code = '23' then 1 else 0 end) = 1
                then 'emergency'
            when max(case when claim_type = 'institutional' then 1 else 0 end) = 1
                then 'outpatient'
            when max(case when place_of_service_code = '02' then 1 else 0 end) = 1
                then 'telehealth'
            else 'office_visit'
        end                                                 as encounter_type,

        -- DRG from institutional claims
        max(ms_drg_code)                                    as ms_drg_code,

        -- Primary diagnosis from first claim
        min(diagnosis_code_1)                               as primary_diagnosis_code,

        -- Measures
        count(distinct claim_id)                            as total_claim_lines,
        sum(coalesce(charge_amount, 0))                     as total_charge_amount,
        sum(coalesce(allowed_amount, 0))                    as total_allowed_amount,
        sum(coalesce(paid_amount, 0))                       as total_paid_amount,
        sum(coalesce(copayment_amount, 0))
            + sum(coalesce(coinsurance_amount, 0))
            + sum(coalesce(deductible_amount, 0))           as total_member_liability,
        datediff('day',
            min(claim_start_date),
            max(claim_end_date))                            as length_of_stay,

        encounter_group_id,
        max(data_source)                                    as data_source

    from island_groups
    group by person_id, encounter_group_id
)

select * from encounters
