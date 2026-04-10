/*
 * Staging Model: Snowflake Member Eligibility → Tuva Input Layer
 *
 * Source: snowflake/ddl/02_raw_tables.sql (CLAIMS_DW.RAW.RAW_MEMBER_ELIGIBILITY)
 * Target: Tuva input_layer.eligibility
 *
 * Key drift resolutions:
 *   - eligibility_start_date → enrollment_start_date (naming)
 *   - eligibility_end_date → enrollment_end_date (naming)
 *   - member_id → person_id (Tuva naming)
 *   - plan_code → plan (naming)
 *   - Dynamic Data Masking policies remain active (no code changes needed)
 */

with source as (
    select * from {{ source('snowflake_raw', 'raw_member_eligibility') }}
),

renamed as (
    select
        -- === Tuva eligibility required fields ===
        cast(member_id as varchar)                          as person_id,
        cast(gender as varchar)                             as gender,
        date_of_birth                                       as birth_date,
        cast(null as varchar)                               as race,
        cast(zip_code as varchar)                           as zip_code,
        cast(state_code as varchar)                         as state,
        eligibility_start_date                              as enrollment_start_date,
        eligibility_end_date                                as enrollment_end_date,
        cast(null as varchar)                               as payer,
        cast(plan_code as varchar)                          as plan,
        cast(null as varchar)                               as original_reason_entitlement_code,
        cast(null as varchar)                               as dual_status_code,
        cast(null as varchar)                               as medicare_status_code,

        -- === Additional fields ===
        cast(subscriber_id as varchar)                      as subscriber_id,
        cast(relationship_code as varchar)                  as member_relationship_code,
        cast(group_number as varchar)                       as group_id,
        cast(line_of_business as varchar)                   as line_of_business,

        -- === Source tracking ===
        'snowflake'                                         as data_source,
        cast('snowflake' as varchar)                        as source_platform,
        load_timestamp                                      as source_load_timestamp

    from source
)

select * from renamed
