/*
 * Staging Model: Teradata Member Eligibility → Tuva Input Layer
 *
 * Source: teradata/claims/ddl/02_raw_tables.sql (CLAIMS_RAW.RAW_MEMBER_ELIGIBILITY)
 * Target: Tuva input_layer.eligibility
 *
 * Column mapping resolves naming drift:
 *   - enrollment_start_date → enrollment_start_date (no change)
 *   - enrollment_end_date → enrollment_end_date (no change)
 *   - member_id → person_id (Tuva naming)
 *   - state → state (CHAR(2) → VARCHAR)
 *   - payer_id → payer
 *
 * Data type resolution:
 *   - CHAR fields cast to VARCHAR for Snowflake compatibility
 *   - DATE fields remain DATE
 */

with source as (
    select * from {{ source('teradata_raw', 'raw_member_eligibility') }}
),

renamed as (
    select
        -- === Tuva eligibility required fields ===
        cast(member_id as varchar)                          as person_id,
        cast(gender as varchar)                             as gender,
        date_of_birth                                       as birth_date,
        cast(null as varchar)                               as race,
        cast(zip_code as varchar)                           as zip_code,
        cast(state as varchar)                              as state,
        enrollment_start_date                               as enrollment_start_date,
        enrollment_end_date                                 as enrollment_end_date,
        cast(payer_id as varchar)                           as payer,
        cast(plan_id as varchar)                            as plan,
        cast(null as varchar)                               as original_reason_entitlement_code,
        cast(null as varchar)                               as dual_status_code,
        cast(null as varchar)                               as medicare_status_code,

        -- === Additional Tuva eligibility fields ===
        cast(subscriber_id as varchar)                      as subscriber_id,
        cast(relation_to_subscriber as varchar)             as member_relationship_code,
        cast(group_id as varchar)                           as group_id,
        cast(line_of_business as varchar)                   as line_of_business,

        -- === Source tracking ===
        'teradata'                                          as data_source,
        cast('teradata' as varchar)                         as source_platform,
        load_ts                                             as source_load_timestamp

    from source
)

select * from renamed
