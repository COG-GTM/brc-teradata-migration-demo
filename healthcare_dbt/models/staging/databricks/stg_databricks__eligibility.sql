/*
 * Staging Model: Databricks Member Eligibility → Tuva Input Layer
 *
 * Source: databricks/ddl/02_raw_tables.sql (claims_raw.raw_member_eligibility)
 * Target: Tuva input_layer.eligibility
 *
 * Key drift resolutions:
 *   - coverage_start_date → enrollment_start_date (naming)
 *   - coverage_end_date → enrollment_end_date (naming)
 *   - member_id → person_id (Tuva naming)
 *   - STRING → VARCHAR (type normalization for Snowflake)
 */

with source as (
    select * from {{ source('databricks_raw', 'raw_member_eligibility') }}
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
        coverage_start_date                                 as enrollment_start_date,
        coverage_end_date                                   as enrollment_end_date,
        cast(null as varchar)                               as payer,
        cast(plan_id as varchar)                            as plan,
        cast(null as varchar)                               as original_reason_entitlement_code,
        cast(null as varchar)                               as dual_status_code,
        cast(medicare_beneficiary_id as varchar)            as medicare_status_code,

        -- === Additional fields ===
        cast(subscriber_id as varchar)                      as subscriber_id,
        cast(null as varchar)                               as member_relationship_code,
        cast(null as varchar)                               as group_id,
        cast(line_of_business as varchar)                   as line_of_business,

        -- === Source tracking ===
        'databricks'                                        as data_source,
        cast('databricks' as varchar)                       as source_platform,
        load_timestamp                                      as source_load_timestamp

    from source
)

select * from renamed
