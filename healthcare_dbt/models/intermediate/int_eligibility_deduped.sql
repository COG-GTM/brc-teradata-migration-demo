/*
 * Intermediate Model: Deduplicated Member Eligibility (Unified)
 *
 * Unions eligibility from all platforms and deduplicates to produce
 * a single record per member. Resolves DRIFT-005 (member dedup sort order).
 *
 * Sort order: enrollment_start_date DESC, source_load_timestamp DESC
 * (adopted from Teradata/Databricks - business date first)
 *
 * Migration source:
 *   - teradata/claims/ddl/03_staging_views.sql (V_MEMBER_LATEST)
 *   - databricks/ddl/03_staging_tables.sql (stg_member_latest)
 *   - snowflake/ddl/03_staging_views.sql (V_MEMBER_LATEST)
 */

{{ config(
    materialized='table',
    tags=['intermediate', 'eligibility']
) }}

with all_platform_eligibility as (
    select *, 1 as platform_priority from {{ ref('stg_snowflake__eligibility') }}
    union all
    select *, 2 as platform_priority from {{ ref('stg_databricks__eligibility') }}
    union all
    select *, 3 as platform_priority from {{ ref('stg_teradata__eligibility') }}
),

deduped as (
    select
        *,
        row_number() over (
            partition by person_id
            order by
                enrollment_start_date desc,
                source_load_timestamp desc,
                platform_priority asc
        ) as dedup_rn
    from all_platform_eligibility
)

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
    subscriber_id,
    member_relationship_code,
    group_id,
    line_of_business,
    data_source,
    source_platform,
    source_load_timestamp
from deduped
where dedup_rn = 1
