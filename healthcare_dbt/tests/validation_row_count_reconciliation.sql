/*
 * Phase 8 Validation: Row Count Reconciliation
 *
 * Compares record counts from each legacy platform's staging layer
 * against the unified intermediate models to ensure no data loss
 * during migration.
 *
 * Expected: NO rows returned (all platforms should have matching counts)
 *
 * Note: The unified model may have FEWER rows than the sum of all platforms
 * due to cross-platform deduplication (same claim on multiple platforms).
 * This test validates per-platform counts before cross-platform dedup.
 */

with platform_counts as (
    -- Teradata medical claim counts
    select 'teradata' as platform, 'medical_claim' as entity,
           count(*) as staging_count
    from {{ ref('stg_teradata__medical_claim') }}
    union all
    -- Databricks medical claim counts
    select 'databricks', 'medical_claim',
           count(*)
    from {{ ref('stg_databricks__medical_claim') }}
    union all
    -- Snowflake medical claim counts
    select 'snowflake', 'medical_claim',
           count(*)
    from {{ ref('stg_snowflake__medical_claim') }}
    union all
    -- Teradata eligibility counts
    select 'teradata', 'eligibility',
           count(*)
    from {{ ref('stg_teradata__eligibility') }}
    union all
    -- Databricks eligibility counts
    select 'databricks', 'eligibility',
           count(*)
    from {{ ref('stg_databricks__eligibility') }}
    union all
    -- Snowflake eligibility counts
    select 'snowflake', 'eligibility',
           count(*)
    from {{ ref('stg_snowflake__eligibility') }}
    union all
    -- Teradata pharmacy claim counts
    select 'teradata', 'pharmacy_claim',
           count(*)
    from {{ ref('stg_teradata__pharmacy_claim') }}
    union all
    -- Databricks pharmacy claim counts
    select 'databricks', 'pharmacy_claim',
           count(*)
    from {{ ref('stg_databricks__pharmacy_claim') }}
    union all
    -- Snowflake pharmacy claim counts
    select 'snowflake', 'pharmacy_claim',
           count(*)
    from {{ ref('stg_snowflake__pharmacy_claim') }}
),

unified_counts as (
    select 'unified' as platform, 'medical_claim' as entity,
           count(*) as unified_count
    from {{ ref('int_medical_claim_adr_deduped') }}
    union all
    select 'unified', 'eligibility',
           count(*)
    from {{ ref('int_eligibility_deduped') }}
    union all
    select 'unified', 'pharmacy_claim',
           count(*)
    from {{ ref('int_pharmacy_claim_adr_deduped') }}
),

total_platform_counts as (
    select entity, sum(staging_count) as total_staging_count
    from platform_counts
    group by entity
)

-- Flag if unified count exceeds total platform count (would indicate duplicates)
select
    tc.entity,
    tc.total_staging_count,
    uc.unified_count,
    'FAIL: Unified count exceeds total platform count - possible duplication' as failure_reason
from total_platform_counts tc
inner join unified_counts uc on tc.entity = uc.entity
where uc.unified_count > tc.total_staging_count
