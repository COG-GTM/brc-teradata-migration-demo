-- =============================================================================
-- Row Count Reconciliation: Customers
-- Validates that the unified customer model contains consistent records.
-- The unified model deduplicates across platforms, so the unified count
-- should be <= the sum of platform counts.
-- =============================================================================

with platform_counts as (

    select 'TERADATA' as platform, count(*) as cnt
    from {{ ref('stg_teradata__customer') }}
    union all
    select 'DATABRICKS', count(*)
    from {{ ref('stg_databricks__customer') }}
    union all
    select 'SNOWFLAKE', count(*)
    from {{ ref('stg_snowflake__customer') }}

),

unified_count as (

    select count(*) as cnt
    from {{ ref('int_unified_customer') }}

),

total_platform_count as (

    select sum(cnt) as total_cnt
    from platform_counts

)

-- Test passes if unified count <= total platform count (dedup removes duplicates)
-- and unified count >= max single platform count (no data loss)
select
    'CUSTOMER_COUNT_CHECK' as check_name,
    t.total_cnt as total_across_platforms,
    u.cnt as unified_count,
    (select max(cnt) from platform_counts) as max_single_platform
from total_platform_count t
cross join unified_count u
where u.cnt > t.total_cnt  -- unified should never exceed total
   or u.cnt < (select max(cnt) from platform_counts)  -- should have at least as many as largest platform
