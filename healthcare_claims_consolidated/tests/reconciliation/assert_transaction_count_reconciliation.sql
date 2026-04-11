-- =============================================================================
-- Row Count Reconciliation: Transactions
-- Validates that the unified transaction model contains the expected number
-- of records from each source platform. In production, compare against
-- actual platform counts stored in a reconciliation control table.
-- =============================================================================

with unified_counts as (

    select
        source_platform,
        count(*) as unified_count
    from {{ ref('int_unified_transaction') }}
    group by source_platform

),

-- In production, these would come from a control table loaded from each platform
expected_counts as (

    select 'TERADATA' as source_platform, count(*) as expected_count
    from {{ ref('stg_teradata__transaction') }}
    union all
    select 'DATABRICKS', count(*)
    from {{ ref('stg_databricks__transaction') }}
    union all
    select 'SNOWFLAKE', count(*)
    from {{ ref('stg_snowflake__transaction') }}

)

-- Test passes if no rows returned (all counts match)
select
    e.source_platform,
    e.expected_count,
    coalesce(u.unified_count, 0) as unified_count,
    e.expected_count - coalesce(u.unified_count, 0) as difference
from expected_counts e
left join unified_counts u
    on e.source_platform = u.source_platform
where e.expected_count != coalesce(u.unified_count, 0)
