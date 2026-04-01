-- Custom test: Reconciliation check ensuring staging models do not gain
-- spurious rows compared to their raw sources. Staging row count must be
-- less than or equal to source (staging may legitimately filter or deduplicate).
-- Any staging count EXCEEDING the source indicates a fan-out bug.

with source_counts as (

    select 'customer' as entity,
           count(*) as source_rows
    from {{ source('barclays_raw', 'customer') }}

    union all

    select 'account' as entity,
           count(*) as source_rows
    from {{ source('barclays_raw', 'account') }}

    union all

    select 'transaction' as entity,
           count(*) as source_rows
    from {{ source('barclays_raw', 'transaction') }}

    union all

    select 'counterparty' as entity,
           count(*) as source_rows
    from {{ source('barclays_raw', 'counterparty') }}

),

staging_counts as (

    select 'customer' as entity,
           count(*) as staging_rows
    from {{ ref('stg_customers') }}

    union all

    select 'account' as entity,
           count(*) as staging_rows
    from {{ ref('stg_accounts') }}

    union all

    select 'transaction' as entity,
           count(*) as staging_rows
    from {{ ref('stg_transactions') }}

    union all

    select 'counterparty' as entity,
           count(*) as staging_rows
    from {{ ref('stg_counterparties') }}

)

-- Fail if any staging model has MORE rows than its source
-- (fewer rows is expected due to deduplication and filtering)
select
    s.entity,
    s.source_rows,
    t.staging_rows

from source_counts s
inner join staging_counts t
    on s.entity = t.entity
where t.staging_rows > s.source_rows
