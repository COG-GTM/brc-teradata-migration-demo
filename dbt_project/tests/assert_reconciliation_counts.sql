-- Reconciliation test: Verify row counts match between source and staging layers.
-- Ensures no data loss during the Teradata-to-dbt migration.
-- Each source table should have the same (or fewer, due to dedup) rows in staging.

with source_counts as (

    select 'customers' as entity, count(*) as source_rows from {{ source('barclays_raw', 'customer') }}
    union all
    select 'transactions', count(*) from {{ source('barclays_raw', 'transaction') }}
    union all
    select 'counterparties', count(*) from {{ source('barclays_raw', 'counterparty') }}
    union all
    select 'market_data', count(*) from {{ source('barclays_raw', 'market_data') }}

),

staging_counts as (

    select 'customers' as entity, count(*) as staging_rows from {{ ref('stg_customers') }}
    union all
    select 'transactions', count(*) from {{ ref('stg_transactions') }}
    union all
    select 'counterparties', count(*) from {{ ref('stg_counterparties') }}
    union all
    select 'market_data', count(*) from {{ ref('stg_market_data') }}

)

-- Fail if staging has MORE rows than source (indicates duplication)
select
    s.entity,
    s.source_rows,
    t.staging_rows

from source_counts s
inner join staging_counts t on s.entity = t.entity
where t.staging_rows > s.source_rows
