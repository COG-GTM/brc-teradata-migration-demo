-- =============================================================================
-- Unified Transaction: Consolidates transaction records from all three platforms.
-- Priority order: Teradata (master) > Snowflake > Databricks
-- Migrated from: All three platform staging models
-- =============================================================================

with teradata_txns as (

    select *, 'TERADATA' as source_platform, 1 as platform_priority
    from {{ ref('stg_teradata__transaction') }}

),

databricks_txns as (

    select *, 'DATABRICKS' as source_platform, 3 as platform_priority
    from {{ ref('stg_databricks__transaction') }}

),

snowflake_txns as (

    select *, 'SNOWFLAKE' as source_platform, 2 as platform_priority
    from {{ ref('stg_snowflake__transaction') }}

),

all_txns as (

    select * from teradata_txns
    union all
    select * from databricks_txns
    union all
    select * from snowflake_txns

),

deduplicated as (

    select
        *,
        row_number() over (
            partition by transaction_id
            order by platform_priority asc
        ) as cross_platform_rank

    from all_txns

)

select
    transaction_id,
    account_id,
    transaction_date,
    transaction_time,
    amount,
    currency,
    transaction_type,
    counterparty_id,
    description,
    channel,
    reference_number,
    balance_after,
    value_band,
    signed_amount,
    source_platform

from deduplicated
where cross_platform_rank = 1
