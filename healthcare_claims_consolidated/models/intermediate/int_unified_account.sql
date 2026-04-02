-- =============================================================================
-- Unified Account: Consolidates account records from all three platforms.
-- Priority order: Teradata (master) > Snowflake > Databricks
-- Migrated from: All three platform staging models
-- =============================================================================

with teradata_accounts as (

    select *, 'TERADATA' as source_platform, 1 as platform_priority
    from {{ ref('stg_teradata__account') }}

),

databricks_accounts as (

    select *, 'DATABRICKS' as source_platform, 3 as platform_priority
    from {{ ref('stg_databricks__account') }}

),

snowflake_accounts as (

    select *, 'SNOWFLAKE' as source_platform, 2 as platform_priority
    from {{ ref('stg_snowflake__account') }}

),

all_accounts as (

    select * from teradata_accounts
    union all
    select * from databricks_accounts
    union all
    select * from snowflake_accounts

),

deduplicated as (

    select
        *,
        row_number() over (
            partition by account_id
            order by platform_priority asc
        ) as cross_platform_rank

    from all_accounts

)

select
    account_id,
    customer_id,
    account_type,
    currency,
    branch_code,
    sort_code,
    status,
    open_date,
    close_date,
    credit_limit,
    overdraft_limit,
    derived_status,
    days_since_opening,
    source_platform

from deduplicated
where cross_platform_rank = 1
