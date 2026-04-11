-- =============================================================================
-- Unified Counterparty: Consolidates counterparty records from all three platforms.
-- Priority order: Teradata (master) > Snowflake > Databricks
-- Migrated from: All three platform staging models
-- =============================================================================

with teradata_cp as (

    select *, 'TERADATA' as source_platform, 1 as platform_priority
    from {{ ref('stg_teradata__counterparty') }}

),

databricks_cp as (

    select *, 'DATABRICKS' as source_platform, 3 as platform_priority
    from {{ ref('stg_databricks__counterparty') }}

),

snowflake_cp as (

    select *, 'SNOWFLAKE' as source_platform, 2 as platform_priority
    from {{ ref('stg_snowflake__counterparty') }}

),

all_cp as (

    select * from teradata_cp
    union all
    select * from databricks_cp
    union all
    select * from snowflake_cp

),

deduplicated as (

    select
        *,
        row_number() over (
            partition by counterparty_id
            order by platform_priority asc
        ) as cross_platform_rank

    from all_cp

)

select
    counterparty_id,
    counterparty_name,
    counterparty_type,
    country_code,
    lei_code,
    swift_bic,
    risk_rating,
    is_sanctions_listed,
    is_pep,
    screening_category,
    source_platform

from deduplicated
where cross_platform_rank = 1
