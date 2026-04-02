-- =============================================================================
-- Unified Market Data: Consolidates market data from all three platforms.
-- Priority order: Teradata (master) > Snowflake > Databricks
-- =============================================================================

with teradata_md as (

    select *, 'TERADATA' as source_platform, 1 as platform_priority
    from {{ ref('stg_teradata__market_data') }}

),

databricks_md as (

    select *, 'DATABRICKS' as source_platform, 3 as platform_priority
    from {{ ref('stg_databricks__market_data') }}

),

snowflake_md as (

    select *, 'SNOWFLAKE' as source_platform, 2 as platform_priority
    from {{ ref('stg_snowflake__market_data') }}

),

all_md as (

    select * from teradata_md
    union all
    select * from databricks_md
    union all
    select * from snowflake_md

),

deduplicated as (

    select
        *,
        row_number() over (
            partition by instrument_id, valuation_date
            order by platform_priority asc
        ) as cross_platform_rank

    from all_md

)

select
    instrument_id,
    valuation_date,
    instrument_type,
    instrument_name,
    currency,
    mid_price,
    bid_price,
    ask_price,
    bid_ask_spread,
    source_system,
    source_platform

from deduplicated
where cross_platform_rank = 1
