-- =============================================================================
-- Unified Customer: Consolidates customer records from all three platforms.
-- Priority order for conflict resolution: Teradata (master) > Snowflake > Databricks
-- Migrated from: All three platform staging models
-- =============================================================================

with teradata_customers as (

    select *, 'TERADATA' as source_platform, 1 as platform_priority
    from {{ ref('stg_teradata__customer') }}

),

databricks_customers as (

    select *, 'DATABRICKS' as source_platform, 3 as platform_priority
    from {{ ref('stg_databricks__customer') }}

),

snowflake_customers as (

    select *, 'SNOWFLAKE' as source_platform, 2 as platform_priority
    from {{ ref('stg_snowflake__customer') }}

),

all_customers as (

    select * from teradata_customers
    union all
    select * from databricks_customers
    union all
    select * from snowflake_customers

),

-- Deduplicate across platforms: prefer Teradata (golden master), then Snowflake, then Databricks
deduplicated as (

    select
        *,
        row_number() over (
            partition by customer_id
            order by platform_priority asc
        ) as cross_platform_rank

    from all_customers

)

select
    customer_id,
    first_name,
    last_name,
    date_of_birth,
    nationality,
    kyc_status,
    risk_rating,
    onboarding_date,
    segment,
    email,
    phone_number,
    address_line_1,
    address_line_2,
    city,
    postcode,
    country,
    source_platform

from deduplicated
where cross_platform_rank = 1
