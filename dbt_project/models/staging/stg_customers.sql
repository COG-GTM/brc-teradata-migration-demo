-- Migrated from: teradata/ddl/03_staging_views.sql (V_CUSTOMER_LATEST)
-- Teradata constructs replaced:
--   QUALIFY ROW_NUMBER() -> sub-query with window function
--   LOCK ROW FOR ACCESS  -> removed (not needed in Snowflake/Databricks)
--   NOT CASESPECIFIC     -> removed (Snowflake is case-insensitive by default)

with source as (

    select * from {{ source('barclays_raw', 'customer') }}

),

deduplicated as (

    select
        customer_id,
        first_name,
        last_name,
        date_of_birth,
        nationality,
        upper(trim(kyc_status)) as kyc_status,
        upper(trim(risk_rating)) as risk_rating,
        onboarding_date,
        upper(trim(segment)) as segment,
        row_number() over (
            partition by customer_id
            order by onboarding_date desc
        ) as row_num

    from source

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
    segment

from deduplicated
where row_num = 1
