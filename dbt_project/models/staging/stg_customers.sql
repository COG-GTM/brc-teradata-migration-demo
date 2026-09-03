-- Migrated from: teradata/ddl/03_staging_views.sql (V_CUSTOMER_LATEST)
-- Teradata constructs replaced:
--   QUALIFY ROW_NUMBER() -> qualify_row_number() compat macro
--                           (native QUALIFY on Snowflake, sub-query on
--                            Databricks / Postgres)
--   LOCK ROW FOR ACCESS  -> removed (not needed in Databricks/Snowflake)
--   NOT CASESPECIFIC     -> removed; comparisons are normalised with upper()
--                           because Databricks string comparison IS case
--                           sensitive (unlike Teradata NOT CASESPECIFIC)

with source as (

    select * from {{ source('barclays_raw', 'customer') }}

),

cleaned as (

    select
        customer_id,
        first_name,
        last_name,
        cast(date_of_birth as date) as date_of_birth,
        nationality,
        upper(trim(kyc_status)) as kyc_status,
        upper(trim(risk_rating)) as risk_rating,
        cast(onboarding_date as date) as onboarding_date,
        upper(trim(segment)) as segment

    from source

)

select * from {{ qualify_row_number(
    source_relation='cleaned',
    partition_by='customer_id',
    order_by='onboarding_date desc, customer_id',
    column_list='customer_id,
        first_name,
        last_name,
        date_of_birth,
        nationality,
        kyc_status,
        risk_rating,
        onboarding_date,
        segment'
) }} as latest_customer
