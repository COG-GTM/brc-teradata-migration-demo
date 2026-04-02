-- =============================================================================
-- Source: Databricks / healthcare_raw / customer (Delta table)
-- Migration: PySpark DataFrame operations -> dbt SQL
--            snake_case column naming preserved (address_line1 -> address_line_1)
--            Delta merge semantics -> standard SQL dedup
-- Date: 2026-04-02
-- =============================================================================

with source as (

    select * from {{ source('databricks_raw', 'customer') }}

),

standardized as (

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
        email,
        phone_number,
        -- Column naming drift resolution: Databricks address_line1 -> address_line_1
        address_line1 as address_line_1,
        address_line2 as address_line_2,
        city,
        postcode,
        country,
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
    segment,
    email,
    phone_number,
    address_line_1,
    address_line_2,
    city,
    postcode,
    country

from standardized
where row_num = 1
