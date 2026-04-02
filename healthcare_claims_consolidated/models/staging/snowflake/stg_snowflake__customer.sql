-- =============================================================================
-- Source: Snowflake / HEALTHCARE_RAW / customer
-- Migration: VARIANT address_json -> individual address columns (structural drift)
--            customer_segment -> segment (column name drift)
--            email_address -> email (column name drift)
--            phone -> phone_number (column name drift)
-- Date: 2026-04-02
-- =============================================================================

with source as (

    select * from {{ source('snowflake_raw', 'customer') }}

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
        -- Column naming drift: customer_segment -> segment
        upper(trim(customer_segment)) as segment,
        -- Column naming drift: email_address -> email
        email_address as email,
        -- Column naming drift: phone -> phone_number
        phone as phone_number,
        -- Structural drift: VARIANT JSON address -> individual columns
        address_json:address_line_1::varchar as address_line_1,
        address_json:address_line_2::varchar as address_line_2,
        address_json:city::varchar as city,
        address_json:postcode::varchar as postcode,
        address_json:country::varchar as country,
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
