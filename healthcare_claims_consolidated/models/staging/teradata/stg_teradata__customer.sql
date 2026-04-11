-- =============================================================================
-- Source: Teradata / BARCLAYS_RAW / CUSTOMER (SET table)
-- Migration: QUALIFY ROW_NUMBER() -> subquery with ROW_NUMBER()
--            LOCK ROW FOR ACCESS  -> removed (not needed in Snowflake)
--            NOT CASESPECIFIC     -> removed (Snowflake default)
--            CHARACTER SET LATIN  -> removed (Snowflake uses UTF-8)
-- Date: 2026-04-02
-- =============================================================================

with source as (

    select * from {{ source('teradata_raw', 'customer') }}

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
        email,
        phone_number,
        address_line_1,
        address_line_2,
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

from deduplicated
where row_num = 1
