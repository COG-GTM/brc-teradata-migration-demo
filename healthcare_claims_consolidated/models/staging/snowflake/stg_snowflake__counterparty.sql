-- =============================================================================
-- Source: Snowflake / HEALTHCARE_RAW / counterparty
-- Migration: name -> counterparty_name (column name drift)
--            entity_type -> counterparty_type (column name drift)
--            screening_flags VARIANT JSON -> individual boolean columns (structural drift)
-- Date: 2026-04-02
-- =============================================================================

with source as (

    select * from {{ source('snowflake_raw', 'counterparty') }}

),

deduplicated as (

    select
        counterparty_id,
        -- Column naming drift: name -> counterparty_name
        name as counterparty_name,
        -- Column naming drift: entity_type -> counterparty_type
        upper(trim(entity_type)) as counterparty_type,
        upper(trim(country_code)) as country_code,
        lei_code,
        swift_bic,
        upper(trim(risk_rating)) as risk_rating,
        -- Structural drift: VARIANT screening_flags JSON -> individual boolean columns
        coalesce(screening_flags:is_sanctioned::boolean, false) as is_sanctions_listed,
        coalesce(screening_flags:is_pep::boolean, false) as is_pep,
        -- Screening category (consistent with Teradata/Databricks staging)
        case
            when coalesce(screening_flags:is_sanctioned::boolean, false) = true
              or coalesce(screening_flags:is_pep::boolean, false) = true then 'HIGH'
            when upper(trim(risk_rating)) = 'H' then 'ELEVATED'
            else 'STANDARD'
        end as screening_category,
        row_number() over (
            partition by counterparty_id
            order by counterparty_id
        ) as row_num

    from source

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
    screening_category

from deduplicated
where row_num = 1
