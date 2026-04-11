-- =============================================================================
-- Source: Databricks / healthcare_raw / counterparty (Delta table)
-- Migration: PySpark DataFrame operations -> dbt SQL
--            is_sanctioned boolean -> already boolean (no conversion needed)
--            is_pep boolean -> already boolean
--            JSON nested struct for flags -> flattened to columns
-- Date: 2026-04-02
-- =============================================================================

with source as (

    select * from {{ source('databricks_raw', 'counterparty') }}

),

deduplicated as (

    select
        counterparty_id,
        counterparty_name,
        upper(trim(counterparty_type)) as counterparty_type,
        upper(trim(country_code)) as country_code,
        lei_code,
        swift_bic,
        upper(trim(risk_rating)) as risk_rating,
        -- Databricks already has boolean flags (no Y/N conversion needed)
        coalesce(is_sanctioned, false) as is_sanctions_listed,
        coalesce(is_pep, false) as is_pep,
        -- Screening category (consistent with Teradata staging)
        case
            when coalesce(is_sanctioned, false) = true or coalesce(is_pep, false) = true then 'HIGH'
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
