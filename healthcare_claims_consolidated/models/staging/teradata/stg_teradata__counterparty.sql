-- =============================================================================
-- Source: Teradata / BARCLAYS_RAW / COUNTERPARTY (SET table)
-- Migration: QUALIFY ROW_NUMBER() -> subquery with ROW_NUMBER()
--            NOT CASESPECIFIC     -> removed
--            LOCK ROW FOR ACCESS  -> removed
--            sanctions_flag Y/N   -> boolean conversion
--            pep_flag Y/N         -> boolean conversion
-- Date: 2026-04-02
-- =============================================================================

with source as (

    select * from {{ source('teradata_raw', 'counterparty') }}

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
        -- Convert Y/N flags to booleans (Teradata used CHAR(1) COMPRESS)
        case when upper(trim(sanctions_flag)) = 'Y' then true else false end as is_sanctions_listed,
        case when upper(trim(pep_flag)) = 'Y' then true else false end as is_pep,
        -- Screening category (from Teradata V_COUNTERPARTY_SCREENED)
        case
            when upper(trim(sanctions_flag)) = 'Y' or upper(trim(pep_flag)) = 'Y' then 'HIGH'
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
