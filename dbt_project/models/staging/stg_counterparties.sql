-- Migrated from: teradata/ddl/03_staging_views.sql (V_COUNTERPARTY_SCREENED)
-- Teradata constructs replaced:
--   LOCK ROW FOR ACCESS -> removed
--   Teradata boolean handling -> standard SQL booleans

with source as (

    select * from {{ source('barclays_raw', 'counterparty') }}

),

-- Deduplication for Teradata SET table (row-level uniqueness)
-- Uses ROW_NUMBER() subquery pattern for cross-database compatibility
deduplicated as (

    select
        *,
        row_number() over (
            partition by counterparty_id
            order by counterparty_id
        ) as _rn
    from source

),

screened as (

    select
        counterparty_id,
        trim(counterparty_name) as counterparty_name,
        upper(trim(counterparty_type)) as counterparty_type,
        upper(trim(country_code)) as country_code,
        trim(lei) as lei,
        coalesce(is_sanctions_listed, false) as is_sanctions_listed,
        coalesce(is_pep, false) as is_pep,

        -- Screening category derivation
        case
            when coalesce(is_sanctions_listed, false) = true
              or coalesce(is_pep, false) = true
                then 'HIGH_RISK'
            when upper(trim(country_code)) in ('IR', 'KP', 'SY', 'CU', 'VE')
                then 'HIGH_RISK'
            when upper(trim(country_code)) not in ('GB', 'US', 'DE', 'FR', 'JP', 'CA', 'AU')
                then 'MEDIUM_RISK'
            else 'STANDARD'
        end as screening_category

    from deduplicated
    where _rn = 1

)

select * from screened
