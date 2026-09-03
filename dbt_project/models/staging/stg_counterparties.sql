-- Migrated from: teradata/ddl/03_staging_views.sql (V_COUNTERPARTY_SCREENED)
-- Teradata constructs replaced:
--   LOCK ROW FOR ACCESS       -> removed
--   Teradata BYTEINT flags    -> real booleans
--
-- Databricks notes: the sanctions/PEP flags arrive as strings ('true'/'false')
-- when the landing tables are loaded from CSV/Auto Loader without an explicit
-- schema, so they are cast to boolean before use. `cast(<flag> as boolean)`
-- is valid on Databricks, Snowflake and Postgres for both boolean and string
-- inputs. Boolean columns are used as predicates directly rather than
-- compared with `= true`, which Databricks rejects for non-boolean inputs.

with source as (

    select * from {{ source('barclays_raw', 'counterparty') }}

),

typed as (

    select
        counterparty_id,
        trim(counterparty_name) as counterparty_name,
        upper(trim(counterparty_type)) as counterparty_type,
        upper(trim(country_code)) as country_code,
        trim(lei) as lei,
        coalesce(cast(is_sanctions_listed as boolean), false) as is_sanctions_listed,
        coalesce(cast(is_pep as boolean), false) as is_pep

    from source

),

screened as (

    select
        counterparty_id,
        counterparty_name,
        counterparty_type,
        country_code,
        lei,
        is_sanctions_listed,
        is_pep,

        -- Screening category derivation
        case
            when is_sanctions_listed or is_pep
                then 'HIGH_RISK'
            when country_code in ('IR', 'KP', 'SY', 'CU', 'VE')
                then 'HIGH_RISK'
            when country_code not in ('GB', 'US', 'DE', 'FR', 'JP', 'CA', 'AU')
                then 'MEDIUM_RISK'
            else 'STANDARD'
        end as screening_category

    from typed

)

select * from screened
