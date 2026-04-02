-- =============================================================================
-- Source: Databricks / healthcare_raw / market_data (Delta table)
-- Migration: PySpark MERGE-on-read -> dbt SQL dedup
--            currency_code -> currency (column name drift)
--            data_source -> source_system (column name drift)
-- Date: 2026-04-02
-- =============================================================================

with source as (

    select * from {{ source('databricks_raw', 'market_data') }}

),

deduplicated as (

    select
        instrument_id,
        valuation_date,
        upper(trim(instrument_type)) as instrument_type,
        instrument_name,
        -- Column naming drift: currency_code -> currency
        upper(trim(currency_code)) as currency,
        coalesce(mid_price, 0) as mid_price,
        coalesce(bid_price, 0) as bid_price,
        coalesce(ask_price, 0) as ask_price,
        nullif(coalesce(ask_price, 0) - coalesce(bid_price, 0), 0) as bid_ask_spread,
        -- Column naming drift: data_source -> source_system
        data_source as source_system,
        row_number() over (
            partition by instrument_id, valuation_date
            order by valuation_date desc
        ) as row_num

    from source

)

select
    instrument_id,
    valuation_date,
    instrument_type,
    instrument_name,
    currency,
    mid_price,
    bid_price,
    ask_price,
    bid_ask_spread,
    source_system

from deduplicated
where row_num = 1
