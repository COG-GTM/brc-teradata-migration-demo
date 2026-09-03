-- Migrated from: teradata/ddl/03_staging_views.sql (V_MARKET_DATA_LATEST)
-- Teradata constructs replaced:
--   QUALIFY ROW_NUMBER()  -> qualify_row_number() compat macro
--                            (native QUALIFY on Snowflake, sub-query on
--                             Databricks / Postgres)
--   LOCK ROW FOR ACCESS   -> removed
--   NULLIFZERO(mid_price) -> nullifzero() compat macro; dividing by NULL yields
--                            NULL on Databricks, Snowflake and Postgres alike.
--
-- decimal(18,8) is spelled identically on Databricks, Snowflake and Postgres
-- and keeps full FX/rate precision; Databricks would otherwise infer double
-- for CSV-loaded prices.

with source as (

    select * from {{ source('barclays_raw', 'market_data') }}

),

typed as (

    select
        instrument_id,
        cast(valuation_date as date) as valuation_date,
        upper(trim(instrument_type)) as instrument_type,
        instrument_name,
        upper(trim(currency)) as currency,
        cast(mid_price as decimal(18,8)) as mid_price,
        cast(bid_price as decimal(18,8)) as bid_price,
        cast(ask_price as decimal(18,8)) as ask_price

    from source

),

with_spread as (

    select
        instrument_id,
        valuation_date,
        instrument_type,
        instrument_name,
        currency,
        mid_price,
        bid_price,
        ask_price,

        -- Bid-ask spread; null when mid_price is null or zero
        (ask_price - bid_price) / {{ nullifzero('mid_price') }} as bid_ask_spread_pct

    from typed

)

select * from {{ qualify_row_number(
    source_relation='with_spread',
    partition_by='instrument_id',
    order_by='valuation_date desc',
    column_list='instrument_id,
        valuation_date,
        instrument_type,
        instrument_name,
        currency,
        mid_price,
        bid_price,
        ask_price,
        bid_ask_spread_pct'
) }} as latest_market_data
