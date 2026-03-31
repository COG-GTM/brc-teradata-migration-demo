-- Migrated from: teradata/ddl/03_staging_views.sql (V_MARKET_DATA_LATEST)
-- Teradata constructs replaced:
--   QUALIFY ROW_NUMBER() -> sub-query with window function
--   LOCK ROW FOR ACCESS  -> removed

with source as (

    select * from {{ source('barclays_raw', 'market_data') }}

),

with_spread as (

    select
        instrument_id,
        valuation_date,
        upper(trim(instrument_type)) as instrument_type,
        instrument_name,
        upper(trim(currency)) as currency,
        mid_price,
        bid_price,
        ask_price,

        -- Bid-ask spread
        case
            when mid_price != 0 and mid_price is not null
                then (ask_price - bid_price) / mid_price
            else null
        end as bid_ask_spread_pct,

        row_number() over (
            partition by instrument_id
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
    bid_ask_spread_pct

from with_spread
where row_num = 1
