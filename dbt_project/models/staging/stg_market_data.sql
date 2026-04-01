-- Migrated from: teradata/ddl/03_staging_views.sql (V_MARKET_DATA_LATEST)
-- Teradata constructs replaced:
--   QUALIFY ROW_NUMBER() -> sub-query with window function (used for dedup only)
--   LOCK ROW FOR ACCESS  -> removed
-- Note: Full time-series is preserved (no latest-only filter) to support
-- downstream regulatory capital trending (CSUM/MAVG/MDIFF patterns).

with source as (

    select * from {{ source('barclays_raw', 'market_data') }}

),

-- Deduplicate: keep one row per instrument_id + valuation_date
deduplicated as (

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
            partition by instrument_id, valuation_date
            order by instrument_id
        ) as _rn

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

from deduplicated
where _rn = 1
