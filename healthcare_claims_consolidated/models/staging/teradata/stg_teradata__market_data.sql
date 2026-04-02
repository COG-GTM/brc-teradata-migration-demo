-- =============================================================================
-- Source: Teradata / BARCLAYS_RAW / MARKET_DATA (MULTISET table)
-- Migration: QUALIFY ROW_NUMBER() -> subquery with ROW_NUMBER()
--            ZEROIFNULL(x)        -> coalesce(x, 0)
--            NULLIFZERO(x)        -> nullif(x, 0)
--            LOCK ROW FOR ACCESS  -> removed
-- Date: 2026-04-02
-- =============================================================================

with source as (

    select * from {{ source('teradata_raw', 'market_data') }}

),

deduplicated as (

    select
        instrument_id,
        valuation_date,
        upper(trim(instrument_type)) as instrument_type,
        instrument_name,
        upper(trim(currency)) as currency,
        coalesce(mid_price, 0) as mid_price,
        coalesce(bid_price, 0) as bid_price,
        coalesce(ask_price, 0) as ask_price,
        nullif(coalesce(ask_price, 0) - coalesce(bid_price, 0), 0) as bid_ask_spread,
        source_system,
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
