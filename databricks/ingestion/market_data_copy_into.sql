-- =============================================================================
-- market_data_copy_into.sql
--
-- Replaces: teradata/fastload/market_data_load.fl (SQL-warehouse variant)
--
-- Mapping rationale:
--   Same legacy job as market_data_autoloader.py, expressed as COPY INTO for
--   teams that prefer a Databricks SQL warehouse over a Python task. COPY INTO
--   is idempotent per file, so re-running after a failure skips already-loaded
--   files — the direct analogue of FastLoad's CHECKPOINT/restart behaviour,
--   without needing to drop and recreate ET/UV tables first.
--
--   Use this variant for backfills of a bounded set of files; use Auto Loader
--   when the landing directory accumulates files continuously (COPY INTO
--   re-lists the directory on every run and degrades on very large listings).
-- =============================================================================

-- Parameters: catalog, schema_raw, landing_path, delimiter
COPY INTO ${catalog}.${schema_raw}.market_data
FROM (
    SELECT
        CAST(instrument_id AS STRING)                       AS instrument_id,
        TO_DATE(valuation_date, 'yyyy-MM-dd')               AS valuation_date,
        CAST(instrument_type AS STRING)                     AS instrument_type,
        CAST(instrument_name AS STRING)                     AS instrument_name,
        CAST(currency AS STRING)                            AS currency,
        CAST(mid_price AS DECIMAL(18,8))                    AS mid_price,
        CAST(bid_price AS DECIMAL(18,8))                    AS bid_price,
        CAST(ask_price AS DECIMAL(18,8))                    AS ask_price,
        'MARKET_DATA_FEED'                                  AS source_system,
        _metadata.file_path                                 AS _source_file,
        CURRENT_TIMESTAMP()                                 AS _ingested_at,
        CAST(NULL AS STRING)                                AS _rescued_data
    FROM '${landing_path}'
)
FILEFORMAT = CSV
PATTERN = 'market_data*.csv'
FORMAT_OPTIONS (
    'header'      = 'true',
    'sep'         = '${delimiter}',
    'inferSchema' = 'false',
    -- PERMISSIVE keeps bad records instead of aborting the whole load, the
    -- behaviour FastLoad achieved with ERRORFILES.
    'mode'        = 'PERMISSIVE'
)
COPY_OPTIONS (
    'mergeSchema' = 'false'
);

-- Post-load count, replacing the FastLoad end-of-run statistics block.
SELECT
    COUNT(*)                AS rows_in_target,
    MAX(valuation_date)     AS max_valuation_date,
    MAX(_ingested_at)       AS last_load_ts
FROM ${catalog}.${schema_raw}.market_data;
