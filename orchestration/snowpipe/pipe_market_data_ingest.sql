/******************************************************************************
 * pipe_market_data_ingest.sql
 *
 * Snowpipe auto-ingest replacing Teradata FastLoad for market data.
 *
 * MIGRATED FROM:
 *   - teradata/fastload/market_data_load.fl
 *   - BRCL_DAILY_002_FASTLOAD_MKTDATA in daily_etl_sequence.txt
 *
 * Original FastLoad pattern:
 *   BEGIN LOADING ... ERRORFILES ... CHECKPOINT 5000
 *   SET RECORD VARTEXT '|'
 *   DEFINE ... FILE=${INPUT_DIR}/market_data_${YYYYMMDD}.dat
 *   INSERT INTO BARCLAYS_RAW.MARKET_DATA VALUES (...)
 *   END LOADING
 *
 * Snowpipe replacement:
 *   Auto-ingest from cloud storage with same pipe-delimited format
 *   Handles FX rates, interest rates, equity prices, bond yields
 ******************************************************************************/

USE ROLE sysadmin;
USE DATABASE barclays_raw;
USE SCHEMA public;

-- =============================================================================
-- FILE FORMAT: Market data files
-- Matches original FastLoad VARTEXT '|' format
-- =============================================================================
CREATE OR REPLACE FILE FORMAT market_data_ingest_fmt
  TYPE = 'CSV'
  FIELD_DELIMITER = '|'
  RECORD_DELIMITER = '\n'
  SKIP_HEADER = 0            -- FastLoad had no header row
  DATE_FORMAT = 'YYYY-MM-DD'
  NULL_IF = ('NULL', '', '\\N')
  TRIM_SPACE = TRUE
  ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE;

-- =============================================================================
-- EXTERNAL STAGE: Market data landing zone
-- Replaces: ${INPUT_DIR}/market_data_${YYYYMMDD}.dat
-- =============================================================================
CREATE OR REPLACE STAGE market_data_ingest_stage
  URL = 's3://barclays-raw-data/market_data/'
  STORAGE_INTEGRATION = barclays_s3_integration
  FILE_FORMAT = market_data_ingest_fmt;

-- =============================================================================
-- RAW LANDING TABLE
-- Matches original Teradata BARCLAYS_RAW.MARKET_DATA schema
-- Column order matches FastLoad DEFINE section
-- =============================================================================
CREATE TABLE IF NOT EXISTS barclays_raw.market_data (
    instrument_id         VARCHAR(20),
    valuation_date        DATE,
    instrument_type       VARCHAR(30),     -- FX, INTEREST_RATE, EQUITY, BOND
    instrument_name       VARCHAR(100),
    currency              VARCHAR(3),
    mid_price             DECIMAL(18,8),
    bid_price             DECIMAL(18,8),
    ask_price             DECIMAL(18,8),
    source_system         VARCHAR(50),
    -- Snowflake ingestion metadata
    _loaded_at            TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    _source_file          VARCHAR(500)  DEFAULT METADATA$FILENAME,
    _source_file_row_num  INTEGER       DEFAULT METADATA$FILE_ROW_NUMBER
);

-- =============================================================================
-- SNOWPIPE: Auto-ingest market data
-- Replaces: FastLoad market_data_load.fl + BRCL_DAILY_002_FASTLOAD_MKTDATA
--
-- The original FastLoad:
--   1. Dropped error tables (MKTDATA_FL_ET, MKTDATA_FL_UV)
--   2. Loaded via BEGIN LOADING ... END LOADING
--   3. CHECKPOINT every 5000 rows
--
-- Snowpipe handles all of this automatically:
--   - Error handling via ON_ERROR + COPY_HISTORY
--   - Checkpointing is implicit (file-level atomicity)
--   - Error tables replaced by COPY_HISTORY + VALIDATE function
-- =============================================================================
CREATE OR REPLACE PIPE barclays_raw.market_data_ingest_pipe
  AUTO_INGEST = TRUE
  INTEGRATION = 'barclays_s3_notification'
  ERROR_INTEGRATION = 'barclays_error_notification'
  COMMENT = 'Auto-ingest market data from S3; replaces FastLoad market_data_load.fl'
AS
  COPY INTO barclays_raw.market_data (
      instrument_id, valuation_date, instrument_type, instrument_name,
      currency, mid_price, bid_price, ask_price, source_system,
      _loaded_at, _source_file, _source_file_row_num
  )
  FROM (
      SELECT
          $1,                                                 -- instrument_id (VARCHAR)
          TO_DATE($2, 'YYYY-MM-DD'),                          -- valuation_date
          $3,                                                 -- instrument_type
          $4,                                                 -- instrument_name
          $5,                                                 -- currency
          TO_DECIMAL($6, 18, 8),                              -- mid_price
          TO_DECIMAL($7, 18, 8),                              -- bid_price
          TO_DECIMAL($8, 18, 8),                              -- ask_price
          $9,                                                 -- source_system
          CURRENT_TIMESTAMP(),
          METADATA$FILENAME,
          METADATA$FILE_ROW_NUMBER
      FROM @market_data_ingest_stage
  )
  FILE_FORMAT = (FORMAT_NAME = 'market_data_ingest_fmt')
  ON_ERROR = 'CONTINUE';

-- =============================================================================
-- VALIDATION QUERIES (replaces FastLoad error table checks)
-- =============================================================================

-- Check pipe status
-- SELECT SYSTEM$PIPE_STATUS('barclays_raw.market_data_ingest_pipe');

-- Validate recent loads
-- SELECT
--     file_name,
--     status,
--     rows_parsed,
--     rows_loaded,
--     error_count,
--     first_error_message,
--     last_load_time
-- FROM TABLE(information_schema.copy_history(
--     table_name => 'barclays_raw.market_data',
--     start_time => DATEADD('hour', -24, CURRENT_TIMESTAMP())
-- ))
-- ORDER BY last_load_time DESC;

-- Validate loaded data (equivalent to checking FastLoad error tables UV/ET)
-- SELECT *
-- FROM TABLE(VALIDATE(barclays_raw.market_data,
--     job_id => '_last'));

-- =============================================================================
-- MARKET DATA QUALITY CHECKS
-- Run after ingestion to validate data integrity
-- =============================================================================
CREATE OR REPLACE TASK market_data_ingest_validation
  WAREHOUSE = compute_wh
  SCHEDULE  = 'USING CRON 15 6 * * * UTC'
  COMMENT   = 'Validate market data ingestion quality; replaces FastLoad error table checks'
AS
BEGIN
  LET v_issues INTEGER := 0;

  -- Check 1: All expected instrument types present for today
  LET v_types_count INTEGER := (
      SELECT COUNT(DISTINCT instrument_type)
      FROM barclays_raw.market_data
      WHERE valuation_date = CURRENT_DATE()
  );

  -- Expect at least 3 types: FX, INTEREST_RATE, EQUITY (BOND may not be daily)
  IF (v_types_count < 3) THEN
      v_issues := v_issues + 1;
      INSERT INTO barclays_dwh.etl_validation_results (
          validation_date, check_name, status, expected_value, actual_value, details
      ) VALUES (
          CURRENT_DATE(), 'MARKET_DATA_TYPE_COVERAGE', 'WARNING',
          '>= 3', v_types_count::STRING,
          'Expected FX, INTEREST_RATE, and EQUITY data for today'
      );
  END IF;

  -- Check 2: No negative prices
  LET v_neg_prices INTEGER := (
      SELECT COUNT(*)
      FROM barclays_raw.market_data
      WHERE valuation_date = CURRENT_DATE()
        AND (mid_price < 0 OR bid_price < 0 OR ask_price < 0)
  );
  IF (v_neg_prices > 0) THEN
      v_issues := v_issues + 1;
      INSERT INTO barclays_dwh.etl_validation_results (
          validation_date, check_name, status, expected_value, actual_value, details
      ) VALUES (
          CURRENT_DATE(), 'MARKET_DATA_NEGATIVE_PRICES', 'FAIL',
          '0', v_neg_prices::STRING,
          'Negative prices detected in market data'
      );
  END IF;

  -- Check 3: Bid/ask spread consistency (bid <= mid <= ask)
  LET v_spread_violations INTEGER := (
      SELECT COUNT(*)
      FROM barclays_raw.market_data
      WHERE valuation_date = CURRENT_DATE()
        AND bid_price IS NOT NULL
        AND ask_price IS NOT NULL
        AND (bid_price > ask_price OR mid_price < bid_price OR mid_price > ask_price)
  );
  IF (v_spread_violations > 0) THEN
      v_issues := v_issues + 1;
      INSERT INTO barclays_dwh.etl_validation_results (
          validation_date, check_name, status, expected_value, actual_value, details
      ) VALUES (
          CURRENT_DATE(), 'MARKET_DATA_SPREAD_CONSISTENCY', 'WARNING',
          '0', v_spread_violations::STRING,
          'Bid/ask spread violations: expected bid <= mid <= ask'
      );
  END IF;

  -- Check copy history for ingestion errors
  LET v_error_files INTEGER := (
      SELECT COUNT(*)
      FROM TABLE(information_schema.copy_history(
          table_name => 'barclays_raw.market_data',
          start_time => DATEADD('hour', -24, CURRENT_TIMESTAMP())
      ))
      WHERE error_count > 0
  );

  INSERT INTO barclays_dwh.etl_audit_log (
      pipeline_name, step_name, status, row_count, started_at, completed_at, details
  ) VALUES (
      'DAILY_ETL', 'SNOWPIPE_MKTDATA_VALIDATION',
      CASE WHEN v_issues = 0 THEN 'SUCCESS' ELSE 'WARNING' END,
      v_types_count,
      CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(),
      OBJECT_CONSTRUCT(
          'instrument_types_today', v_types_count,
          'negative_prices', v_neg_prices,
          'spread_violations', v_spread_violations,
          'files_with_errors', v_error_files
      )::STRING
  );
END;

ALTER TASK market_data_ingest_validation RESUME;
