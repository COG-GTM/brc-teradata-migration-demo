/******************************************************************************
 * pipe_transaction_ingest.sql
 *
 * Snowpipe auto-ingest replacing Teradata BTEQ .IMPORT and TPT patterns
 * for raw transaction data ingestion.
 *
 * MIGRATED FROM:
 *   - BRCL_DAILY_003_TPT_TRANSACTIONS (transaction_load.tpt)
 *   - BRCL_DAILY_004_MLOAD_BALANCES (account_balance_upsert.ml)
 *   - BTEQ .IMPORT patterns in daily_batch_load.bteq
 *
 * Snowpipe continuously loads files as they land in cloud storage,
 * replacing the batch-oriented FastLoad/TPT/MultiLoad paradigm.
 *
 * Data flow:
 *   Cloud Storage (S3/Azure/GCS)
 *     -> Snowpipe (auto-ingest via event notification)
 *       -> barclays_raw.transaction (raw landing table)
 *         -> raw_transaction_stream (CDC stream)
 *           -> Snowflake Tasks process incremental changes
 ******************************************************************************/

USE ROLE sysadmin;
USE DATABASE barclays_raw;
USE SCHEMA public;

-- =============================================================================
-- FILE FORMAT: Transaction data files
-- Matches the pipe-delimited format used by upstream source systems
-- =============================================================================
CREATE OR REPLACE FILE FORMAT transaction_ingest_fmt
  TYPE = 'CSV'
  FIELD_DELIMITER = '|'
  RECORD_DELIMITER = '\n'
  SKIP_HEADER = 1
  DATE_FORMAT = 'YYYY-MM-DD'
  TIMESTAMP_FORMAT = 'YYYY-MM-DD HH24:MI:SS.FF6'
  NULL_IF = ('NULL', '', '\\N')
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
  TRIM_SPACE = TRUE;

-- =============================================================================
-- EXTERNAL STAGE: Source data landing zone
-- Replaces: ${INPUT_DIR} from TPT/BTEQ scripts
-- =============================================================================
CREATE OR REPLACE STAGE transaction_ingest_stage
  URL = 's3://barclays-raw-data/transactions/'
  STORAGE_INTEGRATION = barclays_s3_integration
  FILE_FORMAT = transaction_ingest_fmt;

-- =============================================================================
-- RAW LANDING TABLE
-- Receives all raw transaction data; includes ingestion metadata
-- =============================================================================
CREATE TABLE IF NOT EXISTS barclays_raw.transaction (
    transaction_id        VARCHAR(50),
    account_id            VARCHAR(30),
    transaction_date      DATE,
    transaction_type      VARCHAR(30),
    channel               VARCHAR(20),
    amount                DECIMAL(18,2),
    signed_amount         DECIMAL(18,2),
    currency              VARCHAR(3),
    value_band            VARCHAR(20),
    balance_after         DECIMAL(18,2),
    counterparty_id       VARCHAR(50),
    description           VARCHAR(500),
    reference_number      VARCHAR(50),
    -- Snowflake ingestion metadata (not in Teradata original)
    _loaded_at            TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    _source_file          VARCHAR(500)  DEFAULT METADATA$FILENAME,
    _source_file_row_num  INTEGER       DEFAULT METADATA$FILE_ROW_NUMBER
);

-- =============================================================================
-- SNOWPIPE: Auto-ingest transactions
-- Replaces: TPT transaction_load.tpt + BRCL_DAILY_003_TPT_TRANSACTIONS
--
-- Auto-ingest mode: Snowpipe monitors the stage via cloud event notifications
-- (S3 SQS, Azure Event Grid, or GCS Pub/Sub) and loads new files automatically.
-- =============================================================================
CREATE OR REPLACE PIPE barclays_raw.transaction_ingest_pipe
  AUTO_INGEST = TRUE
  INTEGRATION = 'barclays_s3_notification'
  ERROR_INTEGRATION = 'barclays_error_notification'
  COMMENT = 'Auto-ingest raw transactions from S3; replaces TPT transaction_load.tpt'
AS
  COPY INTO barclays_raw.transaction (
      transaction_id, account_id, transaction_date, transaction_type,
      channel, amount, signed_amount, currency, value_band,
      balance_after, counterparty_id, description, reference_number,
      _loaded_at, _source_file, _source_file_row_num
  )
  FROM (
      SELECT
          $1,   -- transaction_id
          $2,   -- account_id
          $3,   -- transaction_date
          $4,   -- transaction_type
          $5,   -- channel
          $6,   -- amount
          $7,   -- signed_amount
          $8,   -- currency
          $9,   -- value_band
          $10,  -- balance_after
          $11,  -- counterparty_id
          $12,  -- description
          $13,  -- reference_number
          CURRENT_TIMESTAMP(),
          METADATA$FILENAME,
          METADATA$FILE_ROW_NUMBER
      FROM @transaction_ingest_stage
  )
  FILE_FORMAT = (FORMAT_NAME = 'transaction_ingest_fmt')
  ON_ERROR = 'CONTINUE';

-- =============================================================================
-- COPY HISTORY VALIDATION
-- Replaces post-load error table checks from TPT/FastLoad
--
-- Query to check for ingestion errors (run after pipe processes files):
-- =============================================================================

-- View pipe status
-- SELECT SYSTEM$PIPE_STATUS('barclays_raw.transaction_ingest_pipe');

-- Check recent copy history for errors
-- SELECT
--     file_name,
--     status,
--     rows_parsed,
--     rows_loaded,
--     error_count,
--     first_error_message,
--     first_error_line_num,
--     first_error_character_pos,
--     last_load_time
-- FROM TABLE(information_schema.copy_history(
--     table_name => 'barclays_raw.transaction',
--     start_time => DATEADD('hour', -24, CURRENT_TIMESTAMP())
-- ))
-- ORDER BY last_load_time DESC;

-- Check for files with errors (equivalent to checking FastLoad error tables)
-- SELECT
--     file_name,
--     error_count,
--     first_error_message,
--     rows_parsed - rows_loaded AS rejected_rows
-- FROM TABLE(information_schema.copy_history(
--     table_name => 'barclays_raw.transaction',
--     start_time => DATEADD('hour', -24, CURRENT_TIMESTAMP())
-- ))
-- WHERE error_count > 0
-- ORDER BY last_load_time DESC;

-- =============================================================================
-- VALIDATION TASK: Runs after Snowpipe loads to validate data quality
-- This replaces the post-FastLoad error table checks
-- =============================================================================
CREATE OR REPLACE TASK transaction_ingest_validation
  WAREHOUSE = compute_wh
  SCHEDULE  = 'USING CRON 30 6 * * * UTC'
  COMMENT   = 'Validate Snowpipe transaction ingestion; replaces FastLoad error table checks'
AS
BEGIN
  -- Check copy history for any errors in the last 24 hours
  LET v_error_files INTEGER := (
      SELECT COUNT(*)
      FROM TABLE(information_schema.copy_history(
          table_name => 'barclays_raw.transaction',
          start_time => DATEADD('hour', -24, CURRENT_TIMESTAMP())
      ))
      WHERE error_count > 0
  );

  LET v_total_loaded INTEGER := (
      SELECT COALESCE(SUM(rows_loaded), 0)
      FROM TABLE(information_schema.copy_history(
          table_name => 'barclays_raw.transaction',
          start_time => DATEADD('hour', -24, CURRENT_TIMESTAMP())
      ))
  );

  IF (v_error_files > 0) THEN
      INSERT INTO barclays_dwh.etl_validation_results (
          validation_date, check_name, status, expected_value, actual_value, details
      ) VALUES (
          CURRENT_DATE(), 'SNOWPIPE_TXN_INGEST_ERRORS', 'WARNING',
          '0', v_error_files::STRING,
          CONCAT(v_error_files, ' files had ingestion errors. Check COPY_HISTORY for details.')
      );
  END IF;

  INSERT INTO barclays_dwh.etl_audit_log (
      pipeline_name, step_name, status, row_count, started_at, completed_at, details
  ) VALUES (
      'DAILY_ETL', 'SNOWPIPE_TXN_VALIDATION',
      CASE WHEN v_error_files = 0 THEN 'SUCCESS' ELSE 'WARNING' END,
      v_total_loaded,
      CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(),
      OBJECT_CONSTRUCT(
          'files_with_errors', v_error_files,
          'total_rows_loaded', v_total_loaded
      )::STRING
  );
END;

ALTER TASK transaction_ingest_validation RESUME;
