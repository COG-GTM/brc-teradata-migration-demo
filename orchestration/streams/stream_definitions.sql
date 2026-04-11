/******************************************************************************
 * stream_definitions.sql
 *
 * Snowflake Streams for change data capture (CDC) on raw tables.
 *
 * Streams detect new/changed data and are consumed by Snowflake Tasks
 * to process only incremental changes — replacing the Teradata pattern of
 * full-table scans with date filters.
 *
 * Original Teradata pattern:
 *   WHERE transaction_date = CURRENT_DATE  (full partition scan with PPI)
 *
 * Snowflake pattern:
 *   STREAM tracks micro-partition-level changes automatically
 *   Tasks check SYSTEM$STREAM_HAS_DATA() before processing
 *
 * Stream types used:
 *   STANDARD (default) — tracks INSERTs, UPDATEs, DELETEs
 *   APPEND_ONLY — tracks only INSERTs (for raw landing tables)
 ******************************************************************************/

USE ROLE sysadmin;
USE DATABASE barclays_raw;
USE SCHEMA public;

-- =============================================================================
-- RAW LAYER STREAMS (append-only for landing tables)
-- These detect new data arriving via Snowpipe
-- =============================================================================

-- Stream on raw transactions (populated by Snowpipe)
-- Used by: raw_data_validation task (WHEN clause)
-- Used by: daily_transaction_load task (incremental processing)
CREATE OR REPLACE STREAM barclays_raw.raw_transaction_stream
  ON TABLE barclays_raw.transaction
  APPEND_ONLY = TRUE
  SHOW_INITIAL_ROWS = FALSE
  COMMENT = 'Tracks new transactions from Snowpipe; consumed by daily ETL tasks';

-- Stream on raw market data (populated by Snowpipe)
-- Used by: raw_data_validation task (WHEN clause)
-- Used by: market_data_load task (incremental processing)
CREATE OR REPLACE STREAM barclays_raw.raw_market_data_stream
  ON TABLE barclays_raw.market_data
  APPEND_ONLY = TRUE
  SHOW_INITIAL_ROWS = FALSE
  COMMENT = 'Tracks new market data from Snowpipe; consumed by market data load task';

-- Stream on raw account data
-- Used by: daily_transaction_load task (account upserts)
CREATE OR REPLACE STREAM barclays_raw.raw_account_stream
  ON TABLE barclays_raw.account
  APPEND_ONLY = TRUE
  SHOW_INITIAL_ROWS = FALSE
  COMMENT = 'Tracks new/updated account data for dimension upserts';

-- Stream on raw customer data
-- Used by: daily_transaction_load task (SCD2 processing)
CREATE OR REPLACE STREAM barclays_raw.raw_customer_stream
  ON TABLE barclays_raw.customer
  APPEND_ONLY = TRUE
  SHOW_INITIAL_ROWS = FALSE
  COMMENT = 'Tracks new/updated customer data for SCD Type 2 processing';

-- =============================================================================
-- WAREHOUSE LAYER STREAMS (standard CDC for dimension/fact tables)
-- These detect changes in the DWH layer for downstream mart processing
-- =============================================================================

USE DATABASE barclays_dwh;

-- Stream on customer dimension (tracks SCD2 changes)
-- Used by: customer_risk_scoring task (re-score changed customers)
CREATE OR REPLACE STREAM barclays_dwh.dim_customer_stream
  ON TABLE barclays_dwh.dim_customer
  APPEND_ONLY = FALSE
  SHOW_INITIAL_ROWS = FALSE
  COMMENT = 'Tracks SCD2 changes in customer dimension; triggers risk re-scoring';

-- Stream on account dimension (tracks upserts)
-- Used by: balance snapshot calculations
CREATE OR REPLACE STREAM barclays_dwh.dim_account_stream
  ON TABLE barclays_dwh.dim_account
  APPEND_ONLY = FALSE
  SHOW_INITIAL_ROWS = FALSE
  COMMENT = 'Tracks account dimension changes for downstream processing';

-- Stream on transaction fact table
-- Used by: AML screening (detect new transactions for compliance checks)
-- Used by: Balance snapshot updates
CREATE OR REPLACE STREAM barclays_dwh.fct_transaction_stream
  ON TABLE barclays_dwh.fct_transaction
  APPEND_ONLY = TRUE
  SHOW_INITIAL_ROWS = FALSE
  COMMENT = 'Tracks new transactions for AML screening and balance snapshots';

-- Stream on daily balance fact
-- Used by: Risk scoring (latest balance lookups)
CREATE OR REPLACE STREAM barclays_dwh.fct_daily_balance_stream
  ON TABLE barclays_dwh.fct_daily_balance
  APPEND_ONLY = FALSE
  SHOW_INITIAL_ROWS = FALSE
  COMMENT = 'Tracks balance changes for risk scoring inputs';

-- =============================================================================
-- MART LAYER STREAMS (for monitoring and downstream consumers)
-- =============================================================================

USE DATABASE barclays_mart;

-- Stream on AML alerts (for real-time compliance dashboard)
CREATE OR REPLACE STREAM barclays_mart.aml_alerts_stream
  ON TABLE barclays_mart.mart_aml_alerts
  APPEND_ONLY = TRUE
  SHOW_INITIAL_ROWS = FALSE
  COMMENT = 'Tracks new AML alerts for real-time compliance dashboards';

-- Stream on credit risk scores (for risk dashboard updates)
CREATE OR REPLACE STREAM barclays_mart.credit_risk_stream
  ON TABLE barclays_mart.mart_credit_risk
  APPEND_ONLY = FALSE
  SHOW_INITIAL_ROWS = FALSE
  COMMENT = 'Tracks risk score changes for dashboard and regulatory reporting';

-- =============================================================================
-- STREAM MONITORING QUERIES
-- =============================================================================

-- Check which streams have unprocessed data:
-- SELECT
--     stream_catalog || '.' || stream_schema || '.' || stream_name AS stream_fqn,
--     table_catalog || '.' || table_schema || '.' || table_name   AS source_table,
--     type,
--     stale,
--     stale_after
-- FROM information_schema.streams
-- WHERE stream_catalog IN ('BARCLAYS_RAW', 'BARCLAYS_DWH', 'BARCLAYS_MART')
-- ORDER BY stream_catalog, stream_schema, stream_name;

-- Check if a specific stream has data:
-- SELECT SYSTEM$STREAM_HAS_DATA('barclays_raw.raw_transaction_stream');

-- Preview stream contents (without consuming):
-- SELECT *
-- FROM barclays_raw.raw_transaction_stream
-- LIMIT 10;

-- IMPORTANT: Streams are consumed (advanced) when the data is used in a DML
-- statement within a transaction. Ensure Tasks properly consume streams to
-- prevent staleness. Streams become stale if not consumed within the table's
-- DATA_RETENTION_TIME_IN_DAYS (default 1 day on Standard Edition).
