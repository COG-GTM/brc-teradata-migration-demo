/******************************************************************************
 * task_customer_export.sql
 *
 * Snowflake Task replacing Teradata BTEQ customer export.
 *
 * MIGRATED FROM:
 *   - teradata/bteq/customer_export.bteq
 *   - BRCL_DAILY_006_EXPORT_CUSTOMERS in daily_etl_sequence.txt
 *
 * Original Teradata pattern:
 *   .SET SEPARATOR '|'
 *   .EXPORT DATA FILE=${EXPORT_DIR}/customer_export_${YYYYMMDD}.dat
 *   SELECT ... FROM DIM_CUSTOMER WHERE is_current = 'Y'
 *   .EXPORT RESET
 *
 * Snowflake replacement:
 *   COPY INTO @stage with pipe-delimited format
 *   Preserves original column order and date formats
 ******************************************************************************/

USE ROLE sysadmin;
USE DATABASE barclays_dwh;
USE SCHEMA orchestration;

-- =============================================================================
-- FILE FORMAT: Matches original BTEQ pipe-delimited export
-- Original: .SET SEPARATOR '|' / .SET TITLEDASHES OFF
-- =============================================================================
CREATE OR REPLACE FILE FORMAT barclays_customer_export_fmt
  TYPE = 'CSV'
  FIELD_DELIMITER = '|'
  RECORD_DELIMITER = '\n'
  FIELD_OPTIONALLY_ENCLOSED_BY = NONE
  DATE_FORMAT = 'YYYY-MM-DD'
  TIMESTAMP_FORMAT = 'YYYY-MM-DD HH24:MI:SS'
  COMPRESSION = 'NONE'
  NULL_IF = ('')
  SKIP_HEADER = 0;

-- =============================================================================
-- EXTERNAL STAGE: Target location for exported files
-- Replaces: ${EXPORT_DIR} from BTEQ .EXPORT
-- =============================================================================
CREATE OR REPLACE STAGE barclays_customer_export_stage
  URL = 's3://barclays-data-exports/customer/'
  STORAGE_INTEGRATION = barclays_s3_integration
  FILE_FORMAT = barclays_customer_export_fmt;

-- =============================================================================
-- TASK: Customer export
-- Replaces: BRCL_DAILY_006_EXPORT_CUSTOMERS
-- Runs after daily_validation (same dependency as original UC4 sequence)
-- =============================================================================
CREATE OR REPLACE TASK customer_export
  WAREHOUSE = compute_wh
  AFTER daily_validation
  COMMENT   = 'Export current customers pipe-delimited; replaces customer_export.bteq'
AS
BEGIN
  LET v_export_date STRING := TO_CHAR(CURRENT_DATE(), 'YYYYMMDD');

  INSERT INTO barclays_dwh.etl_audit_log (
      pipeline_name, step_name, status, row_count, started_at
  ) VALUES ('DAILY_ETL', 'CUSTOMER_EXPORT', 'RUNNING', 0, CURRENT_TIMESTAMP());

  -- Export matching original BTEQ column order and format
  -- Original: customer_id, first_name, last_name, date_of_birth, nationality,
  --           kyc_status, risk_rating, segment, postcode, country,
  --           effective_from, effective_to, is_current
  COPY INTO @barclays_customer_export_stage/customer_export_${v_export_date}.dat
  FROM (
      SELECT
          customer_id,
          first_name,
          last_name,
          TO_CHAR(date_of_birth, 'YYYY-MM-DD')    AS date_of_birth,
          nationality,
          kyc_status,
          risk_rating,
          segment,
          postcode,
          country,
          TO_CHAR(effective_from, 'YYYY-MM-DD')    AS effective_from,
          TO_CHAR(effective_to, 'YYYY-MM-DD')      AS effective_to,
          is_current
      FROM barclays_dwh.dim_customer
      WHERE is_current = 'Y'
      ORDER BY customer_id
  )
  FILE_FORMAT = (FORMAT_NAME = 'barclays_customer_export_fmt')
  SINGLE = TRUE
  HEADER = FALSE
  OVERWRITE = TRUE;

  LET v_export_count INTEGER := (
      SELECT COUNT(*)
      FROM barclays_dwh.dim_customer
      WHERE is_current = 'Y'
  );

  INSERT INTO barclays_dwh.etl_audit_log (
      pipeline_name, step_name, status, row_count, started_at, completed_at, details
  ) VALUES (
      'DAILY_ETL', 'CUSTOMER_EXPORT', 'SUCCESS', v_export_count,
      CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(),
      CONCAT('Exported ', v_export_count, ' customer records to customer_export_', :v_export_date, '.dat')
  );
END;

-- =============================================================================
-- ENABLE THE TASK
-- =============================================================================
ALTER TASK customer_export RESUME;
