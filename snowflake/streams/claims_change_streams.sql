/*******************************************************************************
 * Healthcare Claims - Change Data Capture Streams
 *
 * Snowflake Streams provide change data capture (CDC) on raw tables.
 * These streams are consumed by the daily pipeline Tasks for incremental
 * processing - only new/changed records are processed each run.
 *
 * Snowflake-specific features:
 *   - CREATE STREAM on tables with CHANGE_TRACKING = TRUE
 *   - APPEND_ONLY mode for insert-only tables (raw landing)
 *   - Standard mode for tables with updates
 *   - SYSTEM$STREAM_HAS_DATA for conditional Task execution
 *   - Metadata columns: METADATA$ACTION, METADATA$ISUPDATE, METADATA$ROW_ID
 ******************************************************************************/

USE DATABASE CLAIMS_DW;
USE SCHEMA RAW;

-- =============================================================================
-- Stream: RAW_MEMBER_ELIGIBILITY
-- Captures new eligibility records for incremental member dimension updates
-- =============================================================================
CREATE OR REPLACE STREAM CLAIMS_DW.RAW.STREAM_RAW_MEMBER_ELIGIBILITY
    ON TABLE CLAIMS_DW.RAW.RAW_MEMBER_ELIGIBILITY
    APPEND_ONLY = FALSE
    SHOW_INITIAL_ROWS = FALSE
    COMMENT = 'CDC stream on member eligibility - triggers DIM_MEMBER SCD2 updates';


-- =============================================================================
-- Stream: RAW_MEDICAL_CLAIM
-- Captures new and changed medical claims for ADR dedup processing
-- =============================================================================
CREATE OR REPLACE STREAM CLAIMS_DW.RAW.STREAM_RAW_MEDICAL_CLAIM
    ON TABLE CLAIMS_DW.RAW.RAW_MEDICAL_CLAIM
    APPEND_ONLY = FALSE
    SHOW_INITIAL_ROWS = FALSE
    COMMENT = 'CDC stream on medical claims - triggers ADR dedup and encounter grouping';


-- =============================================================================
-- Stream: RAW_PHARMACY_CLAIM
-- Captures new and changed pharmacy claims for ADR dedup processing
-- =============================================================================
CREATE OR REPLACE STREAM CLAIMS_DW.RAW.STREAM_RAW_PHARMACY_CLAIM
    ON TABLE CLAIMS_DW.RAW.RAW_PHARMACY_CLAIM
    APPEND_ONLY = FALSE
    SHOW_INITIAL_ROWS = FALSE
    COMMENT = 'CDC stream on pharmacy claims - triggers ADR dedup processing';


-- =============================================================================
-- Stream: FCT_MEDICAL_CLAIM (warehouse layer)
-- Captures changes to fact table for mart refreshes
-- =============================================================================
CREATE OR REPLACE STREAM CLAIMS_DW.WAREHOUSE.STREAM_FCT_MEDICAL_CLAIM
    ON TABLE CLAIMS_DW.WAREHOUSE.FCT_MEDICAL_CLAIM
    APPEND_ONLY = FALSE
    SHOW_INITIAL_ROWS = FALSE
    COMMENT = 'CDC stream on medical claim facts - triggers mart aggregation refresh';


-- =============================================================================
-- Stream: FCT_PHARMACY_CLAIM (warehouse layer)
-- =============================================================================
CREATE OR REPLACE STREAM CLAIMS_DW.WAREHOUSE.STREAM_FCT_PHARMACY_CLAIM
    ON TABLE CLAIMS_DW.WAREHOUSE.FCT_PHARMACY_CLAIM
    APPEND_ONLY = FALSE
    SHOW_INITIAL_ROWS = FALSE
    COMMENT = 'CDC stream on pharmacy claim facts - triggers mart aggregation refresh';


-- =============================================================================
-- Stream: FCT_ENCOUNTER (warehouse layer)
-- =============================================================================
CREATE OR REPLACE STREAM CLAIMS_DW.WAREHOUSE.STREAM_FCT_ENCOUNTER
    ON TABLE CLAIMS_DW.WAREHOUSE.FCT_ENCOUNTER
    APPEND_ONLY = FALSE
    SHOW_INITIAL_ROWS = FALSE
    COMMENT = 'CDC stream on encounter facts - triggers encounter summary refresh';


-- =============================================================================
-- Stream: DIM_MEMBER (warehouse layer)
-- Captures member dimension changes for downstream mart updates
-- =============================================================================
CREATE OR REPLACE STREAM CLAIMS_DW.WAREHOUSE.STREAM_DIM_MEMBER
    ON TABLE CLAIMS_DW.WAREHOUSE.DIM_MEMBER
    APPEND_ONLY = FALSE
    SHOW_INITIAL_ROWS = FALSE
    COMMENT = 'CDC stream on member dimension - triggers member month recalculation';


-- =============================================================================
-- Utility: Check if streams have data (used by Tasks WHEN clause)
-- =============================================================================
-- SELECT SYSTEM$STREAM_HAS_DATA('CLAIMS_DW.RAW.STREAM_RAW_MEMBER_ELIGIBILITY');
-- SELECT SYSTEM$STREAM_HAS_DATA('CLAIMS_DW.RAW.STREAM_RAW_MEDICAL_CLAIM');
-- SELECT SYSTEM$STREAM_HAS_DATA('CLAIMS_DW.RAW.STREAM_RAW_PHARMACY_CLAIM');


-- =============================================================================
-- Example: Query a stream to see pending changes
-- =============================================================================
-- SELECT
--     METADATA$ACTION       AS stream_action,
--     METADATA$ISUPDATE     AS is_update,
--     METADATA$ROW_ID       AS row_id,
--     member_id,
--     first_name,
--     last_name,
--     plan_code,
--     load_timestamp
-- FROM CLAIMS_DW.RAW.STREAM_RAW_MEMBER_ELIGIBILITY
-- LIMIT 100;


-- =============================================================================
-- Example: Incremental processing pattern using a stream
-- This pattern is used internally by the pipeline Tasks
-- =============================================================================
-- BEGIN
--     -- Process new records from the stream
--     INSERT INTO CLAIMS_DW.WAREHOUSE.some_target_table
--     SELECT ...
--     FROM CLAIMS_DW.RAW.STREAM_RAW_MEDICAL_CLAIM s
--     WHERE s.METADATA$ACTION = 'INSERT';
--
--     -- The stream offset advances automatically after a DML consumes it
--     -- within a transaction. No manual offset management needed.
-- END;
