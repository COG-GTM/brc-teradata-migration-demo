-- =============================================================================
-- Snowflake Streams: Change Data Capture
-- Replaces:
--   - Teradata COLLECT STATISTICS (tracking data changes)
--   - Databricks Delta Lake change data feed
--   - Manual change detection via timestamp columns
--
-- Streams track DML changes on raw tables to enable efficient
-- incremental processing in the dbt pipeline.
-- =============================================================================

-- Stream on customer table (for SCD Type 2 detection)
CREATE OR REPLACE STREAM healthcare_consolidated.streams.customer_changes
    ON TABLE healthcare_consolidated.raw.customer
    APPEND_ONLY = FALSE
    SHOW_INITIAL_ROWS = FALSE
    COMMENT = 'Track customer changes for SCD Type 2 processing. Replaces Teradata cursor-based change detection.';


-- Stream on account table
CREATE OR REPLACE STREAM healthcare_consolidated.streams.account_changes
    ON TABLE healthcare_consolidated.raw.account
    APPEND_ONLY = FALSE
    SHOW_INITIAL_ROWS = FALSE
    COMMENT = 'Track account changes for incremental processing.';


-- Stream on transaction table (append-only for efficiency)
CREATE OR REPLACE STREAM healthcare_consolidated.streams.transaction_inserts
    ON TABLE healthcare_consolidated.raw.transaction
    APPEND_ONLY = TRUE
    SHOW_INITIAL_ROWS = FALSE
    COMMENT = 'Track new transaction inserts. Append-only for high-volume table efficiency.';


-- Stream on counterparty table (for sanctions list updates)
CREATE OR REPLACE STREAM healthcare_consolidated.streams.counterparty_changes
    ON TABLE healthcare_consolidated.raw.counterparty
    APPEND_ONLY = FALSE
    SHOW_INITIAL_ROWS = FALSE
    COMMENT = 'Track counterparty changes for AML screening triggers.';


-- Task to process customer changes (triggered by stream)
CREATE OR REPLACE TASK healthcare_consolidated.tasks.process_customer_changes
    WAREHOUSE = 'COMPUTE_WH'
    SCHEDULE = '5 MINUTE'
    WHEN SYSTEM$STREAM_HAS_DATA('healthcare_consolidated.streams.customer_changes')
    COMMENT = 'Process customer changes detected by stream. Near-real-time SCD2 updates.'
AS
    CALL healthcare_consolidated.procedures.run_dbt_command(
        'run --select stg_teradata__customer int_unified_customer'
    );


-- Task to process transaction inserts (triggered by stream)
CREATE OR REPLACE TASK healthcare_consolidated.tasks.process_transaction_inserts
    WAREHOUSE = 'COMPUTE_WH'
    SCHEDULE = '5 MINUTE'
    WHEN SYSTEM$STREAM_HAS_DATA('healthcare_consolidated.streams.transaction_inserts')
    COMMENT = 'Process new transactions detected by stream. Near-real-time enrichment.'
AS
    CALL healthcare_consolidated.procedures.run_dbt_command(
        'run --select stg_teradata__transaction int_unified_transaction int_transaction_enriched'
    );
