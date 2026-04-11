-- =============================================================================
-- Snowflake Task DAG: Daily Batch Pipeline
-- Migrated from: teradata/bteq/daily_batch_load.bteq
--
-- Original BTEQ orchestration:
--   .LOGON ${TERADATA_HOST}/${TERADATA_USER},${TERADATA_PASSWORD}
--   .IF ERRORCODE <> 0 THEN .GOTO ERROR_EXIT
--   CALL BARCLAYS_DWH.sp_daily_transaction_load(...)
--   .IF ERRORCODE <> 0 THEN .GOTO ERROR_EXIT
--   CALL BARCLAYS_DWH.sp_customer_risk_scoring(...)
--   CALL BARCLAYS_DWH.sp_aml_screening(...)
--
-- Replaced with Snowflake Task DAG with proper error handling.
-- dbt orchestration handles the transformation; Tasks handle scheduling.
-- =============================================================================

-- Root task: triggers the daily dbt run
CREATE OR REPLACE TASK healthcare_consolidated.tasks.daily_dbt_run
    WAREHOUSE = 'COMPUTE_WH'
    SCHEDULE = 'USING CRON 0 6 * * * UTC'  -- 6 AM UTC daily
    COMMENT = 'Daily dbt run for healthcare claims consolidated pipeline. Migrated from BTEQ daily_batch_load.bteq'
AS
    CALL SYSTEM$SEND_NOTIFICATION(
        'healthcare_pipeline_alerts',
        'Daily Pipeline Started',
        'Healthcare consolidated daily pipeline starting at ' || CURRENT_TIMESTAMP()
    );


-- Task 1: Run dbt seed (load reference data)
CREATE OR REPLACE TASK healthcare_consolidated.tasks.dbt_seed
    WAREHOUSE = 'COMPUTE_WH'
    AFTER healthcare_consolidated.tasks.daily_dbt_run
    COMMENT = 'Load/refresh seed data (reference tables). Replaces BTEQ flat file imports.'
AS
    -- In production, this would call a Snowflake stored procedure that invokes dbt
    -- via Snowflake's External Functions or a Lambda/Azure Function integration
    CALL healthcare_consolidated.procedures.run_dbt_command('seed --full-refresh');


-- Task 2: Run dbt staging models
CREATE OR REPLACE TASK healthcare_consolidated.tasks.dbt_staging
    WAREHOUSE = 'COMPUTE_WH'
    AFTER healthcare_consolidated.tasks.dbt_seed
    COMMENT = 'Run staging models for all three platforms. Replaces Teradata staging views with LOCK ROW FOR ACCESS.'
AS
    CALL healthcare_consolidated.procedures.run_dbt_command('run --select tag:staging');


-- Task 3: Run dbt intermediate models
CREATE OR REPLACE TASK healthcare_consolidated.tasks.dbt_intermediate
    WAREHOUSE = 'COMPUTE_WH'
    AFTER healthcare_consolidated.tasks.dbt_staging
    COMMENT = 'Run intermediate unified models. Replaces cross-platform dedup and enrichment logic.'
AS
    CALL healthcare_consolidated.procedures.run_dbt_command('run --select tag:intermediate');


-- Task 4: Run dbt mart models (finance)
CREATE OR REPLACE TASK healthcare_consolidated.tasks.dbt_marts_finance
    WAREHOUSE = 'COMPUTE_WH'
    AFTER healthcare_consolidated.tasks.dbt_intermediate
    COMMENT = 'Run finance mart models. Replaces sp_daily_transaction_load and sp_monthly_pnl_rollup.'
AS
    CALL healthcare_consolidated.procedures.run_dbt_command('run --select tag:finance');


-- Task 5: Run dbt mart models (risk) - can run in parallel with finance
CREATE OR REPLACE TASK healthcare_consolidated.tasks.dbt_marts_risk
    WAREHOUSE = 'COMPUTE_WH'
    AFTER healthcare_consolidated.tasks.dbt_intermediate
    COMMENT = 'Run risk mart models. Replaces sp_customer_risk_scoring and sp_regulatory_capital_calc.'
AS
    CALL healthcare_consolidated.procedures.run_dbt_command('run --select tag:risk');


-- Task 6: Run dbt mart models (compliance) - can run in parallel with finance/risk
CREATE OR REPLACE TASK healthcare_consolidated.tasks.dbt_marts_compliance
    WAREHOUSE = 'COMPUTE_WH'
    AFTER healthcare_consolidated.tasks.dbt_intermediate
    COMMENT = 'Run compliance mart models. Replaces sp_aml_screening.'
AS
    CALL healthcare_consolidated.procedures.run_dbt_command('run --select tag:compliance');


-- Task 7: Run dbt tests (after all marts complete)
CREATE OR REPLACE TASK healthcare_consolidated.tasks.dbt_test
    WAREHOUSE = 'COMPUTE_WH'
    AFTER healthcare_consolidated.tasks.dbt_marts_finance,
         healthcare_consolidated.tasks.dbt_marts_risk,
         healthcare_consolidated.tasks.dbt_marts_compliance
    COMMENT = 'Run full dbt test suite including Tuva DQ tests. Replaces BTEQ validation queries.'
AS
    CALL healthcare_consolidated.procedures.run_dbt_command('test');


-- Task 8: Run dbt snapshots (after tests pass)
CREATE OR REPLACE TASK healthcare_consolidated.tasks.dbt_snapshot
    WAREHOUSE = 'COMPUTE_WH'
    AFTER healthcare_consolidated.tasks.dbt_test
    COMMENT = 'Run SCD Type 2 snapshots. Replaces Teradata cursor-based SCD2 logic in sp_daily_transaction_load.'
AS
    CALL healthcare_consolidated.procedures.run_dbt_command('snapshot');


-- Task 9: Pipeline completion notification
CREATE OR REPLACE TASK healthcare_consolidated.tasks.pipeline_complete
    WAREHOUSE = 'COMPUTE_WH'
    AFTER healthcare_consolidated.tasks.dbt_snapshot
    COMMENT = 'Send completion notification. Replaces BTEQ .LOGOFF and status reporting.'
AS
    CALL SYSTEM$SEND_NOTIFICATION(
        'healthcare_pipeline_alerts',
        'Daily Pipeline Complete',
        'Healthcare consolidated daily pipeline completed successfully at ' || CURRENT_TIMESTAMP()
    );


-- Error handling task (replaces BTEQ .GOTO ERROR_EXIT pattern)
CREATE OR REPLACE TASK healthcare_consolidated.tasks.error_handler
    WAREHOUSE = 'COMPUTE_WH'
    COMMENT = 'Error handler for failed tasks. Replaces BTEQ .IF ERRORCODE / .GOTO ERROR_EXIT.'
    -- This task is triggered by task failure via SYSTEM$TASK_RUNTIME_INFO
AS
    CALL healthcare_consolidated.procedures.handle_pipeline_error(
        SYSTEM$TASK_RUNTIME_INFO('CURRENT_ROOT_TASK_NAME'),
        SYSTEM$TASK_RUNTIME_INFO('CURRENT_TASK_NAME')
    );


-- Enable all tasks (run in reverse dependency order)
ALTER TASK healthcare_consolidated.tasks.pipeline_complete RESUME;
ALTER TASK healthcare_consolidated.tasks.dbt_snapshot RESUME;
ALTER TASK healthcare_consolidated.tasks.dbt_test RESUME;
ALTER TASK healthcare_consolidated.tasks.dbt_marts_compliance RESUME;
ALTER TASK healthcare_consolidated.tasks.dbt_marts_risk RESUME;
ALTER TASK healthcare_consolidated.tasks.dbt_marts_finance RESUME;
ALTER TASK healthcare_consolidated.tasks.dbt_intermediate RESUME;
ALTER TASK healthcare_consolidated.tasks.dbt_staging RESUME;
ALTER TASK healthcare_consolidated.tasks.dbt_seed RESUME;
ALTER TASK healthcare_consolidated.tasks.daily_dbt_run RESUME;
