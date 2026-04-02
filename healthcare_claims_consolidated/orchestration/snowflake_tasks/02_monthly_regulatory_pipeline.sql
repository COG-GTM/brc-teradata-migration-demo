-- =============================================================================
-- Snowflake Task DAG: Monthly Regulatory Pipeline
-- Migrated from: teradata/bteq/monthly_regulatory_report.bteq
--
-- Original BTEQ orchestration:
--   CALL BARCLAYS_DWH.sp_regulatory_capital_calc(...)
--   CALL BARCLAYS_DWH.sp_monthly_pnl_rollup(...)
--   .IF ERRORCODE <> 0 THEN .GOTO ERROR_EXIT
--
-- Replaced with Snowflake Task with monthly schedule.
-- =============================================================================

-- Root task: triggers the monthly regulatory run
CREATE OR REPLACE TASK healthcare_consolidated.tasks.monthly_regulatory_run
    WAREHOUSE = 'COMPUTE_WH'
    SCHEDULE = 'USING CRON 0 2 1 * * UTC'  -- 2 AM UTC, 1st of each month
    COMMENT = 'Monthly regulatory pipeline. Migrated from BTEQ monthly_regulatory_report.bteq'
AS
    CALL SYSTEM$SEND_NOTIFICATION(
        'healthcare_pipeline_alerts',
        'Monthly Regulatory Pipeline Started',
        'Month-end regulatory pipeline starting at ' || CURRENT_TIMESTAMP()
    );


-- Task 1: Run regulatory capital calculations
CREATE OR REPLACE TASK healthcare_consolidated.tasks.regulatory_capital_calc
    WAREHOUSE = 'COMPUTE_WH'
    AFTER healthcare_consolidated.tasks.monthly_regulatory_run
    COMMENT = 'Run Basel III regulatory capital calculations. Replaces sp_regulatory_capital_calc.'
AS
    CALL healthcare_consolidated.procedures.run_dbt_command(
        'run --select fct_regulatory_capital --full-refresh'
    );


-- Task 2: Run monthly P&L rollup
CREATE OR REPLACE TASK healthcare_consolidated.tasks.monthly_pnl_rollup
    WAREHOUSE = 'COMPUTE_WH'
    AFTER healthcare_consolidated.tasks.monthly_regulatory_run
    COMMENT = 'Run monthly P&L rollup. Replaces sp_monthly_pnl_rollup.'
AS
    CALL healthcare_consolidated.procedures.run_dbt_command(
        'run --select fct_monthly_pnl --full-refresh'
    );


-- Task 3: Run regulatory validation tests
CREATE OR REPLACE TASK healthcare_consolidated.tasks.regulatory_validation
    WAREHOUSE = 'COMPUTE_WH'
    AFTER healthcare_consolidated.tasks.regulatory_capital_calc,
         healthcare_consolidated.tasks.monthly_pnl_rollup
    COMMENT = 'Validate regulatory outputs. Replaces BTEQ validation queries.'
AS
    CALL healthcare_consolidated.procedures.run_dbt_command(
        'test --select tag:risk tag:finance'
    );


-- Task 4: Completion notification
CREATE OR REPLACE TASK healthcare_consolidated.tasks.regulatory_complete
    WAREHOUSE = 'COMPUTE_WH'
    AFTER healthcare_consolidated.tasks.regulatory_validation
    COMMENT = 'Monthly regulatory pipeline completion notification.'
AS
    CALL SYSTEM$SEND_NOTIFICATION(
        'healthcare_pipeline_alerts',
        'Monthly Regulatory Pipeline Complete',
        'Month-end regulatory pipeline completed at ' || CURRENT_TIMESTAMP()
    );


-- Enable monthly tasks
ALTER TASK healthcare_consolidated.tasks.regulatory_complete RESUME;
ALTER TASK healthcare_consolidated.tasks.regulatory_validation RESUME;
ALTER TASK healthcare_consolidated.tasks.monthly_pnl_rollup RESUME;
ALTER TASK healthcare_consolidated.tasks.regulatory_capital_calc RESUME;
ALTER TASK healthcare_consolidated.tasks.monthly_regulatory_run RESUME;
