/*******************************************************************************
 * Phase 7: Monthly Enrollment Pipeline
 *
 * Replaces:
 *   - teradata/claims/bteq/monthly_enrollment_refresh.bteq
 *   - databricks/jobs/monthly_enrollment_job.json
 *   - snowflake/tasks/monthly_enrollment_task.sql
 *
 * Runs on the 1st of each month to refresh member enrollment data
 * and calculate member month metrics.
 ******************************************************************************/

USE DATABASE CLAIMS_DW;
USE SCHEMA ORCHESTRATION;

-- =============================================================================
-- Monthly Enrollment Task
-- =============================================================================
CREATE OR REPLACE TASK CLAIMS_DW.ORCHESTRATION.TASK_MONTHLY_ENROLLMENT
    WAREHOUSE = CLAIMS_WH
    SCHEDULE = 'USING CRON 0 2 1 * * America/New_York'  -- 1st of month at 2 AM ET
    COMMENT = 'Monthly enrollment refresh - replaces 3 platform-specific enrollment jobs'
AS
BEGIN
    -- Step 1: Refresh eligibility staging
    CALL CLAIMS_DW.ORCHESTRATION.SP_RUN_DBT_MODELS('tag:eligibility');

    -- Step 2: Refresh member month enrollment
    CALL CLAIMS_DW.ORCHESTRATION.SP_RUN_DBT_MODELS('tag:member_month');

    -- Step 3: Refresh PMPM mart
    CALL CLAIMS_DW.ORCHESTRATION.SP_RUN_DBT_MODELS('mart_member_months');

    -- Log completion
    INSERT INTO CLAIMS_DW.ORCHESTRATION.PIPELINE_EXECUTION_LOG
        (task_name, selector, status, started_at, completed_at)
    VALUES
        ('TASK_MONTHLY_ENROLLMENT', 'monthly', 'COMPLETED',
         CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP());
END;


-- =============================================================================
-- Quarterly Quality Measures Task
-- Replaces: teradata/claims/bteq/quarterly_quality_measures.bteq
-- =============================================================================
CREATE OR REPLACE TASK CLAIMS_DW.ORCHESTRATION.TASK_QUARTERLY_QUALITY
    WAREHOUSE = CLAIMS_WH
    SCHEDULE = 'USING CRON 0 3 1 1,4,7,10 * America/New_York'  -- Quarterly
    COMMENT = 'Quarterly HEDIS quality measure refresh'
AS
BEGIN
    CALL CLAIMS_DW.ORCHESTRATION.SP_RUN_DBT_MODELS('tag:quality_measures');

    INSERT INTO CLAIMS_DW.ORCHESTRATION.PIPELINE_EXECUTION_LOG
        (task_name, selector, status, started_at, completed_at)
    VALUES
        ('TASK_QUARTERLY_QUALITY', 'quarterly', 'COMPLETED',
         CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP());
END;
