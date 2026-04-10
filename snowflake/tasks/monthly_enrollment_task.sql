/*******************************************************************************
 * Healthcare Claims - Monthly Enrollment Task
 *
 * Scheduled task that runs on the 1st of each month to generate
 * the previous month's member enrollment data in MART_MEMBER_MONTHS.
 *
 * Snowflake-specific features:
 *   - CRON schedule expression
 *   - SYSTEM$SEND_EMAIL for notifications
 *   - Snowflake date functions for month calculation
 ******************************************************************************/

USE DATABASE CLAIMS_DW;
USE SCHEMA MART;

-- =============================================================================
-- Monthly enrollment generation task
-- Runs at 4:00 AM UTC on the 1st of each month
-- =============================================================================
CREATE OR REPLACE TASK CLAIMS_DW.MART.TASK_MONTHLY_ENROLLMENT
    WAREHOUSE = CLAIMS_ETL_WH
    SCHEDULE  = 'USING CRON 0 4 1 * * UTC'
    COMMENT   = 'Monthly task: generates member enrollment for the previous month'
AS
BEGIN
    -- Calculate previous month boundaries
    LET v_prev_month_start VARCHAR := TO_CHAR(
        DATEADD('month', -1, DATE_TRUNC('month', CURRENT_DATE())),
        'YYYY-MM'
    );
    LET v_prev_month_end VARCHAR := v_prev_month_start;

    -- Log start
    INSERT INTO CLAIMS_DW.RAW.ETL_EXECUTION_LOG
        (procedure_name, claim_category, status, message, execution_timestamp)
    SELECT 'TASK_MONTHLY_ENROLLMENT', 'ENROLLMENT', 'STARTED',
           'Monthly enrollment generation started for ' || :v_prev_month_start,
           CURRENT_TIMESTAMP();

    -- Call the enrollment procedure for the previous month
    CALL CLAIMS_DW.MART.SP_MEMBER_MONTH_ENROLLMENT(
        :v_prev_month_start,
        :v_prev_month_end
    );

    -- Log completion
    INSERT INTO CLAIMS_DW.RAW.ETL_EXECUTION_LOG
        (procedure_name, claim_category, status, message, execution_timestamp)
    SELECT 'TASK_MONTHLY_ENROLLMENT', 'ENROLLMENT', 'SUCCESS',
           'Monthly enrollment generation completed for ' || :v_prev_month_start,
           CURRENT_TIMESTAMP();

    -- Send notification
    CALL SYSTEM$SEND_EMAIL(
        'claims_pipeline_notifications',
        'claims-etl-alerts@example.com',
        'CLAIMS PIPELINE: Monthly Enrollment Completed',
        'Monthly enrollment generation for ' || :v_prev_month_start ||
        ' completed successfully at ' || CURRENT_TIMESTAMP()::VARCHAR
    );

EXCEPTION
    WHEN OTHER THEN
        INSERT INTO CLAIMS_DW.RAW.ETL_EXECUTION_LOG
            (procedure_name, claim_category, status, message, execution_timestamp)
        SELECT 'TASK_MONTHLY_ENROLLMENT', 'ENROLLMENT', 'FAILED',
               'Error: ' || SQLERRM, CURRENT_TIMESTAMP();

        CALL SYSTEM$SEND_EMAIL(
            'claims_pipeline_notifications',
            'claims-etl-alerts@example.com',
            'CLAIMS PIPELINE ERROR: Monthly Enrollment Failed',
            'Error in TASK_MONTHLY_ENROLLMENT: ' || SQLERRM
        );
END;


-- =============================================================================
-- Backfill task: generate enrollment for a historical date range
-- This is a one-time task, not scheduled
-- =============================================================================
CREATE OR REPLACE TASK CLAIMS_DW.MART.TASK_ENROLLMENT_BACKFILL
    WAREHOUSE = CLAIMS_ETL_WH
    -- No SCHEDULE - manually triggered via EXECUTE TASK
    COMMENT   = 'One-time backfill: generates member enrollment for historical months'
    -- This task is only triggered manually
    ALLOW_OVERLAPPING_EXECUTION = FALSE
AS
BEGIN
    -- Backfill 24 months of enrollment data
    LET v_start_month VARCHAR := TO_CHAR(
        DATEADD('month', -24, DATE_TRUNC('month', CURRENT_DATE())),
        'YYYY-MM'
    );
    LET v_end_month VARCHAR := TO_CHAR(
        DATEADD('month', -1, DATE_TRUNC('month', CURRENT_DATE())),
        'YYYY-MM'
    );

    INSERT INTO CLAIMS_DW.RAW.ETL_EXECUTION_LOG
        (procedure_name, claim_category, status, message, execution_timestamp)
    SELECT 'TASK_ENROLLMENT_BACKFILL', 'ENROLLMENT', 'STARTED',
           'Backfill started for ' || :v_start_month || ' to ' || :v_end_month,
           CURRENT_TIMESTAMP();

    CALL CLAIMS_DW.MART.SP_MEMBER_MONTH_ENROLLMENT(:v_start_month, :v_end_month);

    INSERT INTO CLAIMS_DW.RAW.ETL_EXECUTION_LOG
        (procedure_name, claim_category, status, message, execution_timestamp)
    SELECT 'TASK_ENROLLMENT_BACKFILL', 'ENROLLMENT', 'SUCCESS',
           'Backfill completed for ' || :v_start_month || ' to ' || :v_end_month,
           CURRENT_TIMESTAMP();

EXCEPTION
    WHEN OTHER THEN
        INSERT INTO CLAIMS_DW.RAW.ETL_EXECUTION_LOG
            (procedure_name, claim_category, status, message, execution_timestamp)
        SELECT 'TASK_ENROLLMENT_BACKFILL', 'ENROLLMENT', 'FAILED',
               'Error: ' || SQLERRM, CURRENT_TIMESTAMP();
END;


-- =============================================================================
-- Resume tasks (created in suspended state by default)
-- =============================================================================
-- ALTER TASK CLAIMS_DW.MART.TASK_MONTHLY_ENROLLMENT RESUME;
-- EXECUTE TASK CLAIMS_DW.MART.TASK_ENROLLMENT_BACKFILL;  -- manual trigger
