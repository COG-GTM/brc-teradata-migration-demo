-- =============================================================================
-- Helper Stored Procedures for Task Orchestration
-- These procedures wrap dbt CLI invocations and error handling.
-- =============================================================================

-- Procedure to run dbt commands via external function
-- In production, this would call an AWS Lambda / Azure Function
-- that executes dbt commands against the Snowflake target.
CREATE OR REPLACE PROCEDURE healthcare_consolidated.procedures.run_dbt_command(
    dbt_command VARCHAR
)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
BEGIN
    -- Log the command execution
    INSERT INTO healthcare_consolidated.audit.pipeline_log (
        log_timestamp,
        task_name,
        dbt_command,
        status
    ) VALUES (
        CURRENT_TIMESTAMP(),
        SYSTEM$TASK_RUNTIME_INFO('CURRENT_TASK_NAME'),
        dbt_command,
        'STARTED'
    );

    -- In production, invoke external function:
    -- SELECT healthcare_consolidated.functions.execute_dbt(:dbt_command);

    -- Update log with completion
    INSERT INTO healthcare_consolidated.audit.pipeline_log (
        log_timestamp,
        task_name,
        dbt_command,
        status
    ) VALUES (
        CURRENT_TIMESTAMP(),
        SYSTEM$TASK_RUNTIME_INFO('CURRENT_TASK_NAME'),
        dbt_command,
        'COMPLETED'
    );

    RETURN 'SUCCESS: ' || dbt_command;
END;
$$;


-- Error handling procedure
-- Replaces BTEQ .GOTO ERROR_EXIT pattern
CREATE OR REPLACE PROCEDURE healthcare_consolidated.procedures.handle_pipeline_error(
    root_task_name VARCHAR,
    failed_task_name VARCHAR
)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
BEGIN
    -- Log the error
    INSERT INTO healthcare_consolidated.audit.pipeline_log (
        log_timestamp,
        task_name,
        dbt_command,
        status,
        error_message
    ) VALUES (
        CURRENT_TIMESTAMP(),
        failed_task_name,
        NULL,
        'FAILED',
        'Task ' || failed_task_name || ' failed in pipeline ' || root_task_name
    );

    -- Send alert notification
    CALL SYSTEM$SEND_NOTIFICATION(
        'healthcare_pipeline_alerts',
        'PIPELINE FAILURE: ' || root_task_name,
        'Task ' || failed_task_name || ' failed at ' || CURRENT_TIMESTAMP()
    );

    RETURN 'ERROR_HANDLED: ' || failed_task_name;
END;
$$;


-- Audit log table
CREATE TABLE IF NOT EXISTS healthcare_consolidated.audit.pipeline_log (
    log_id INTEGER AUTOINCREMENT,
    log_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    task_name VARCHAR(256),
    dbt_command VARCHAR(1024),
    status VARCHAR(50),
    error_message VARCHAR(4096),
    PRIMARY KEY (log_id)
);
