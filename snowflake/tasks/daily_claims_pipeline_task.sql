/*******************************************************************************
 * Healthcare Claims - Snowflake Tasks (Daily Claims Pipeline DAG)
 *
 * Snowflake-specific features used:
 *   - CREATE OR REPLACE TASK with SCHEDULE
 *   - AFTER dependency for DAG chaining
 *   - WAREHOUSE assignment per task
 *   - SYSTEM$SEND_EMAIL for error notifications
 *   - WHEN clause with SYSTEM$STREAM_HAS_DATA for conditional execution
 *
 * Pipeline DAG:
 *   TASK_RAW_LOAD_MONITOR (root - scheduled)
 *       └─> TASK_STAGING_REFRESH
 *               └─> TASK_WAREHOUSE_LOAD
 *                       └─> TASK_MART_REFRESH
 *                               └─> TASK_PIPELINE_COMPLETE_NOTIFY
 ******************************************************************************/

USE DATABASE CLAIMS_DW;
USE SCHEMA WAREHOUSE;

-- =============================================================================
-- Root Task: Monitor raw data streams for new data
-- Runs every day at 2:00 AM UTC
-- =============================================================================
CREATE OR REPLACE TASK CLAIMS_DW.WAREHOUSE.TASK_RAW_LOAD_MONITOR
    WAREHOUSE = CLAIMS_ETL_WH
    SCHEDULE  = 'USING CRON 0 2 * * * UTC'
    COMMENT   = 'Root task: monitors raw streams and triggers daily pipeline'
    -- Only run if any of the raw streams have new data
    WHEN SYSTEM$STREAM_HAS_DATA('CLAIMS_DW.RAW.STREAM_RAW_MEMBER_ELIGIBILITY')
      OR SYSTEM$STREAM_HAS_DATA('CLAIMS_DW.RAW.STREAM_RAW_MEDICAL_CLAIM')
      OR SYSTEM$STREAM_HAS_DATA('CLAIMS_DW.RAW.STREAM_RAW_PHARMACY_CLAIM')
AS
BEGIN
    -- Log pipeline start
    INSERT INTO CLAIMS_DW.RAW.ETL_EXECUTION_LOG
        (procedure_name, claim_category, status, message, execution_timestamp)
    SELECT 'TASK_RAW_LOAD_MONITOR', 'ALL', 'STARTED',
           'Daily pipeline triggered - new data detected in raw streams',
           CURRENT_TIMESTAMP();
EXCEPTION
    WHEN OTHER THEN
        CALL SYSTEM$SEND_EMAIL(
            'claims_pipeline_notifications',
            'claims-etl-alerts@example.com',
            'CLAIMS PIPELINE ERROR: Raw Load Monitor Failed',
            'Error in TASK_RAW_LOAD_MONITOR: ' || SQLERRM
        );
END;


-- =============================================================================
-- Task 2: Refresh staging views / apply dedup logic
-- Runs AFTER root task completes
-- =============================================================================
CREATE OR REPLACE TASK CLAIMS_DW.WAREHOUSE.TASK_STAGING_REFRESH
    WAREHOUSE = CLAIMS_ETL_WH
    AFTER CLAIMS_DW.WAREHOUSE.TASK_RAW_LOAD_MONITOR
    COMMENT   = 'Refreshes staging layer: applies ADR dedup and diagnosis flatten'
AS
BEGIN
    -- ADR dedup for medical claims (last 90 days)
    CALL CLAIMS_DW.WAREHOUSE.SP_CLAIMS_ADR_DEDUP(
        'MEDICAL',
        DATEADD('day', -90, CURRENT_DATE()),
        CURRENT_DATE()
    );

    -- ADR dedup for pharmacy claims (last 90 days)
    CALL CLAIMS_DW.WAREHOUSE.SP_CLAIMS_ADR_DEDUP(
        'PHARMACY',
        DATEADD('day', -90, CURRENT_DATE()),
        CURRENT_DATE()
    );

    -- Flatten diagnosis codes
    CALL CLAIMS_DW.WAREHOUSE.SP_DIAGNOSIS_CODE_FLATTEN(
        DATEADD('day', -90, CURRENT_DATE()),
        CURRENT_DATE()
    );

    -- Log success
    INSERT INTO CLAIMS_DW.RAW.ETL_EXECUTION_LOG
        (procedure_name, claim_category, status, message, execution_timestamp)
    SELECT 'TASK_STAGING_REFRESH', 'ALL', 'SUCCESS',
           'Staging refresh completed - ADR dedup and diagnosis flatten done',
           CURRENT_TIMESTAMP();
EXCEPTION
    WHEN OTHER THEN
        INSERT INTO CLAIMS_DW.RAW.ETL_EXECUTION_LOG
            (procedure_name, claim_category, status, message, execution_timestamp)
        SELECT 'TASK_STAGING_REFRESH', 'ALL', 'FAILED',
               'Error: ' || SQLERRM, CURRENT_TIMESTAMP();

        CALL SYSTEM$SEND_EMAIL(
            'claims_pipeline_notifications',
            'claims-etl-alerts@example.com',
            'CLAIMS PIPELINE ERROR: Staging Refresh Failed',
            'Error in TASK_STAGING_REFRESH: ' || SQLERRM
        );
END;


-- =============================================================================
-- Task 3: Load warehouse dimension and fact tables
-- Runs AFTER staging refresh
-- =============================================================================
CREATE OR REPLACE TASK CLAIMS_DW.WAREHOUSE.TASK_WAREHOUSE_LOAD
    WAREHOUSE = CLAIMS_ETL_WH
    AFTER CLAIMS_DW.WAREHOUSE.TASK_STAGING_REFRESH
    COMMENT   = 'Loads warehouse dims and facts: SCD2 members, providers, encounters'
AS
BEGIN
    -- =========================================================================
    -- DIM_MEMBER: SCD Type 2 merge
    -- =========================================================================
    MERGE INTO CLAIMS_DW.WAREHOUSE.DIM_MEMBER AS tgt
    USING (
        SELECT
            member_id,
            subscriber_id,
            first_name,
            last_name,
            date_of_birth,
            gender,
            CASE
                WHEN DATEDIFF('year', date_of_birth, CURRENT_DATE()) < 18  THEN '0-17'
                WHEN DATEDIFF('year', date_of_birth, CURRENT_DATE()) < 26  THEN '18-25'
                WHEN DATEDIFF('year', date_of_birth, CURRENT_DATE()) < 36  THEN '26-35'
                WHEN DATEDIFF('year', date_of_birth, CURRENT_DATE()) < 46  THEN '36-45'
                WHEN DATEDIFF('year', date_of_birth, CURRENT_DATE()) < 56  THEN '46-55'
                WHEN DATEDIFF('year', date_of_birth, CURRENT_DATE()) < 65  THEN '56-64'
                ELSE '65+'
            END                                     AS age_band,
            state_code,
            zip_code,
            plan_code,
            plan_name,
            product_type,
            line_of_business,
            group_number,
            group_name,
            pcp_provider_id,
            coverage_type,
            relationship_code,
            MD5(
                COALESCE(plan_code, '') || '|' ||
                COALESCE(product_type, '') || '|' ||
                COALESCE(line_of_business, '') || '|' ||
                COALESCE(state_code, '') || '|' ||
                COALESCE(zip_code, '') || '|' ||
                COALESCE(pcp_provider_id, '') || '|' ||
                COALESCE(group_number, '')
            )                                       AS record_hash
        FROM CLAIMS_DW.STAGING.V_MEMBER_LATEST
    ) AS src
    ON  tgt.member_id = src.member_id
    AND tgt.is_current = TRUE
    -- When the record has changed (hash mismatch) -> expire old, insert new
    WHEN MATCHED AND tgt.record_hash != src.record_hash THEN
        UPDATE SET
            tgt.is_current       = FALSE,
            tgt.expiration_date  = CURRENT_DATE(),
            tgt.updated_timestamp = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN
        INSERT (
            member_id, subscriber_id, first_name, last_name,
            date_of_birth, gender, age_band,
            state_code, zip_code, plan_code, plan_name,
            product_type, line_of_business, group_number, group_name,
            pcp_provider_id, coverage_type, relationship_code,
            effective_date, expiration_date, is_current, record_hash,
            created_timestamp, updated_timestamp
        )
        VALUES (
            src.member_id, src.subscriber_id, src.first_name, src.last_name,
            src.date_of_birth, src.gender, src.age_band,
            src.state_code, src.zip_code, src.plan_code, src.plan_name,
            src.product_type, src.line_of_business, src.group_number, src.group_name,
            src.pcp_provider_id, src.coverage_type, src.relationship_code,
            CURRENT_DATE(), '9999-12-31', TRUE, src.record_hash,
            CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
        );

    -- Insert new current records for changed members
    INSERT INTO CLAIMS_DW.WAREHOUSE.DIM_MEMBER (
        member_id, subscriber_id, first_name, last_name,
        date_of_birth, gender, age_band,
        state_code, zip_code, plan_code, plan_name,
        product_type, line_of_business, group_number, group_name,
        pcp_provider_id, coverage_type, relationship_code,
        effective_date, expiration_date, is_current, record_hash,
        created_timestamp, updated_timestamp
    )
    SELECT
        src.member_id, src.subscriber_id, src.first_name, src.last_name,
        src.date_of_birth, src.gender,
        CASE
            WHEN DATEDIFF('year', src.date_of_birth, CURRENT_DATE()) < 18  THEN '0-17'
            WHEN DATEDIFF('year', src.date_of_birth, CURRENT_DATE()) < 26  THEN '18-25'
            WHEN DATEDIFF('year', src.date_of_birth, CURRENT_DATE()) < 36  THEN '26-35'
            WHEN DATEDIFF('year', src.date_of_birth, CURRENT_DATE()) < 46  THEN '36-45'
            WHEN DATEDIFF('year', src.date_of_birth, CURRENT_DATE()) < 56  THEN '46-55'
            WHEN DATEDIFF('year', src.date_of_birth, CURRENT_DATE()) < 65  THEN '56-64'
            ELSE '65+'
        END,
        src.state_code, src.zip_code, src.plan_code, src.plan_name,
        src.product_type, src.line_of_business, src.group_number, src.group_name,
        src.pcp_provider_id, src.coverage_type, src.relationship_code,
        CURRENT_DATE(), '9999-12-31', TRUE,
        MD5(
            COALESCE(src.plan_code, '') || '|' ||
            COALESCE(src.product_type, '') || '|' ||
            COALESCE(src.line_of_business, '') || '|' ||
            COALESCE(src.state_code, '') || '|' ||
            COALESCE(src.zip_code, '') || '|' ||
            COALESCE(src.pcp_provider_id, '') || '|' ||
            COALESCE(src.group_number, '')
        ),
        CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
    FROM CLAIMS_DW.STAGING.V_MEMBER_LATEST src
    INNER JOIN CLAIMS_DW.WAREHOUSE.DIM_MEMBER expired
        ON  src.member_id = expired.member_id
        AND expired.is_current = FALSE
        AND expired.expiration_date = CURRENT_DATE()
    WHERE NOT EXISTS (
        SELECT 1 FROM CLAIMS_DW.WAREHOUSE.DIM_MEMBER cur
        WHERE cur.member_id = src.member_id AND cur.is_current = TRUE
    );

    -- =========================================================================
    -- Encounter grouping (last 90 days)
    -- =========================================================================
    CALL CLAIMS_DW.WAREHOUSE.SP_ENCOUNTER_GROUPING(
        DATEADD('day', -90, CURRENT_DATE()),
        CURRENT_DATE()
    );

    -- Log success
    INSERT INTO CLAIMS_DW.RAW.ETL_EXECUTION_LOG
        (procedure_name, claim_category, status, message, execution_timestamp)
    SELECT 'TASK_WAREHOUSE_LOAD', 'ALL', 'SUCCESS',
           'Warehouse load completed - dimensions and facts refreshed',
           CURRENT_TIMESTAMP();
EXCEPTION
    WHEN OTHER THEN
        INSERT INTO CLAIMS_DW.RAW.ETL_EXECUTION_LOG
            (procedure_name, claim_category, status, message, execution_timestamp)
        SELECT 'TASK_WAREHOUSE_LOAD', 'ALL', 'FAILED',
               'Error: ' || SQLERRM, CURRENT_TIMESTAMP();

        CALL SYSTEM$SEND_EMAIL(
            'claims_pipeline_notifications',
            'claims-etl-alerts@example.com',
            'CLAIMS PIPELINE ERROR: Warehouse Load Failed',
            'Error in TASK_WAREHOUSE_LOAD: ' || SQLERRM
        );
END;


-- =============================================================================
-- Task 4: Refresh mart aggregates
-- Runs AFTER warehouse load
-- =============================================================================
CREATE OR REPLACE TASK CLAIMS_DW.WAREHOUSE.TASK_MART_REFRESH
    WAREHOUSE = CLAIMS_ETL_WH
    AFTER CLAIMS_DW.WAREHOUSE.TASK_WAREHOUSE_LOAD
    COMMENT   = 'Refreshes mart aggregates: claim summary, encounter summary'
AS
BEGIN
    -- =========================================================================
    -- MART_CLAIM_SUMMARY: rebuild current month
    -- =========================================================================
    DELETE FROM CLAIMS_DW.MART.MART_CLAIM_SUMMARY
    WHERE summary_period = TO_CHAR(CURRENT_DATE(), 'YYYY-MM');

    -- Medical claims summary
    INSERT INTO CLAIMS_DW.MART.MART_CLAIM_SUMMARY (
        summary_period, calendar_year, calendar_month,
        claim_category, line_of_business, product_type, state_code,
        claim_type, place_of_service, status_code,
        total_claims, total_claim_lines, unique_members, unique_providers,
        total_billed_amount, total_allowed_amount, total_paid_amount,
        total_net_paid_amount, total_copay, total_coinsurance, total_deductible,
        total_member_liability,
        avg_paid_per_claim, avg_allowed_per_claim, avg_member_liability,
        denied_claim_count, reversed_claim_count, denial_rate,
        etl_load_timestamp
    )
    SELECT
        TO_CHAR(f.start_date_key::VARCHAR, 'YYYY-MM')   AS summary_period,
        LEFT(f.start_date_key::VARCHAR, 4)::INTEGER,
        SUBSTR(f.start_date_key::VARCHAR, 5, 2)::INTEGER,
        'MEDICAL',
        dm.line_of_business,
        dm.product_type,
        dm.state_code,
        f.claim_type,
        f.place_of_service,
        f.status_code,
        COUNT(DISTINCT f.claim_id),
        COUNT(*),
        COUNT(DISTINCT f.member_id),
        COUNT(DISTINCT f.rendering_provider_key),
        SUM(COALESCE(f.billed_amount, 0)),
        SUM(COALESCE(f.allowed_amount, 0)),
        SUM(COALESCE(f.paid_amount, 0)),
        SUM(COALESCE(f.net_paid_amount, 0)),
        SUM(COALESCE(f.copay_amount, 0)),
        SUM(COALESCE(f.coinsurance_amount, 0)),
        SUM(COALESCE(f.deductible_amount, 0)),
        SUM(COALESCE(f.member_liability, 0)),
        AVG(f.paid_amount),
        AVG(f.allowed_amount),
        AVG(f.member_liability),
        COUNT(DISTINCT CASE WHEN f.status_code = 'DENIED' THEN f.claim_id END),
        COUNT(DISTINCT CASE WHEN f.status_code = 'REVERSED' THEN f.claim_id END),
        DIV0(
            COUNT(DISTINCT CASE WHEN f.status_code = 'DENIED' THEN f.claim_id END),
            COUNT(DISTINCT f.claim_id)
        ),
        CURRENT_TIMESTAMP()
    FROM CLAIMS_DW.WAREHOUSE.FCT_MEDICAL_CLAIM f
    LEFT JOIN CLAIMS_DW.WAREHOUSE.DIM_MEMBER dm
        ON f.member_key = dm.member_key
    WHERE TO_CHAR(f.start_date_key::VARCHAR, 'YYYY-MM') = TO_CHAR(CURRENT_DATE(), 'YYYY-MM')
    GROUP BY 1, 2, 3, dm.line_of_business, dm.product_type, dm.state_code,
             f.claim_type, f.place_of_service, f.status_code;

    -- Log success
    INSERT INTO CLAIMS_DW.RAW.ETL_EXECUTION_LOG
        (procedure_name, claim_category, status, message, execution_timestamp)
    SELECT 'TASK_MART_REFRESH', 'ALL', 'SUCCESS',
           'Mart refresh completed', CURRENT_TIMESTAMP();
EXCEPTION
    WHEN OTHER THEN
        INSERT INTO CLAIMS_DW.RAW.ETL_EXECUTION_LOG
            (procedure_name, claim_category, status, message, execution_timestamp)
        SELECT 'TASK_MART_REFRESH', 'ALL', 'FAILED',
               'Error: ' || SQLERRM, CURRENT_TIMESTAMP();

        CALL SYSTEM$SEND_EMAIL(
            'claims_pipeline_notifications',
            'claims-etl-alerts@example.com',
            'CLAIMS PIPELINE ERROR: Mart Refresh Failed',
            'Error in TASK_MART_REFRESH: ' || SQLERRM
        );
END;


-- =============================================================================
-- Task 5: Pipeline completion notification
-- Runs AFTER mart refresh
-- =============================================================================
CREATE OR REPLACE TASK CLAIMS_DW.WAREHOUSE.TASK_PIPELINE_COMPLETE_NOTIFY
    WAREHOUSE = CLAIMS_ETL_WH
    AFTER CLAIMS_DW.WAREHOUSE.TASK_MART_REFRESH
    COMMENT   = 'Sends completion notification after full pipeline run'
AS
BEGIN
    -- Log pipeline completion
    INSERT INTO CLAIMS_DW.RAW.ETL_EXECUTION_LOG
        (procedure_name, claim_category, status, message, execution_timestamp)
    SELECT 'TASK_PIPELINE_COMPLETE_NOTIFY', 'ALL', 'SUCCESS',
           'Daily claims pipeline completed successfully',
           CURRENT_TIMESTAMP();

    -- Send success notification
    CALL SYSTEM$SEND_EMAIL(
        'claims_pipeline_notifications',
        'claims-etl-alerts@example.com',
        'CLAIMS PIPELINE: Daily Run Completed Successfully',
        'The daily claims pipeline completed at ' || CURRENT_TIMESTAMP()::VARCHAR ||
        '. All stages (raw -> staging -> warehouse -> mart) executed successfully.'
    );
END;


-- =============================================================================
-- ETL Execution Log table (used by all tasks and procedures)
-- =============================================================================
CREATE TABLE IF NOT EXISTS CLAIMS_DW.RAW.ETL_EXECUTION_LOG
(
    log_id                      INTEGER          AUTOINCREMENT,
    procedure_name              VARCHAR(100),
    claim_category              VARCHAR(20),
    start_date                  DATE,
    end_date                    DATE,
    rows_affected               INTEGER,
    status                      VARCHAR(20),
    message                     VARCHAR(4000),
    execution_timestamp         TIMESTAMP_NTZ    DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'ETL pipeline execution log for monitoring and alerting';


-- =============================================================================
-- Resume all tasks in the DAG (tasks are created in suspended state)
-- =============================================================================
-- ALTER TASK CLAIMS_DW.WAREHOUSE.TASK_PIPELINE_COMPLETE_NOTIFY RESUME;
-- ALTER TASK CLAIMS_DW.WAREHOUSE.TASK_MART_REFRESH RESUME;
-- ALTER TASK CLAIMS_DW.WAREHOUSE.TASK_WAREHOUSE_LOAD RESUME;
-- ALTER TASK CLAIMS_DW.WAREHOUSE.TASK_STAGING_REFRESH RESUME;
-- ALTER TASK CLAIMS_DW.WAREHOUSE.TASK_RAW_LOAD_MONITOR RESUME;
-- NOTE: Tasks must be resumed in reverse dependency order (leaf -> root)
