-- ============================================================================
-- Databricks Healthcare Claims - Monthly Enrollment Processing Procedure
-- ============================================================================
-- Processes member enrollment data on a monthly basis to generate and
-- maintain the mart_member_months table (one row per member per month).
--
-- This procedure:
--   1. Reads eligibility coverage periods from staging
--   2. Expands each period into individual monthly rows
--   3. Deduplicates overlapping coverage months
--   4. Calculates age bands and derived enrollment attributes
--   5. Writes to the mart_member_months table
--
-- Usage:
--   CALL claims_warehouse.sp_monthly_enrollment('2025-01');
--   CALL claims_warehouse.sp_monthly_enrollment();  -- processes current month
--
-- Platform: Databricks SQL
-- ============================================================================

CREATE OR REPLACE PROCEDURE claims_warehouse.sp_monthly_enrollment(
    IN target_month STRING DEFAULT DATE_FORMAT(CURRENT_DATE(), 'yyyy-MM')
)
LANGUAGE SQL
COMMENT 'Monthly enrollment processing: generates member month rows from eligibility periods'
AS
BEGIN
    -- -----------------------------------------------------------------------
    -- Variables
    -- -----------------------------------------------------------------------
    DECLARE v_start_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP();
    DECLARE v_month_start DATE;
    DECLARE v_month_end DATE;
    DECLARE v_members_processed BIGINT;
    DECLARE v_member_months_generated BIGINT;

    -- Parse target month
    SET v_month_start = TO_DATE(CONCAT(target_month, '-01'), 'yyyy-MM-dd');
    SET v_month_end = LAST_DAY(v_month_start);

    SELECT CONCAT('Monthly enrollment processing started for: ', target_month,
                  ' (', CAST(v_month_start AS STRING), ' to ',
                  CAST(v_month_end AS STRING), ')');

    -- -----------------------------------------------------------------------
    -- Step 1: Generate member months from eligibility coverage periods
    -- For the target month, find all members who were enrolled (coverage
    -- period overlaps with the target month).
    -- -----------------------------------------------------------------------

    -- Create temporary view of eligible members for the target month
    CREATE OR REPLACE TEMPORARY VIEW v_eligible_members AS
    SELECT DISTINCT
        e.member_id,
        e.plan_id,
        e.plan_type,
        e.line_of_business,
        e.group_id,
        e.enrollment_status
    FROM claims_raw.raw_member_eligibility e
    WHERE e.coverage_start_date <= v_month_end
      AND COALESCE(e.coverage_end_date, DATE '2099-12-31') >= v_month_start
      AND UPPER(TRIM(e.enrollment_status)) IN ('ACTIVE', 'COBRA');

    SELECT COUNT(*) INTO v_members_processed FROM v_eligible_members;

    SELECT CONCAT('Step 1: Found ', CAST(v_members_processed AS STRING),
                  ' eligible members for ', target_month);

    -- -----------------------------------------------------------------------
    -- Step 2: Build member month records with demographics
    -- Join eligible members with staged (PHI-masked) member data
    -- -----------------------------------------------------------------------

    CREATE OR REPLACE TEMPORARY VIEW v_member_month_records AS
    SELECT
        e.member_id,
        -- Try to get surrogate key from dim_member
        d.member_key,
        v_month_start AS enrollment_month,
        YEAR(v_month_start) AS enrollment_year,
        MONTH(v_month_start) AS enrollment_month_number,
        DATE_FORMAT(v_month_start, 'yyyy-MM') AS enrollment_year_month,
        e.plan_id,
        e.plan_type,
        e.line_of_business,
        e.group_id,
        m.gender,
        -- Calculate age at the target month using masked DOB (year only)
        FLOOR(MONTHS_BETWEEN(v_month_start, m.date_of_birth_masked) / 12) AS age_at_month,
        m.state_code,
        m.zip_code_3digit,
        m.risk_score,
        m.pcp_provider_id,
        e.enrollment_status,
        TRUE AS is_enrolled,
        CURRENT_TIMESTAMP() AS created_timestamp
    FROM v_eligible_members e
    INNER JOIN claims_staging.stg_member_latest m
        ON e.member_id = m.member_id
    LEFT JOIN claims_warehouse.dim_member d
        ON e.member_id = d.member_id AND d.is_current = TRUE;

    -- -----------------------------------------------------------------------
    -- Step 3: Add age bands
    -- -----------------------------------------------------------------------

    CREATE OR REPLACE TEMPORARY VIEW v_member_month_with_age_band AS
    SELECT
        r.*,
        CASE
            WHEN r.age_at_month < 18 THEN '0-17'
            WHEN r.age_at_month < 26 THEN '18-25'
            WHEN r.age_at_month < 35 THEN '26-34'
            WHEN r.age_at_month < 45 THEN '35-44'
            WHEN r.age_at_month < 55 THEN '45-54'
            WHEN r.age_at_month < 65 THEN '55-64'
            ELSE '65+'
        END AS age_band
    FROM v_member_month_records r;

    -- -----------------------------------------------------------------------
    -- Step 4: Merge into mart_member_months
    -- Use MERGE to handle both new inserts and updates to existing months
    -- -----------------------------------------------------------------------

    MERGE INTO claims_mart.mart_member_months AS target
    USING v_member_month_with_age_band AS source
    ON target.member_id = source.member_id
       AND target.enrollment_month = source.enrollment_month
    WHEN MATCHED THEN
        UPDATE SET
            target.member_key = source.member_key,
            target.plan_id = source.plan_id,
            target.plan_type = source.plan_type,
            target.line_of_business = source.line_of_business,
            target.group_id = source.group_id,
            target.gender = source.gender,
            target.age_at_month = source.age_at_month,
            target.age_band = source.age_band,
            target.state_code = source.state_code,
            target.zip_code_3digit = source.zip_code_3digit,
            target.risk_score = source.risk_score,
            target.pcp_provider_id = source.pcp_provider_id,
            target.enrollment_status = source.enrollment_status,
            target.is_enrolled = source.is_enrolled,
            target.created_timestamp = source.created_timestamp
    WHEN NOT MATCHED THEN
        INSERT (
            member_id, member_key, enrollment_month, enrollment_year,
            enrollment_month_number, enrollment_year_month, plan_id, plan_type,
            line_of_business, group_id, gender, age_at_month, age_band,
            state_code, zip_code_3digit, risk_score, pcp_provider_id,
            enrollment_status, is_enrolled, created_timestamp
        )
        VALUES (
            source.member_id, source.member_key, source.enrollment_month,
            source.enrollment_year, source.enrollment_month_number,
            source.enrollment_year_month, source.plan_id, source.plan_type,
            source.line_of_business, source.group_id, source.gender,
            source.age_at_month, source.age_band, source.state_code,
            source.zip_code_3digit, source.risk_score, source.pcp_provider_id,
            source.enrollment_status, source.is_enrolled, source.created_timestamp
        );

    SELECT COUNT(*) INTO v_member_months_generated
    FROM claims_mart.mart_member_months
    WHERE enrollment_month = v_month_start;

    -- -----------------------------------------------------------------------
    -- Step 5: Generate enrollment summary statistics
    -- -----------------------------------------------------------------------

    SELECT CONCAT('Enrollment summary for ', target_month, ':');

    -- By line of business
    SELECT
        line_of_business,
        COUNT(*) AS member_count,
        COUNT(DISTINCT member_id) AS unique_members
    FROM claims_mart.mart_member_months
    WHERE enrollment_month = v_month_start
    GROUP BY line_of_business
    ORDER BY member_count DESC;

    -- By age band
    SELECT
        age_band,
        COUNT(*) AS member_count
    FROM claims_mart.mart_member_months
    WHERE enrollment_month = v_month_start
    GROUP BY age_band
    ORDER BY age_band;

    -- -----------------------------------------------------------------------
    -- Step 6: Optimize the mart table partition
    -- -----------------------------------------------------------------------

    OPTIMIZE claims_mart.mart_member_months
    WHERE enrollment_month = v_month_start
    ZORDER BY (member_id);

    -- -----------------------------------------------------------------------
    -- Complete
    -- -----------------------------------------------------------------------

    SELECT CONCAT(
        'Monthly enrollment processing complete. ',
        'Month: ', target_month, '. ',
        'Members processed: ', CAST(v_members_processed AS STRING), '. ',
        'Member months generated: ', CAST(v_member_months_generated AS STRING), '. ',
        'Duration: ', CAST(
            TIMESTAMPDIFF(SECOND, v_start_ts, CURRENT_TIMESTAMP())
        AS STRING), ' seconds.'
    );
END;
