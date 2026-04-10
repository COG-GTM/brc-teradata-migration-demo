/*******************************************************************************
 * sp_member_month_enrollment
 *
 * Calculates member-months of enrollment by expanding date ranges into
 * individual monthly records. Used for PMPM (Per Member Per Month)
 * calculations and utilization reporting.
 *
 * Uses Teradata EXPAND ON for date range expansion - a Teradata-specific
 * temporal feature that generates rows for each period within a range.
 *
 * Also uses Teradata date arithmetic where (date - date) = integer days.
 *
 * Teradata-specific features:
 *   - EXPAND ON for temporal expansion
 *   - PERIOD data type
 *   - Teradata date arithmetic (date - date = integer days)
 *   - CREATE VOLATILE TABLE
 *   - QUALIFY ROW_NUMBER()
 *   - ACTIVITY_COUNT
 ******************************************************************************/

REPLACE PROCEDURE CLAIMS_MART.sp_member_month_enrollment (
    IN p_year  SMALLINT,
    IN p_month SMALLINT  /* 0 = all months in year */
)
BEGIN
    -- Local variable declarations
    DECLARE v_batch_id       BIGINT;
    DECLARE v_start_date     DATE;
    DECLARE v_end_date       DATE;
    DECLARE v_row_count      INTEGER;
    DECLARE v_sqlstate       CHAR(5);

    -- Error handler
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        SET v_sqlstate = SQLSTATE;
        INSERT INTO CLAIMS_DWH.ETL_ERROR_LOG (
            procedure_name, error_code, error_state, error_ts, batch_id
        ) VALUES (
            'sp_member_month_enrollment', SQLCODE, v_sqlstate, CURRENT_TIMESTAMP, v_batch_id
        );
    END;

    -- Generate batch ID
    SET v_batch_id = CAST(
        CAST(p_year AS CHAR(4)) || CASE WHEN p_month = 0 THEN '00' ELSE CAST(p_month AS FORMAT '99') END || '040'
        AS BIGINT
    );

    -- Calculate date range
    IF p_month = 0 THEN
        SET v_start_date = CAST(CAST(p_year AS CHAR(4)) || '-01-01' AS DATE FORMAT 'YYYY-MM-DD');
        SET v_end_date   = CAST(CAST(p_year AS CHAR(4)) || '-12-31' AS DATE FORMAT 'YYYY-MM-DD');
    ELSE
        SET v_start_date = CAST(
            CAST(p_year AS CHAR(4)) || '-' || CAST(p_month AS FORMAT '99') || '-01'
            AS DATE FORMAT 'YYYY-MM-DD'
        );
        /* Last day of the month using Teradata date arithmetic */
        SET v_end_date = ADD_MONTHS(v_start_date, 1) - 1;
    END IF;

    ---------------------------------------------------------------------------
    -- STEP 1: Expand enrollment periods into monthly records
    -- Uses EXPAND ON to generate one row per month of enrollment
    ---------------------------------------------------------------------------
    CREATE VOLATILE TABLE VT_MEMBER_MONTHS_EXPANDED AS (
        SELECT
            mem.member_id,
            mem.plan_id,
            mem.payer_id,
            mem.line_of_business,
            mem.gender,
            mem.date_of_birth,
            mem.state,
            mem.group_id,
            mem.zip_code,
            /* EXPAND ON generates BEGIN and END of each period interval */
            BEGIN(expanded_period) AS month_start_date,
            END(expanded_period) - 1 AS month_end_date,
            expanded_period,
            /* Year-month key for joining */
            EXTRACT(YEAR FROM BEGIN(expanded_period)) * 100
                + EXTRACT(MONTH FROM BEGIN(expanded_period)) AS year_month,
            EXTRACT(YEAR FROM BEGIN(expanded_period))  AS year_number,
            EXTRACT(MONTH FROM BEGIN(expanded_period)) AS month_number
        FROM (
            SELECT
                member_id,
                plan_id,
                payer_id,
                line_of_business,
                gender,
                date_of_birth,
                state,
                group_id,
                zip_code,
                PERIOD(
                    /* Clip enrollment to our target range */
                    CASE
                        WHEN enrollment_start_date < v_start_date THEN v_start_date
                        ELSE enrollment_start_date
                    END,
                    CASE
                        WHEN enrollment_end_date IS NULL THEN v_end_date + 1
                        WHEN enrollment_end_date > v_end_date THEN v_end_date + 1
                        ELSE enrollment_end_date + 1
                    END
                ) AS enrollment_period
            FROM CLAIMS_RAW.RAW_MEMBER_ELIGIBILITY
            WHERE enrollment_start_date <= v_end_date
              AND (enrollment_end_date IS NULL OR enrollment_end_date >= v_start_date)
        ) elig
        EXPAND ON enrollment_period AS expanded_period BY ANCHOR MONTH_BEGIN
    ) WITH DATA
    PRIMARY INDEX (member_id, year_month)
    ON COMMIT PRESERVE ROWS;

    SET v_row_count = ACTIVITY_COUNT;

    ---------------------------------------------------------------------------
    -- STEP 2: Calculate enrollment days and age for each member-month
    ---------------------------------------------------------------------------
    CREATE VOLATILE TABLE VT_MEMBER_MONTHS_ENRICHED AS (
        SELECT
            mm.member_id,
            mm.year_month,
            mm.year_number,
            mm.month_number,
            mm.plan_id,
            mm.payer_id,
            mm.line_of_business,
            mm.gender,
            /* Age at the start of the month using Teradata date arithmetic */
            (mm.month_start_date - mm.date_of_birth) / 365 AS age_at_month,
            CASE
                WHEN (mm.month_start_date - mm.date_of_birth) / 365 < 18  THEN '0-17'
                WHEN (mm.month_start_date - mm.date_of_birth) / 365 < 26  THEN '18-25'
                WHEN (mm.month_start_date - mm.date_of_birth) / 365 < 35  THEN '26-34'
                WHEN (mm.month_start_date - mm.date_of_birth) / 365 < 45  THEN '35-44'
                WHEN (mm.month_start_date - mm.date_of_birth) / 365 < 55  THEN '45-54'
                WHEN (mm.month_start_date - mm.date_of_birth) / 365 < 65  THEN '55-64'
                ELSE '65+'
            END AS age_band,
            mm.state,
            mm.group_id,
            /* Calculate enrollment days in this month */
            (mm.month_end_date - mm.month_start_date) + 1 AS enrollment_days_in_month,
            /* Determine if full month enrollment */
            CASE
                WHEN mm.month_start_date = CAST(
                    CAST(mm.year_number AS CHAR(4)) || '-'
                    || CAST(mm.month_number AS FORMAT '99') || '-01'
                    AS DATE FORMAT 'YYYY-MM-DD')
                AND mm.month_end_date = ADD_MONTHS(
                    CAST(CAST(mm.year_number AS CHAR(4)) || '-'
                    || CAST(mm.month_number AS FORMAT '99') || '-01'
                    AS DATE FORMAT 'YYYY-MM-DD'), 1) - 1
                    THEN 'Y'
                ELSE 'N'
            END AS is_full_month
        FROM VT_MEMBER_MONTHS_EXPANDED mm
        /* Deduplicate in case of overlapping enrollment spans */
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY mm.member_id, mm.year_month
            ORDER BY mm.month_start_date ASC
        ) = 1
    ) WITH DATA
    PRIMARY INDEX (member_id, year_month)
    ON COMMIT PRESERVE ROWS;

    ---------------------------------------------------------------------------
    -- STEP 3: Attach claim utilization and costs per member-month
    ---------------------------------------------------------------------------
    DELETE FROM CLAIMS_MART.MART_MEMBER_MONTHS
    WHERE year_month BETWEEN
        CAST(CAST(p_year AS CHAR(4)) || CASE WHEN p_month = 0 THEN '01' ELSE CAST(p_month AS FORMAT '99') END AS INTEGER)
    AND
        CAST(CAST(p_year AS CHAR(4)) || CASE WHEN p_month = 0 THEN '12' ELSE CAST(p_month AS FORMAT '99') END AS INTEGER);

    INSERT INTO CLAIMS_MART.MART_MEMBER_MONTHS (
        member_id, year_month, year_number, month_number,
        plan_id, payer_id, line_of_business, gender,
        age_at_month, age_band, state, group_id,
        enrollment_days_in_month, is_full_month,
        medical_claim_count, pharmacy_claim_count,
        medical_paid_amount, pharmacy_paid_amount, total_paid_amount,
        medical_allowed_amount, pharmacy_allowed_amount, total_allowed_amount,
        member_oop_amount,
        etl_batch_id, etl_loaded_ts
    )
    SELECT
        mme.member_id,
        mme.year_month,
        mme.year_number,
        mme.month_number,
        mme.plan_id,
        mme.payer_id,
        mme.line_of_business,
        mme.gender,
        mme.age_at_month,
        mme.age_band,
        mme.state,
        mme.group_id,
        mme.enrollment_days_in_month,
        mme.is_full_month,
        ZEROIFNULL(mc.medical_claim_count),
        ZEROIFNULL(rx.pharmacy_claim_count),
        ZEROIFNULL(mc.medical_paid_amount),
        ZEROIFNULL(rx.pharmacy_paid_amount),
        ZEROIFNULL(mc.medical_paid_amount) + ZEROIFNULL(rx.pharmacy_paid_amount),
        ZEROIFNULL(mc.medical_allowed_amount),
        ZEROIFNULL(rx.pharmacy_allowed_amount),
        ZEROIFNULL(mc.medical_allowed_amount) + ZEROIFNULL(rx.pharmacy_allowed_amount),
        ZEROIFNULL(mc.medical_member_oop) + ZEROIFNULL(rx.pharmacy_member_oop),
        v_batch_id,
        CURRENT_TIMESTAMP(6)
    FROM VT_MEMBER_MONTHS_ENRICHED mme
    /* Medical claims summary per member-month */
    LEFT JOIN (
        SELECT
            dm.member_id,
            CAST(CAST(dd.calendar_date AS FORMAT 'YYYYMM') AS INTEGER) AS year_month,
            COUNT(DISTINCT fc.claim_id) AS medical_claim_count,
            SUM(ZEROIFNULL(fc.paid_amount)) AS medical_paid_amount,
            SUM(ZEROIFNULL(fc.allowed_amount)) AS medical_allowed_amount,
            SUM(ZEROIFNULL(fc.coinsurance) + ZEROIFNULL(fc.copay)
                + ZEROIFNULL(fc.deductible)) AS medical_member_oop
        FROM CLAIMS_DWH.FCT_MEDICAL_CLAIM fc
        INNER JOIN CLAIMS_DWH.DIM_MEMBER dm
            ON fc.member_sk = dm.member_sk AND dm.is_current = 'Y'
        INNER JOIN CLAIMS_DWH.DIM_DATE dd
            ON fc.service_date_from_key = dd.date_key
        WHERE dd.calendar_date BETWEEN v_start_date AND v_end_date
        GROUP BY dm.member_id,
            CAST(CAST(dd.calendar_date AS FORMAT 'YYYYMM') AS INTEGER)
    ) mc ON mme.member_id = mc.member_id AND mme.year_month = mc.year_month
    /* Pharmacy claims summary per member-month */
    LEFT JOIN (
        SELECT
            dm.member_id,
            CAST(CAST(dd.calendar_date AS FORMAT 'YYYYMM') AS INTEGER) AS year_month,
            COUNT(DISTINCT fp.claim_id) AS pharmacy_claim_count,
            SUM(ZEROIFNULL(fp.paid_amount)) AS pharmacy_paid_amount,
            SUM(ZEROIFNULL(fp.allowed_amount)) AS pharmacy_allowed_amount,
            SUM(ZEROIFNULL(fp.copay) + ZEROIFNULL(fp.coinsurance)
                + ZEROIFNULL(fp.deductible)) AS pharmacy_member_oop
        FROM CLAIMS_DWH.FCT_PHARMACY_CLAIM fp
        INNER JOIN CLAIMS_DWH.DIM_MEMBER dm
            ON fp.member_sk = dm.member_sk AND dm.is_current = 'Y'
        INNER JOIN CLAIMS_DWH.DIM_DATE dd
            ON fp.dispensing_date_key = dd.date_key
        WHERE dd.calendar_date BETWEEN v_start_date AND v_end_date
        GROUP BY dm.member_id,
            CAST(CAST(dd.calendar_date AS FORMAT 'YYYYMM') AS INTEGER)
    ) rx ON mme.member_id = rx.member_id AND mme.year_month = rx.year_month;

    SET v_row_count = ACTIVITY_COUNT;

    ---------------------------------------------------------------------------
    -- STEP 4: Collect statistics and clean up
    ---------------------------------------------------------------------------
    COLLECT STATISTICS ON CLAIMS_MART.MART_MEMBER_MONTHS
        COLUMN (member_id, year_month);
    COLLECT STATISTICS ON CLAIMS_MART.MART_MEMBER_MONTHS
        COLUMN (year_month);
    COLLECT STATISTICS ON CLAIMS_MART.MART_MEMBER_MONTHS
        COLUMN (payer_id);

    DROP TABLE VT_MEMBER_MONTHS_ENRICHED;
    DROP TABLE VT_MEMBER_MONTHS_EXPANDED;

END;
