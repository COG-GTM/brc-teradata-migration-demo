/*******************************************************************************
 * Healthcare Claims - Monthly Member Enrollment Generation
 *
 * Generates one row per member per month of enrollment for the
 * MART_MEMBER_MONTHS table. Uses Snowflake date functions and
 * GENERATOR to create the monthly spine.
 *
 * This procedure:
 *   1. Generates a calendar spine for the requested date range
 *   2. Joins members to months where they were eligible
 *   3. Attaches utilization and cost summaries per member-month
 *   4. Calculates PMPM (Per Member Per Month) metrics
 ******************************************************************************/

CREATE OR REPLACE PROCEDURE CLAIMS_DW.MART.SP_MEMBER_MONTH_ENROLLMENT(
    P_START_YEAR_MONTH   VARCHAR,    -- 'YYYY-MM' format
    P_END_YEAR_MONTH     VARCHAR     -- 'YYYY-MM' format
)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
COMMENT = 'Generates monthly member enrollment with utilization and cost summaries'
AS
$$
DECLARE
    v_rows_affected INTEGER DEFAULT 0;
    v_start_date    DATE;
    v_end_date      DATE;
    v_result        VARCHAR DEFAULT '';
BEGIN

    -- =========================================================================
    -- Step 0: Parse input parameters
    -- =========================================================================
    v_start_date := TO_DATE(:P_START_YEAR_MONTH || '-01', 'YYYY-MM-DD');
    v_end_date   := LAST_DAY(TO_DATE(:P_END_YEAR_MONTH || '-01', 'YYYY-MM-DD'));

    -- =========================================================================
    -- Step 1: Generate monthly calendar spine using GENERATOR
    -- =========================================================================
    CREATE OR REPLACE TEMPORARY TABLE CLAIMS_DW.MART.TMP_MONTH_SPINE AS
    SELECT
        TO_CHAR(month_start, 'YYYY-MM')                AS year_month,
        YEAR(month_start)                               AS calendar_year,
        MONTH(month_start)                              AS calendar_month,
        month_start,
        LAST_DAY(month_start)                           AS month_end,
        DATEDIFF('day', month_start, LAST_DAY(month_start)) + 1 AS days_in_month
    FROM (
        SELECT DATEADD('month', seq, :v_start_date)     AS month_start
        FROM (
            SELECT SEQ4() AS seq
            FROM TABLE(GENERATOR(ROWCOUNT => 1200))     -- up to 100 years
        )
        WHERE DATEADD('month', seq, :v_start_date) <= :v_end_date
    );

    -- =========================================================================
    -- Step 2: Cross-join members with months where they were eligible
    -- =========================================================================
    CREATE OR REPLACE TEMPORARY TABLE CLAIMS_DW.MART.TMP_MEMBER_MONTHS AS
    SELECT
        m.member_id,
        dm.member_key,
        ms.year_month,
        ms.calendar_year,
        ms.calendar_month,
        ms.month_start,
        ms.month_end,
        ms.days_in_month,
        -- Member attributes as of that month
        DATEDIFF('year', m.date_of_birth, ms.month_start)   AS age_as_of_month,
        CASE
            WHEN DATEDIFF('year', m.date_of_birth, ms.month_start) < 18  THEN '0-17'
            WHEN DATEDIFF('year', m.date_of_birth, ms.month_start) < 26  THEN '18-25'
            WHEN DATEDIFF('year', m.date_of_birth, ms.month_start) < 36  THEN '26-35'
            WHEN DATEDIFF('year', m.date_of_birth, ms.month_start) < 46  THEN '36-45'
            WHEN DATEDIFF('year', m.date_of_birth, ms.month_start) < 56  THEN '46-55'
            WHEN DATEDIFF('year', m.date_of_birth, ms.month_start) < 65  THEN '56-64'
            ELSE '65+'
        END                                                  AS age_band,
        m.gender,
        m.state_code,
        m.zip_code,
        m.plan_code,
        m.product_type,
        m.line_of_business,
        m.group_number,
        m.pcp_provider_id,
        m.coverage_type,
        -- Days enrolled in the month (partial month handling)
        DATEDIFF('day',
            GREATEST(m.eligibility_start_date, ms.month_start),
            LEAST(COALESCE(m.eligibility_end_date, ms.month_end), ms.month_end)
        ) + 1                                                AS days_enrolled_in_month
    FROM CLAIMS_DW.STAGING.V_MEMBER_LATEST m
    INNER JOIN CLAIMS_DW.MART.TMP_MONTH_SPINE ms
        ON  m.eligibility_start_date <= ms.month_end
        AND COALESCE(m.eligibility_end_date, '9999-12-31') >= ms.month_start
    LEFT JOIN CLAIMS_DW.WAREHOUSE.DIM_MEMBER dm
        ON  m.member_id = dm.member_id
        AND dm.is_current = TRUE;

    -- =========================================================================
    -- Step 3: Summarize medical utilization and cost per member-month
    -- =========================================================================
    CREATE OR REPLACE TEMPORARY TABLE CLAIMS_DW.MART.TMP_MEDICAL_SUMMARY AS
    SELECT
        mc.member_id,
        TO_CHAR(mc.start_date, 'YYYY-MM')               AS year_month,
        COUNT(DISTINCT mc.claim_id)                      AS medical_claim_count,
        SUM(COALESCE(mc.paid_amount, 0))                 AS total_medical_paid,
        SUM(COALESCE(mc.allowed_amount, 0))              AS total_medical_allowed,
        COUNT(DISTINCT CASE WHEN mc.claim_type = 'I' AND mc.place_of_service = '21'
              THEN mc.claim_id END)                      AS inpatient_admission_count,
        COUNT(DISTINCT CASE WHEN mc.place_of_service = '23'
              THEN mc.claim_id END)                      AS ed_visit_count,
        COUNT(DISTINCT CASE WHEN mc.place_of_service = '11'
              THEN mc.claim_id END)                      AS office_visit_count
    FROM CLAIMS_DW.STAGING.V_MEDICAL_CLAIM_CURRENT mc
    WHERE mc.start_date BETWEEN :v_start_date AND :v_end_date
      AND UPPER(mc.status_code) IN ('PAID', 'ADJUSTED')
    GROUP BY mc.member_id, TO_CHAR(mc.start_date, 'YYYY-MM');

    -- =========================================================================
    -- Step 4: Summarize pharmacy utilization and cost per member-month
    -- =========================================================================
    CREATE OR REPLACE TEMPORARY TABLE CLAIMS_DW.MART.TMP_PHARMACY_SUMMARY AS
    SELECT
        pc.member_id,
        TO_CHAR(pc.fill_date, 'YYYY-MM')                AS year_month,
        COUNT(DISTINCT pc.claim_id)                      AS pharmacy_claim_count,
        SUM(COALESCE(pc.paid_amount, 0))                 AS total_pharmacy_paid,
        SUM(COALESCE(pc.allowed_amount, 0))              AS total_pharmacy_allowed
    FROM CLAIMS_DW.STAGING.V_PHARMACY_CLAIM_CURRENT pc
    WHERE pc.fill_date BETWEEN :v_start_date AND :v_end_date
      AND UPPER(pc.status_code) IN ('PAID', 'ADJUSTED')
    GROUP BY pc.member_id, TO_CHAR(pc.fill_date, 'YYYY-MM');

    -- =========================================================================
    -- Step 5: Summarize encounters per member-month
    -- =========================================================================
    CREATE OR REPLACE TEMPORARY TABLE CLAIMS_DW.MART.TMP_ENCOUNTER_SUMMARY AS
    SELECT
        e.member_id,
        TO_CHAR(e.encounter_start_date, 'YYYY-MM')      AS year_month,
        COUNT(DISTINCT e.encounter_id)                   AS encounter_count
    FROM CLAIMS_DW.WAREHOUSE.FCT_ENCOUNTER e
    WHERE e.encounter_start_date BETWEEN :v_start_date AND :v_end_date
    GROUP BY e.member_id, TO_CHAR(e.encounter_start_date, 'YYYY-MM');

    -- =========================================================================
    -- Step 6: Merge everything into MART_MEMBER_MONTHS
    -- =========================================================================
    DELETE FROM CLAIMS_DW.MART.MART_MEMBER_MONTHS
    WHERE year_month BETWEEN :P_START_YEAR_MONTH AND :P_END_YEAR_MONTH;

    INSERT INTO CLAIMS_DW.MART.MART_MEMBER_MONTHS (
        member_id, member_key, year_month, calendar_year, calendar_month,
        age_as_of_month, age_band, gender, state_code, zip_code,
        plan_code, product_type, line_of_business, group_number,
        pcp_provider_id, coverage_type,
        is_enrolled, member_month_count, days_enrolled_in_month,
        medical_claim_count, pharmacy_claim_count, encounter_count,
        inpatient_admission_count, ed_visit_count, office_visit_count,
        total_medical_paid, total_medical_allowed,
        total_pharmacy_paid, total_pharmacy_allowed,
        total_paid, total_allowed, total_member_liability,
        medical_pmpm, pharmacy_pmpm, total_pmpm,
        etl_load_timestamp
    )
    SELECT
        mm.member_id,
        mm.member_key,
        mm.year_month,
        mm.calendar_year,
        mm.calendar_month,
        mm.age_as_of_month,
        mm.age_band,
        mm.gender,
        mm.state_code,
        mm.zip_code,
        mm.plan_code,
        mm.product_type,
        mm.line_of_business,
        mm.group_number,
        mm.pcp_provider_id,
        mm.coverage_type,
        TRUE                                             AS is_enrolled,
        1                                                AS member_month_count,
        mm.days_enrolled_in_month,
        COALESCE(med.medical_claim_count, 0),
        COALESCE(rx.pharmacy_claim_count, 0),
        COALESCE(enc.encounter_count, 0),
        COALESCE(med.inpatient_admission_count, 0),
        COALESCE(med.ed_visit_count, 0),
        COALESCE(med.office_visit_count, 0),
        COALESCE(med.total_medical_paid, 0),
        COALESCE(med.total_medical_allowed, 0),
        COALESCE(rx.total_pharmacy_paid, 0),
        COALESCE(rx.total_pharmacy_allowed, 0),
        COALESCE(med.total_medical_paid, 0) + COALESCE(rx.total_pharmacy_paid, 0),
        COALESCE(med.total_medical_allowed, 0) + COALESCE(rx.total_pharmacy_allowed, 0),
        0                                                AS total_member_liability,
        -- PMPM = total cost / 1 (single member-month)
        COALESCE(med.total_medical_paid, 0)              AS medical_pmpm,
        COALESCE(rx.total_pharmacy_paid, 0)              AS pharmacy_pmpm,
        COALESCE(med.total_medical_paid, 0) + COALESCE(rx.total_pharmacy_paid, 0) AS total_pmpm,
        CURRENT_TIMESTAMP()
    FROM CLAIMS_DW.MART.TMP_MEMBER_MONTHS mm
    LEFT JOIN CLAIMS_DW.MART.TMP_MEDICAL_SUMMARY med
        ON  mm.member_id = med.member_id
        AND mm.year_month = med.year_month
    LEFT JOIN CLAIMS_DW.MART.TMP_PHARMACY_SUMMARY rx
        ON  mm.member_id = rx.member_id
        AND mm.year_month = rx.year_month
    LEFT JOIN CLAIMS_DW.MART.TMP_ENCOUNTER_SUMMARY enc
        ON  mm.member_id = enc.member_id
        AND mm.year_month = enc.year_month;

    LET v_rows_affected := SQLROWCOUNT;

    -- Clean up temp tables
    DROP TABLE IF EXISTS CLAIMS_DW.MART.TMP_MONTH_SPINE;
    DROP TABLE IF EXISTS CLAIMS_DW.MART.TMP_MEMBER_MONTHS;
    DROP TABLE IF EXISTS CLAIMS_DW.MART.TMP_MEDICAL_SUMMARY;
    DROP TABLE IF EXISTS CLAIMS_DW.MART.TMP_PHARMACY_SUMMARY;
    DROP TABLE IF EXISTS CLAIMS_DW.MART.TMP_ENCOUNTER_SUMMARY;

    v_result := 'Member month enrollment completed for ' || :P_START_YEAR_MONTH
             || ' to ' || :P_END_YEAR_MONTH
             || '. Rows inserted: ' || v_rows_affected::VARCHAR;
    RETURN v_result;

EXCEPTION
    WHEN OTHER THEN
        DROP TABLE IF EXISTS CLAIMS_DW.MART.TMP_MONTH_SPINE;
        DROP TABLE IF EXISTS CLAIMS_DW.MART.TMP_MEMBER_MONTHS;
        DROP TABLE IF EXISTS CLAIMS_DW.MART.TMP_MEDICAL_SUMMARY;
        DROP TABLE IF EXISTS CLAIMS_DW.MART.TMP_PHARMACY_SUMMARY;
        DROP TABLE IF EXISTS CLAIMS_DW.MART.TMP_ENCOUNTER_SUMMARY;
        RETURN 'ERROR in SP_MEMBER_MONTH_ENROLLMENT: ' || SQLERRM;
END;
$$;
