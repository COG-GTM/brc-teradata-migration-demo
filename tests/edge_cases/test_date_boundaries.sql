/******************************************************************************
 * test_date_boundaries.sql
 *
 * Tests date boundary handling in the migrated pipeline.
 *
 * Key migration considerations:
 *   - Teradata date arithmetic: date - date = integer days
 *   - Snowflake equivalent: DATEDIFF('day', date1, date2)
 *   - Year boundary transitions (Dec 31 -> Jan 1)
 *   - Leap year handling (Feb 29)
 *   - Month-end date calculations
 *   - SCD2 effective_from/effective_to boundaries
 *   - Date key format: YYYYMMDD integer
 ******************************************************************************/

-- =============================================================================
-- TEST 1: Year boundary — transactions spanning Dec 31 -> Jan 1
-- Verify batch_id generation and date_key are correct across year boundaries
-- =============================================================================
WITH year_boundary_txns AS (
    SELECT
        date_key,
        TO_DATE(TO_CHAR(date_key), 'YYYYMMDD') AS business_date,
        YEAR(TO_DATE(TO_CHAR(date_key), 'YYYYMMDD')) AS txn_year,
        COUNT(*) AS txn_count
    FROM barclays_dwh.fct_transaction
    WHERE date_key IN (
        -- Last day of each year and first day of next year
        TO_NUMBER(TO_CHAR(DATE_TRUNC('year', CURRENT_DATE()) - 1, 'YYYYMMDD')),   -- Dec 31
        TO_NUMBER(TO_CHAR(DATE_TRUNC('year', CURRENT_DATE()), 'YYYYMMDD'))         -- Jan 1
    )
    GROUP BY date_key
)

SELECT
    'YEAR_BOUNDARY_TRANSACTIONS' AS test_name,
    COUNT(*) AS boundary_dates_with_data,
    SUM(txn_count) AS total_boundary_txns,
    -- Verify date_key format is correct (8 digits, valid date)
    SUM(CASE
        WHEN LENGTH(date_key::STRING) <> 8 THEN 1
        WHEN TRY_TO_DATE(TO_CHAR(date_key), 'YYYYMMDD') IS NULL THEN 1
        ELSE 0
    END) AS invalid_date_keys,
    CASE
        WHEN SUM(CASE WHEN TRY_TO_DATE(TO_CHAR(date_key), 'YYYYMMDD') IS NULL THEN 1 ELSE 0 END) = 0
        THEN 'PASS'
        ELSE 'FAIL'
    END AS status
FROM year_boundary_txns;

-- =============================================================================
-- TEST 2: Leap year handling — Feb 29 dates
-- Verify date_key 20240229 is valid and Feb 28 -> Mar 1 transitions are correct
-- =============================================================================
SELECT
    'LEAP_YEAR_HANDLING' AS test_name,
    -- Check if Feb 29 dates exist in dim_date and are valid
    (SELECT COUNT(*) FROM barclays_dwh.dim_date
     WHERE calendar_date = '2024-02-29') AS feb29_2024_exists,
    (SELECT COUNT(*) FROM barclays_dwh.dim_date
     WHERE calendar_date = '2025-02-29') AS feb29_2025_exists,  -- Should be 0 (not leap year)
    -- Verify no transactions on invalid Feb 29
    (SELECT COUNT(*) FROM barclays_dwh.fct_transaction
     WHERE date_key = 20250229) AS txns_on_invalid_feb29,
    CASE
        WHEN (SELECT COUNT(*) FROM barclays_dwh.fct_transaction WHERE date_key = 20250229) = 0
        THEN 'PASS'
        ELSE 'FAIL'
    END AS status;

-- =============================================================================
-- TEST 3: Month-end date calculations
-- Verify monthly reporting uses correct last-day-of-month
-- Critical for regulatory_capital_calc and pnl_rollup
-- =============================================================================
SELECT
    'MONTH_END_DATES' AS test_name,
    reporting_month,
    -- Verify reporting_month is always the 1st of the month
    CASE WHEN DAY(reporting_month) = 1 THEN 'CORRECT' ELSE 'INCORRECT' END AS is_first_of_month,
    -- Verify no gaps in monthly sequence
    LAG(reporting_month) OVER (ORDER BY reporting_month) AS prev_month,
    DATEDIFF('month', LAG(reporting_month) OVER (ORDER BY reporting_month), reporting_month) AS month_gap,
    CASE
        WHEN DAY(reporting_month) <> 1 THEN 'FAIL'
        WHEN DATEDIFF('month', LAG(reporting_month) OVER (ORDER BY reporting_month), reporting_month) > 1
        THEN 'WARNING'
        ELSE 'PASS'
    END AS status
FROM (
    SELECT DISTINCT reporting_month
    FROM barclays_mart.mart_monthly_pnl
    WHERE rollup_level = 'TOTAL'
)
ORDER BY reporting_month;

-- =============================================================================
-- TEST 4: SCD2 effective date boundaries
-- Verify no gaps or overlaps in SCD2 date ranges for same customer
-- =============================================================================
WITH scd2_ranges AS (
    SELECT
        customer_id,
        effective_from,
        effective_to,
        is_current,
        LEAD(effective_from) OVER (
            PARTITION BY customer_id ORDER BY effective_from
        ) AS next_effective_from,
        LAG(effective_to) OVER (
            PARTITION BY customer_id ORDER BY effective_from
        ) AS prev_effective_to
    FROM barclays_dwh.dim_customer
)

SELECT
    'SCD2_DATE_BOUNDARIES' AS test_name,
    -- Check for gaps (prev_effective_to + 1 day should = effective_from)
    SUM(CASE
        WHEN prev_effective_to IS NOT NULL
         AND DATEADD('day', 1, prev_effective_to) <> effective_from
        THEN 1 ELSE 0
    END) AS date_gaps,
    -- Check for overlaps (effective_from <= prev_effective_to)
    SUM(CASE
        WHEN prev_effective_to IS NOT NULL
         AND effective_from <= prev_effective_to
        THEN 1 ELSE 0
    END) AS date_overlaps,
    -- Check that exactly one record per customer is current
    (SELECT COUNT(*) FROM (
        SELECT customer_id, COUNT(*) AS current_count
        FROM barclays_dwh.dim_customer
        WHERE is_current = 'Y'
        GROUP BY customer_id
        HAVING COUNT(*) > 1
    )) AS multi_current_records,
    -- Check that current records have effective_to = 9999-12-31
    SUM(CASE
        WHEN is_current = 'Y' AND effective_to <> '9999-12-31'::DATE
        THEN 1 ELSE 0
    END) AS current_wrong_end_date,
    CASE
        WHEN SUM(CASE WHEN prev_effective_to IS NOT NULL
                  AND DATEADD('day', 1, prev_effective_to) <> effective_from THEN 1 ELSE 0 END) = 0
         AND SUM(CASE WHEN prev_effective_to IS NOT NULL
                  AND effective_from <= prev_effective_to THEN 1 ELSE 0 END) = 0
         AND SUM(CASE WHEN is_current = 'Y' AND effective_to <> '9999-12-31'::DATE THEN 1 ELSE 0 END) = 0
        THEN 'PASS'
        ELSE 'FAIL'
    END AS status
FROM scd2_ranges;

-- =============================================================================
-- TEST 5: Date key format validation
-- All date_key values should be valid 8-digit YYYYMMDD integers
-- =============================================================================
SELECT
    'DATE_KEY_FORMAT' AS test_name,
    COUNT(*) AS total_rows,
    SUM(CASE WHEN LENGTH(date_key::STRING) <> 8 THEN 1 ELSE 0 END) AS wrong_length,
    SUM(CASE WHEN TRY_TO_DATE(TO_CHAR(date_key), 'YYYYMMDD') IS NULL THEN 1 ELSE 0 END) AS invalid_date,
    SUM(CASE WHEN date_key < 19000101 OR date_key > 20991231 THEN 1 ELSE 0 END) AS out_of_range,
    CASE
        WHEN SUM(CASE WHEN TRY_TO_DATE(TO_CHAR(date_key), 'YYYYMMDD') IS NULL THEN 1 ELSE 0 END) = 0
         AND SUM(CASE WHEN date_key < 19000101 OR date_key > 20991231 THEN 1 ELSE 0 END) = 0
        THEN 'PASS'
        ELSE 'FAIL'
    END AS status
FROM barclays_dwh.fct_transaction;

-- =============================================================================
-- TEST 6: Assessment date and reporting date consistency
-- Risk scores should only exist for dates when daily ETL ran
-- =============================================================================
SELECT
    'RISK_DATE_CONSISTENCY' AS test_name,
    COUNT(DISTINCT cr.assessment_date) AS risk_assessment_dates,
    -- Risk dates should match dates in etl_audit_log
    SUM(CASE
        WHEN al.started_at IS NULL THEN 1 ELSE 0
    END) AS risk_dates_without_etl,
    CASE
        WHEN SUM(CASE WHEN al.started_at IS NULL THEN 1 ELSE 0 END) = 0 THEN 'PASS'
        ELSE 'WARNING'
    END AS status
FROM (SELECT DISTINCT assessment_date FROM barclays_mart.mart_credit_risk) cr
LEFT JOIN (
    SELECT DISTINCT started_at::DATE AS etl_date
    FROM barclays_dwh.etl_audit_log
    WHERE pipeline_name = 'DAILY_ETL' AND status = 'SUCCESS'
) al ON cr.assessment_date = al.etl_date;
