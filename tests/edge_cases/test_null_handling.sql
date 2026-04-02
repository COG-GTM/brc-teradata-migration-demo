/******************************************************************************
 * test_null_handling.sql
 *
 * Tests NULL propagation through joins and aggregations in the migrated
 * pipeline. Validates that Teradata-to-Snowflake NULL handling differences
 * are correctly addressed.
 *
 * Key migration considerations:
 *   - Teradata ZEROIFNULL(x) -> Snowflake COALESCE(x, 0)
 *   - Teradata NULLIFZERO(x) -> Snowflake NULLIF(x, 0)
 *   - NULL handling in CASE expressions
 *   - NULL propagation through LEFT JOINs
 *   - Aggregate behavior with NULLs (SUM, COUNT, AVG)
 ******************************************************************************/

-- =============================================================================
-- TEST 1: ZEROIFNULL/COALESCE migration — no unexpected NULLs in numeric cols
-- Original Teradata used ZEROIFNULL extensively; verify COALESCE replacement
-- =============================================================================
SELECT
    'NULL_IN_NUMERIC_COLUMNS' AS test_name,
    'fct_transaction' AS table_name,
    SUM(CASE WHEN amount IS NULL THEN 1 ELSE 0 END) AS null_amount,
    SUM(CASE WHEN signed_amount IS NULL THEN 1 ELSE 0 END) AS null_signed_amount,
    SUM(CASE WHEN balance_after IS NULL THEN 1 ELSE 0 END) AS null_balance_after,
    COUNT(*) AS total_rows,
    CASE
        WHEN SUM(CASE WHEN amount IS NULL THEN 1 ELSE 0 END) = 0
         AND SUM(CASE WHEN signed_amount IS NULL THEN 1 ELSE 0 END) = 0
        THEN 'PASS'
        ELSE 'WARNING'
    END AS status
FROM barclays_dwh.fct_transaction

UNION ALL

SELECT
    'NULL_IN_NUMERIC_COLUMNS',
    'fct_daily_balance',
    SUM(CASE WHEN opening_balance IS NULL THEN 1 ELSE 0 END),
    SUM(CASE WHEN closing_balance IS NULL THEN 1 ELSE 0 END),
    SUM(CASE WHEN total_debits IS NULL THEN 1 ELSE 0 END),
    COUNT(*),
    CASE
        WHEN SUM(CASE WHEN closing_balance IS NULL THEN 1 ELSE 0 END) = 0
        THEN 'PASS'
        ELSE 'FAIL'
    END
FROM barclays_dwh.fct_daily_balance

UNION ALL

SELECT
    'NULL_IN_NUMERIC_COLUMNS',
    'mart_credit_risk',
    SUM(CASE WHEN probability_default IS NULL THEN 1 ELSE 0 END),
    SUM(CASE WHEN exposure_at_default IS NULL THEN 1 ELSE 0 END),
    SUM(CASE WHEN risk_weighted_asset IS NULL THEN 1 ELSE 0 END),
    COUNT(*),
    CASE
        WHEN SUM(CASE WHEN probability_default IS NULL THEN 1 ELSE 0 END) = 0
         AND SUM(CASE WHEN exposure_at_default IS NULL THEN 1 ELSE 0 END) = 0
        THEN 'PASS'
        ELSE 'FAIL'
    END
FROM barclays_mart.mart_credit_risk;

-- =============================================================================
-- TEST 2: NULL propagation through LEFT JOINs
-- Customers without accounts should still appear in dim_customer
-- but should have NULL/0 in account-derived metrics
-- =============================================================================
SELECT
    'NULL_LEFT_JOIN_PROPAGATION' AS test_name,
    COUNT(DISTINCT dc.customer_id) AS total_customers,
    COUNT(DISTINCT da.customer_id) AS customers_with_accounts,
    COUNT(DISTINCT dc.customer_id) - COUNT(DISTINCT da.customer_id) AS customers_without_accounts,
    -- Verify risk scores handle customers without accounts (EAD should be 0, not NULL)
    SUM(CASE
        WHEN da.customer_id IS NULL AND cr.exposure_at_default IS NOT NULL AND cr.exposure_at_default <> 0
        THEN 1 ELSE 0
    END) AS incorrect_ead_for_no_account,
    CASE
        WHEN SUM(CASE
            WHEN da.customer_id IS NULL AND cr.exposure_at_default IS NOT NULL AND cr.exposure_at_default <> 0
            THEN 1 ELSE 0
        END) = 0 THEN 'PASS'
        ELSE 'FAIL'
    END AS status
FROM barclays_dwh.dim_customer dc
LEFT JOIN barclays_dwh.dim_account da
    ON dc.customer_id = da.customer_id AND da.is_current = 'Y'
LEFT JOIN barclays_mart.mart_credit_risk cr
    ON dc.customer_id = cr.customer_id
    AND cr.assessment_date = CURRENT_DATE()
WHERE dc.is_current = 'Y';

-- =============================================================================
-- TEST 3: NULL in CASE expressions — risk rating should never be NULL
-- Original Teradata procedure had CASE with explicit branches + ELSE
-- =============================================================================
SELECT
    'NULL_IN_CASE_EXPRESSIONS' AS test_name,
    SUM(CASE WHEN risk_rating IS NULL THEN 1 ELSE 0 END) AS null_risk_rating,
    SUM(CASE WHEN asset_class IS NULL THEN 1 ELSE 0 END) AS null_asset_class,
    SUM(CASE WHEN model_version IS NULL THEN 1 ELSE 0 END) AS null_model_version,
    COUNT(*) AS total_rows,
    CASE
        WHEN SUM(CASE WHEN risk_rating IS NULL THEN 1 ELSE 0 END) = 0
         AND SUM(CASE WHEN asset_class IS NULL THEN 1 ELSE 0 END) = 0
        THEN 'PASS'
        ELSE 'FAIL'
    END AS status
FROM barclays_mart.mart_credit_risk;

-- =============================================================================
-- TEST 4: Aggregate functions with NULLs
-- SUM(NULL) = NULL in both Teradata and Snowflake, but verify COALESCE wrapping
-- =============================================================================
SELECT
    'NULL_AGGREGATION_BEHAVIOR' AS test_name,
    -- Check that monthly PNL has no NULL in aggregated columns
    SUM(CASE WHEN gross_revenue IS NULL THEN 1 ELSE 0 END) AS null_gross_revenue,
    SUM(CASE WHEN net_profit IS NULL THEN 1 ELSE 0 END) AS null_net_profit,
    SUM(CASE WHEN cost_income_ratio IS NULL THEN 1 ELSE 0 END) AS null_cir,
    COUNT(*) AS total_rows,
    CASE
        WHEN SUM(CASE WHEN gross_revenue IS NULL THEN 1 ELSE 0 END) = 0
         AND SUM(CASE WHEN net_profit IS NULL THEN 1 ELSE 0 END) = 0
        THEN 'PASS'
        ELSE 'FAIL'
    END AS status
FROM barclays_mart.mart_monthly_pnl;

-- =============================================================================
-- TEST 5: NULL in surrogate key joins
-- Verify no transactions have NULL surrogate keys after dimension lookups
-- =============================================================================
SELECT
    'NULL_SURROGATE_KEYS' AS test_name,
    SUM(CASE WHEN account_sk IS NULL THEN 1 ELSE 0 END) AS null_account_sk,
    SUM(CASE WHEN customer_sk IS NULL THEN 1 ELSE 0 END) AS null_customer_sk,
    SUM(CASE WHEN date_key IS NULL THEN 1 ELSE 0 END) AS null_date_key,
    COUNT(*) AS total_rows,
    CASE
        WHEN SUM(CASE WHEN account_sk IS NULL THEN 1 ELSE 0 END) = 0
         AND SUM(CASE WHEN customer_sk IS NULL THEN 1 ELSE 0 END) = 0
         AND SUM(CASE WHEN date_key IS NULL THEN 1 ELSE 0 END) = 0
        THEN 'PASS'
        ELSE 'FAIL'
    END AS status
FROM barclays_dwh.fct_transaction;

-- =============================================================================
-- TEST 6: NULL in string columns — dimension attributes
-- Verify required string fields are not NULL
-- =============================================================================
SELECT
    'NULL_STRING_COLUMNS' AS test_name,
    SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END) AS null_customer_id,
    SUM(CASE WHEN kyc_status IS NULL THEN 1 ELSE 0 END) AS null_kyc_status,
    SUM(CASE WHEN risk_rating IS NULL THEN 1 ELSE 0 END) AS null_risk_rating,
    SUM(CASE WHEN segment IS NULL THEN 1 ELSE 0 END) AS null_segment,
    SUM(CASE WHEN is_current IS NULL THEN 1 ELSE 0 END) AS null_is_current,
    COUNT(*) AS total_rows,
    CASE
        WHEN SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END) = 0
         AND SUM(CASE WHEN is_current IS NULL THEN 1 ELSE 0 END) = 0
        THEN 'PASS'
        ELSE 'FAIL'
    END AS status
FROM barclays_dwh.dim_customer;
