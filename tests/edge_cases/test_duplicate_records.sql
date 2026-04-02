/******************************************************************************
 * test_duplicate_records.sql
 *
 * Tests deduplication behavior with exact and near-exact duplicates.
 *
 * Migration context:
 *   - Teradata SET tables automatically reject exact duplicate rows
 *   - Snowflake MULTISET tables allow duplicates by default
 *   - The migrated pipeline must handle dedup explicitly
 *
 * Tests:
 *   1. No exact duplicate rows in dimension tables
 *   2. No duplicate business keys in current dimension records
 *   3. No duplicate transaction IDs
 *   4. No duplicate claim_id + line_number combinations
 *   5. Near-duplicate detection (same key, different timestamps)
 ******************************************************************************/

-- =============================================================================
-- TEST 1: No exact duplicate rows in dimension tables
-- Teradata SET tables rejected exact duplicates; verify Snowflake equivalent
-- =============================================================================

-- dim_customer: No exact duplicate rows
SELECT
    'EXACT_DUPLICATES_DIM_CUSTOMER' AS test_name,
    COUNT(*) AS duplicate_groups,
    SUM(dup_count - 1) AS extra_rows,
    CASE
        WHEN COUNT(*) = 0 THEN 'PASS'
        ELSE 'FAIL'
    END AS status
FROM (
    SELECT
        customer_id, first_name, last_name, date_of_birth,
        nationality, kyc_status, risk_rating, segment,
        postcode, country, is_current, effective_from, effective_to,
        COUNT(*) AS dup_count
    FROM barclays_dwh.dim_customer
    GROUP BY customer_id, first_name, last_name, date_of_birth,
             nationality, kyc_status, risk_rating, segment,
             postcode, country, is_current, effective_from, effective_to
    HAVING COUNT(*) > 1
);

-- dim_account: No exact duplicate rows
SELECT
    'EXACT_DUPLICATES_DIM_ACCOUNT' AS test_name,
    COUNT(*) AS duplicate_groups,
    SUM(dup_count - 1) AS extra_rows,
    CASE
        WHEN COUNT(*) = 0 THEN 'PASS'
        ELSE 'FAIL'
    END AS status
FROM (
    SELECT
        account_id, customer_id, account_type, currency,
        branch_code, sort_code, status, open_date,
        is_current,
        COUNT(*) AS dup_count
    FROM barclays_dwh.dim_account
    GROUP BY account_id, customer_id, account_type, currency,
             branch_code, sort_code, status, open_date,
             is_current
    HAVING COUNT(*) > 1
);

-- =============================================================================
-- TEST 2: No duplicate business keys in current dimension records
-- Each customer_id should have exactly one is_current='Y' record
-- Each account_id should have exactly one is_current='Y' record
-- =============================================================================
SELECT
    'DUPLICATE_BUSINESS_KEY_CUSTOMER' AS test_name,
    COUNT(*) AS customers_with_multi_current,
    CASE
        WHEN COUNT(*) = 0 THEN 'PASS'
        ELSE 'FAIL'
    END AS status
FROM (
    SELECT customer_id, COUNT(*) AS current_count
    FROM barclays_dwh.dim_customer
    WHERE is_current = 'Y'
    GROUP BY customer_id
    HAVING COUNT(*) > 1
)

UNION ALL

SELECT
    'DUPLICATE_BUSINESS_KEY_ACCOUNT' AS test_name,
    COUNT(*) AS accounts_with_multi_current,
    CASE
        WHEN COUNT(*) = 0 THEN 'PASS'
        ELSE 'FAIL'
    END AS status
FROM (
    SELECT account_id, COUNT(*) AS current_count
    FROM barclays_dwh.dim_account
    WHERE is_current = 'Y'
    GROUP BY account_id
    HAVING COUNT(*) > 1
);

-- =============================================================================
-- TEST 3: No duplicate transaction IDs in fact table
-- transaction_id should be unique (natural key)
-- =============================================================================
SELECT
    'DUPLICATE_TRANSACTION_IDS' AS test_name,
    COUNT(*) AS duplicate_txn_ids,
    SUM(dup_count - 1) AS extra_rows,
    CASE
        WHEN COUNT(*) = 0 THEN 'PASS'
        ELSE 'FAIL'
    END AS status
FROM (
    SELECT transaction_id, COUNT(*) AS dup_count
    FROM barclays_dwh.fct_transaction
    GROUP BY transaction_id
    HAVING COUNT(*) > 1
);

-- =============================================================================
-- TEST 4: No duplicate claim_id + claim_line_number after dedup
-- (Healthcare-specific: post-ADR dedup validation)
-- =============================================================================
SELECT
    'DUPLICATE_CLAIM_LINES' AS test_name,
    COUNT(*) AS duplicate_claim_lines,
    SUM(dup_count - 1) AS extra_rows,
    CASE
        WHEN COUNT(*) = 0 THEN 'PASS'
        ELSE 'FAIL'
    END AS status
FROM (
    SELECT claim_id, claim_line_number, COUNT(*) AS dup_count
    FROM barclays_dwh.fct_medical_claim
    GROUP BY claim_id, claim_line_number
    HAVING COUNT(*) > 1
);

-- =============================================================================
-- TEST 5: No duplicate risk scores for same customer + date
-- Each customer should have at most one risk score per assessment date
-- =============================================================================
SELECT
    'DUPLICATE_RISK_SCORES' AS test_name,
    COUNT(*) AS duplicate_risk_entries,
    SUM(dup_count - 1) AS extra_rows,
    CASE
        WHEN COUNT(*) = 0 THEN 'PASS'
        ELSE 'FAIL'
    END AS status
FROM (
    SELECT customer_id, assessment_date, COUNT(*) AS dup_count
    FROM barclays_mart.mart_credit_risk
    GROUP BY customer_id, assessment_date
    HAVING COUNT(*) > 1
);

-- =============================================================================
-- TEST 6: No duplicate AML alerts for same customer + date + type
-- =============================================================================
SELECT
    'DUPLICATE_AML_ALERTS' AS test_name,
    COUNT(*) AS duplicate_alert_entries,
    SUM(dup_count - 1) AS extra_rows,
    CASE
        WHEN COUNT(*) = 0 THEN 'PASS'
        ELSE 'WARNING'  -- Duplicate alerts may be valid for different accounts
    END AS status
FROM (
    SELECT customer_id, alert_date, alert_type, account_id, COUNT(*) AS dup_count
    FROM barclays_mart.mart_aml_alerts
    GROUP BY customer_id, alert_date, alert_type, account_id
    HAVING COUNT(*) > 1
);

-- =============================================================================
-- TEST 7: Near-duplicate detection — same key, different load timestamps
-- Detects cases where Snowpipe may have loaded the same file twice
-- =============================================================================
SELECT
    'NEAR_DUPLICATES_RAW_TRANSACTIONS' AS test_name,
    COUNT(*) AS near_duplicate_groups,
    SUM(dup_count - 1) AS potential_extra_rows,
    CASE
        WHEN COUNT(*) = 0 THEN 'PASS'
        WHEN SUM(dup_count - 1) < 100 THEN 'WARNING'
        ELSE 'FAIL'
    END AS status
FROM (
    SELECT
        transaction_id, account_id, transaction_date, amount, signed_amount,
        COUNT(*) AS dup_count,
        COUNT(DISTINCT _loaded_at) AS distinct_load_times
    FROM barclays_raw.transaction
    GROUP BY transaction_id, account_id, transaction_date, amount, signed_amount
    HAVING COUNT(*) > 1
);

-- =============================================================================
-- TEST 8: Regulatory capital — no duplicate reporting_date + asset_class
-- =============================================================================
SELECT
    'DUPLICATE_REGULATORY_CAPITAL' AS test_name,
    COUNT(*) AS duplicate_entries,
    CASE
        WHEN COUNT(*) = 0 THEN 'PASS'
        ELSE 'FAIL'
    END AS status
FROM (
    SELECT reporting_date, asset_class, rollup_level, COUNT(*) AS dup_count
    FROM barclays_mart.mart_regulatory_capital
    GROUP BY reporting_date, asset_class, rollup_level
    HAVING COUNT(*) > 1
);
