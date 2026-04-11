/******************************************************************************
 * test_aggregate_reconciliation.sql
 *
 * Aggregate-level reconciliation comparing key financial totals between
 * the raw/source layer and the migrated DWH/mart layer.
 *
 * Tests:
 *   1. Total paid/signed amounts by month
 *   2. Customer/member counts by segment
 *   3. Transaction counts by type
 *   4. Balance snapshot totals
 *   5. Monthly trend comparison
 *
 * All comparisons are grouped by month for trend analysis.
 ******************************************************************************/

-- =============================================================================
-- Configuration
-- =============================================================================
SET recon_start = DATEADD('month', -6, DATE_TRUNC('month', CURRENT_DATE()));
SET recon_end   = CURRENT_DATE();
SET amount_threshold_pct = 0.01;  -- 0.01% tolerance on monetary amounts
SET count_threshold_pct  = 1.0;   -- 1% tolerance on counts

-- =============================================================================
-- TEST 1: Total transaction amounts by month
-- Compare raw signed_amount totals against DWH fct_transaction totals
-- =============================================================================
WITH raw_monthly_amounts AS (
    SELECT
        DATE_TRUNC('month', transaction_date) AS month_start,
        SUM(signed_amount)                     AS total_signed_amount,
        SUM(amount)                            AS total_abs_amount,
        SUM(CASE WHEN signed_amount > 0 THEN signed_amount ELSE 0 END) AS total_credits,
        SUM(CASE WHEN signed_amount < 0 THEN ABS(signed_amount) ELSE 0 END) AS total_debits,
        COUNT(*)                               AS txn_count
    FROM barclays_raw.transaction
    WHERE transaction_date BETWEEN $recon_start AND $recon_end
    GROUP BY DATE_TRUNC('month', transaction_date)
),

dwh_monthly_amounts AS (
    SELECT
        DATE_TRUNC('month', TO_DATE(TO_CHAR(date_key), 'YYYYMMDD')) AS month_start,
        SUM(signed_amount)                     AS total_signed_amount,
        SUM(amount)                            AS total_abs_amount,
        SUM(CASE WHEN signed_amount > 0 THEN signed_amount ELSE 0 END) AS total_credits,
        SUM(CASE WHEN signed_amount < 0 THEN ABS(signed_amount) ELSE 0 END) AS total_debits,
        COUNT(*)                               AS txn_count
    FROM barclays_dwh.fct_transaction
    WHERE TO_DATE(TO_CHAR(date_key), 'YYYYMMDD') BETWEEN $recon_start AND $recon_end
    GROUP BY DATE_TRUNC('month', TO_DATE(TO_CHAR(date_key), 'YYYYMMDD'))
)

SELECT
    'MONTHLY_AMOUNTS'                          AS test_category,
    COALESCE(r.month_start, d.month_start)     AS month_start,
    r.total_signed_amount                      AS raw_signed_amount,
    d.total_signed_amount                      AS dwh_signed_amount,
    d.total_signed_amount - COALESCE(r.total_signed_amount, 0) AS amount_difference,
    CASE
        WHEN r.total_signed_amount = 0 AND d.total_signed_amount = 0 THEN 0
        WHEN r.total_signed_amount = 0 THEN 100.0
        ELSE ROUND(ABS(d.total_signed_amount - r.total_signed_amount)
                    * 100.0 / ABS(r.total_signed_amount), 6)
    END                                        AS pct_difference,
    r.txn_count                                AS raw_txn_count,
    d.txn_count                                AS dwh_txn_count,
    CASE
        WHEN COALESCE(r.total_signed_amount, 0) = COALESCE(d.total_signed_amount, 0) THEN 'PASS'
        WHEN ABS(d.total_signed_amount - COALESCE(r.total_signed_amount, 0))
             * 100.0 / NULLIF(ABS(r.total_signed_amount), 0) <= $amount_threshold_pct THEN 'PASS'
        ELSE 'FAIL'
    END                                        AS status
FROM raw_monthly_amounts r
FULL OUTER JOIN dwh_monthly_amounts d ON r.month_start = d.month_start
ORDER BY month_start;

-- =============================================================================
-- TEST 2: Customer counts by segment
-- Compare raw customer counts against DWH dim_customer (current records)
-- =============================================================================
WITH raw_segment_counts AS (
    SELECT
        segment,
        COUNT(DISTINCT customer_id) AS customer_count
    FROM barclays_raw.customer
    GROUP BY segment
),

dwh_segment_counts AS (
    SELECT
        segment,
        COUNT(DISTINCT customer_id) AS customer_count
    FROM barclays_dwh.dim_customer
    WHERE is_current = 'Y'
    GROUP BY segment
)

SELECT
    'CUSTOMER_BY_SEGMENT'                          AS test_category,
    COALESCE(r.segment, d.segment)                 AS segment,
    r.customer_count                               AS raw_customer_count,
    d.customer_count                               AS dwh_customer_count,
    COALESCE(d.customer_count, 0) - COALESCE(r.customer_count, 0) AS difference,
    CASE
        WHEN COALESCE(r.customer_count, 0) = 0 AND COALESCE(d.customer_count, 0) = 0 THEN 'PASS'
        WHEN COALESCE(r.customer_count, 0) = COALESCE(d.customer_count, 0) THEN 'PASS'
        ELSE 'WARNING'
    END                                            AS status
FROM raw_segment_counts r
FULL OUTER JOIN dwh_segment_counts d ON r.segment = d.segment
ORDER BY segment;

-- =============================================================================
-- TEST 3: Transaction counts by type (monthly)
-- =============================================================================
WITH raw_type_counts AS (
    SELECT
        DATE_TRUNC('month', transaction_date) AS month_start,
        transaction_type,
        COUNT(*) AS txn_count,
        SUM(amount) AS total_amount
    FROM barclays_raw.transaction
    WHERE transaction_date BETWEEN $recon_start AND $recon_end
    GROUP BY DATE_TRUNC('month', transaction_date), transaction_type
),

dwh_type_counts AS (
    SELECT
        DATE_TRUNC('month', TO_DATE(TO_CHAR(date_key), 'YYYYMMDD')) AS month_start,
        transaction_type,
        COUNT(*) AS txn_count,
        SUM(amount) AS total_amount
    FROM barclays_dwh.fct_transaction
    WHERE TO_DATE(TO_CHAR(date_key), 'YYYYMMDD') BETWEEN $recon_start AND $recon_end
    GROUP BY DATE_TRUNC('month', TO_DATE(TO_CHAR(date_key), 'YYYYMMDD')), transaction_type
)

SELECT
    'TXN_COUNT_BY_TYPE'                                AS test_category,
    COALESCE(r.month_start, d.month_start)             AS month_start,
    COALESCE(r.transaction_type, d.transaction_type)   AS transaction_type,
    r.txn_count                                        AS raw_txn_count,
    d.txn_count                                        AS dwh_txn_count,
    COALESCE(d.txn_count, 0) - COALESCE(r.txn_count, 0) AS count_difference,
    r.total_amount                                     AS raw_total_amount,
    d.total_amount                                     AS dwh_total_amount,
    CASE
        WHEN COALESCE(r.txn_count, 0) = COALESCE(d.txn_count, 0)
             AND COALESCE(r.total_amount, 0) = COALESCE(d.total_amount, 0) THEN 'PASS'
        WHEN ABS(COALESCE(d.txn_count, 0) - COALESCE(r.txn_count, 0))
             * 100.0 / NULLIF(r.txn_count, 0) <= $count_threshold_pct THEN 'WARNING'
        ELSE 'FAIL'
    END                                                AS status
FROM raw_type_counts r
FULL OUTER JOIN dwh_type_counts d
    ON r.month_start = d.month_start AND r.transaction_type = d.transaction_type
ORDER BY month_start, transaction_type;

-- =============================================================================
-- TEST 4: Balance snapshot totals by month
-- Compare aggregated daily balance totals
-- =============================================================================
SELECT
    'BALANCE_SNAPSHOT_MONTHLY'                      AS test_category,
    DATE_TRUNC('month', TO_DATE(TO_CHAR(date_key), 'YYYYMMDD')) AS month_start,
    COUNT(DISTINCT account_sk)                      AS accounts_with_balances,
    SUM(closing_balance)                            AS total_closing_balance,
    SUM(total_debits)                               AS total_monthly_debits,
    SUM(total_credits)                              AS total_monthly_credits,
    SUM(transaction_count)                          AS total_monthly_txns,
    -- Sanity check: credits - debits should roughly equal balance change
    CASE
        WHEN ABS(SUM(total_credits) - SUM(total_debits)) > ABS(SUM(closing_balance)) * 10
        THEN 'WARNING'
        ELSE 'PASS'
    END                                             AS status
FROM barclays_dwh.fct_daily_balance
WHERE TO_DATE(TO_CHAR(date_key), 'YYYYMMDD') BETWEEN $recon_start AND $recon_end
GROUP BY DATE_TRUNC('month', TO_DATE(TO_CHAR(date_key), 'YYYYMMDD'))
ORDER BY month_start;

-- =============================================================================
-- TEST 5: Risk score coverage — every current customer should have a score
-- =============================================================================
SELECT
    'RISK_SCORE_COVERAGE'                           AS test_category,
    dc.segment,
    COUNT(DISTINCT dc.customer_id)                  AS total_customers,
    COUNT(DISTINCT cr.customer_id)                  AS customers_with_risk,
    COUNT(DISTINCT dc.customer_id) - COUNT(DISTINCT cr.customer_id) AS missing_risk_scores,
    CASE
        WHEN COUNT(DISTINCT dc.customer_id) = COUNT(DISTINCT cr.customer_id) THEN 'PASS'
        WHEN COUNT(DISTINCT dc.customer_id) - COUNT(DISTINCT cr.customer_id)
             <= COUNT(DISTINCT dc.customer_id) * 0.01 THEN 'WARNING'
        ELSE 'FAIL'
    END                                             AS status
FROM barclays_dwh.dim_customer dc
LEFT JOIN barclays_mart.mart_credit_risk cr
    ON dc.customer_id = cr.customer_id
    AND cr.assessment_date = $recon_end
WHERE dc.is_current = 'Y'
GROUP BY dc.segment
ORDER BY segment;
