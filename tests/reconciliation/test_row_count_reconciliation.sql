/******************************************************************************
 * test_row_count_reconciliation.sql
 *
 * Row count reconciliation between Teradata source (or raw layer) and
 * the migrated Snowflake DWH/mart tables.
 *
 * For each key table, compares expected vs actual row counts and flags
 * any discrepancies above a configurable threshold.
 *
 * Output columns:
 *   table_name, platform, expected_count, actual_count, difference,
 *   pct_difference, status (PASS/FAIL/WARNING)
 *
 * Usage:
 *   Run this after each daily/monthly ETL to confirm data completeness.
 *   During migration, compare Teradata exports against Snowflake counts.
 ******************************************************************************/

-- =============================================================================
-- Configuration: Set the reconciliation date and threshold
-- =============================================================================
SET recon_date = CURRENT_DATE();
SET pct_threshold = 1.0;  -- Percentage difference threshold for FAIL status

-- =============================================================================
-- Row Count Reconciliation: All Key Tables
-- =============================================================================
WITH expected_counts AS (
    -- RAW layer counts (source of truth from Snowpipe ingestion)
    SELECT 'barclays_raw.transaction'    AS table_name, 'RAW'  AS platform,
           COUNT(*) AS row_count
    FROM barclays_raw.transaction
    WHERE transaction_date = $recon_date

    UNION ALL
    SELECT 'barclays_raw.market_data',   'RAW',
           COUNT(*)
    FROM barclays_raw.market_data
    WHERE valuation_date = $recon_date

    UNION ALL
    SELECT 'barclays_raw.customer',      'RAW',
           COUNT(*)
    FROM barclays_raw.customer

    UNION ALL
    SELECT 'barclays_raw.account',       'RAW',
           COUNT(*)
    FROM barclays_raw.account
),

actual_counts AS (
    -- DWH layer counts (after ETL processing)
    SELECT 'barclays_dwh.fct_transaction'  AS table_name, 'DWH' AS platform,
           COUNT(*) AS row_count
    FROM barclays_dwh.fct_transaction
    WHERE date_key = TO_NUMBER(TO_CHAR($recon_date, 'YYYYMMDD'))

    UNION ALL
    SELECT 'barclays_dwh.dim_market_data', 'DWH',
           COUNT(*)
    FROM barclays_dwh.dim_market_data
    WHERE valuation_date = $recon_date

    UNION ALL
    SELECT 'barclays_dwh.dim_customer',    'DWH',
           COUNT(*)
    FROM barclays_dwh.dim_customer
    WHERE is_current = 'Y'

    UNION ALL
    SELECT 'barclays_dwh.dim_account',     'DWH',
           COUNT(*)
    FROM barclays_dwh.dim_account
    WHERE is_current = 'Y'

    UNION ALL
    SELECT 'barclays_dwh.fct_daily_balance', 'DWH',
           COUNT(*)
    FROM barclays_dwh.fct_daily_balance
    WHERE date_key = TO_NUMBER(TO_CHAR($recon_date, 'YYYYMMDD'))

    -- MART layer counts
    UNION ALL
    SELECT 'barclays_mart.mart_credit_risk', 'MART',
           COUNT(*)
    FROM barclays_mart.mart_credit_risk
    WHERE assessment_date = $recon_date

    UNION ALL
    SELECT 'barclays_mart.mart_aml_alerts',  'MART',
           COUNT(*)
    FROM barclays_mart.mart_aml_alerts
    WHERE alert_date = $recon_date
),

-- Map raw tables to their DWH/mart counterparts
reconciliation_pairs AS (
    SELECT
        'RAW_TO_DWH_TRANSACTIONS' AS check_name,
        'barclays_raw.transaction' AS source_table,
        'barclays_dwh.fct_transaction' AS target_table,
        (SELECT row_count FROM expected_counts WHERE table_name = 'barclays_raw.transaction') AS expected_count,
        (SELECT row_count FROM actual_counts WHERE table_name = 'barclays_dwh.fct_transaction') AS actual_count

    UNION ALL
    SELECT
        'RAW_TO_DWH_MARKET_DATA',
        'barclays_raw.market_data',
        'barclays_dwh.dim_market_data',
        (SELECT row_count FROM expected_counts WHERE table_name = 'barclays_raw.market_data'),
        (SELECT row_count FROM actual_counts WHERE table_name = 'barclays_dwh.dim_market_data')

    UNION ALL
    SELECT
        'RAW_TO_DWH_CUSTOMERS',
        'barclays_raw.customer',
        'barclays_dwh.dim_customer',
        (SELECT row_count FROM expected_counts WHERE table_name = 'barclays_raw.customer'),
        (SELECT row_count FROM actual_counts WHERE table_name = 'barclays_dwh.dim_customer')

    UNION ALL
    SELECT
        'RAW_TO_DWH_ACCOUNTS',
        'barclays_raw.account',
        'barclays_dwh.dim_account',
        (SELECT row_count FROM expected_counts WHERE table_name = 'barclays_raw.account'),
        (SELECT row_count FROM actual_counts WHERE table_name = 'barclays_dwh.dim_account')

    UNION ALL
    SELECT
        'DWH_CUSTOMERS_TO_RISK_SCORES',
        'barclays_dwh.dim_customer (current)',
        'barclays_mart.mart_credit_risk',
        (SELECT row_count FROM actual_counts WHERE table_name = 'barclays_dwh.dim_customer'),
        (SELECT row_count FROM actual_counts WHERE table_name = 'barclays_mart.mart_credit_risk')
)

-- =============================================================================
-- Final reconciliation output
-- =============================================================================
SELECT
    check_name,
    source_table,
    target_table,
    expected_count,
    actual_count,
    actual_count - expected_count AS difference,
    CASE
        WHEN expected_count = 0 AND actual_count = 0 THEN 0.0
        WHEN expected_count = 0 THEN 100.0
        ELSE ROUND(ABS(actual_count - expected_count) * 100.0 / expected_count, 4)
    END AS pct_difference,
    CASE
        WHEN expected_count = 0 AND actual_count = 0 THEN 'PASS'
        WHEN expected_count = 0 AND actual_count > 0 THEN 'WARNING'
        WHEN ABS(actual_count - expected_count) * 100.0 / NULLIF(expected_count, 0) > $pct_threshold THEN 'FAIL'
        WHEN ABS(actual_count - expected_count) * 100.0 / NULLIF(expected_count, 0) > 0 THEN 'WARNING'
        ELSE 'PASS'
    END AS status,
    $recon_date AS reconciliation_date,
    CURRENT_TIMESTAMP() AS run_timestamp
FROM reconciliation_pairs
ORDER BY
    CASE status WHEN 'FAIL' THEN 1 WHEN 'WARNING' THEN 2 ELSE 3 END,
    check_name;

-- =============================================================================
-- Historical trend: Row counts over the last 30 days
-- =============================================================================
-- SELECT
--     TO_DATE(TO_CHAR(date_key), 'YYYYMMDD') AS business_date,
--     COUNT(*) AS daily_txn_count,
--     AVG(COUNT(*)) OVER (ORDER BY date_key ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS ma7_txn_count
-- FROM barclays_dwh.fct_transaction
-- WHERE date_key >= TO_NUMBER(TO_CHAR(DATEADD('day', -30, CURRENT_DATE()), 'YYYYMMDD'))
-- GROUP BY date_key
-- ORDER BY date_key;

-- =============================================================================
-- Persist results for dashboard
-- =============================================================================
-- INSERT INTO barclays_dwh.etl_validation_results (
--     validation_date, check_name, status, expected_value, actual_value, details
-- )
-- SELECT
--     $recon_date,
--     'ROW_COUNT_' || check_name,
--     status,
--     expected_count::STRING,
--     actual_count::STRING,
--     CONCAT('Difference: ', difference, ' (', pct_difference, '%)')
-- FROM (
--     -- Paste the main query above here
-- );
