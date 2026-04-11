/******************************************************************************
 * test_zero_dollar_claims.sql
 *
 * Tests handling of zero-amount transactions and claims.
 *
 * Zero-dollar records are valid in many scenarios:
 *   - Fee waivers
 *   - Zero-interest periods
 *   - Void/cancelled transactions
 *   - Claims with $0 patient responsibility
 *   - Adjustment entries that net to zero
 *
 * These should be preserved (not filtered out) unless business rules
 * explicitly exclude them.
 ******************************************************************************/

-- =============================================================================
-- TEST 1: Zero-amount transactions exist and are preserved
-- Original Teradata: ZEROIFNULL could mask legitimate zeros
-- Verify zeros are not accidentally introduced by COALESCE(x, 0) migration
-- =============================================================================
SELECT
    'ZERO_AMOUNT_TRANSACTIONS' AS test_name,
    COUNT(*) AS total_transactions,
    SUM(CASE WHEN amount = 0 THEN 1 ELSE 0 END) AS zero_amount_count,
    SUM(CASE WHEN signed_amount = 0 THEN 1 ELSE 0 END) AS zero_signed_amount_count,
    ROUND(SUM(CASE WHEN amount = 0 THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 4) AS pct_zero,
    -- Zero amounts should be a small percentage; if > 10% something may be wrong
    CASE
        WHEN SUM(CASE WHEN amount = 0 THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0) > 10
        THEN 'WARNING'
        ELSE 'PASS'
    END AS status
FROM barclays_dwh.fct_transaction;

-- =============================================================================
-- TEST 2: Zero-dollar transactions by type — identify patterns
-- Some types (like FEE waivers) legitimately have zero amounts
-- =============================================================================
SELECT
    'ZERO_AMOUNT_BY_TYPE' AS test_name,
    transaction_type,
    COUNT(*) AS total_of_type,
    SUM(CASE WHEN amount = 0 THEN 1 ELSE 0 END) AS zero_count,
    ROUND(SUM(CASE WHEN amount = 0 THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 2) AS pct_zero,
    CASE
        -- FEE type with high zero rate is suspicious
        WHEN transaction_type = 'FEE' AND
             SUM(CASE WHEN amount = 0 THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0) > 50
        THEN 'WARNING'
        ELSE 'PASS'
    END AS status
FROM barclays_dwh.fct_transaction
GROUP BY transaction_type
ORDER BY zero_count DESC;

-- =============================================================================
-- TEST 3: Zero-balance accounts — should still have balance records
-- An account with zero balance is valid; verify it's not filtered out
-- =============================================================================
SELECT
    'ZERO_BALANCE_ACCOUNTS' AS test_name,
    COUNT(*) AS total_balance_records,
    SUM(CASE WHEN closing_balance = 0 THEN 1 ELSE 0 END) AS zero_balance_count,
    SUM(CASE WHEN opening_balance = 0 AND closing_balance = 0
             AND total_debits = 0 AND total_credits = 0 THEN 1 ELSE 0 END) AS fully_zero_records,
    -- Every active account should have a balance record for today
    (SELECT COUNT(*) FROM barclays_dwh.dim_account WHERE is_current = 'Y') AS active_accounts,
    (SELECT COUNT(DISTINCT account_sk) FROM barclays_dwh.fct_daily_balance
     WHERE date_key = TO_NUMBER(TO_CHAR(CURRENT_DATE(), 'YYYYMMDD'))) AS accounts_with_balance,
    CASE
        WHEN (SELECT COUNT(*) FROM barclays_dwh.dim_account WHERE is_current = 'Y')
           = (SELECT COUNT(DISTINCT account_sk) FROM barclays_dwh.fct_daily_balance
              WHERE date_key = TO_NUMBER(TO_CHAR(CURRENT_DATE(), 'YYYYMMDD')))
        THEN 'PASS'
        ELSE 'WARNING'
    END AS status;

-- =============================================================================
-- TEST 4: Zero credit limit / overdraft — risk calculation impact
-- ZEROIFNULL migration: verify EAD calculation handles zeros correctly
-- EAD = credit_limit + overdraft_limit; if both zero, EAD should be 0
-- =============================================================================
SELECT
    'ZERO_CREDIT_LIMIT_RISK' AS test_name,
    COUNT(*) AS total_risk_scores,
    SUM(CASE WHEN exposure_at_default = 0 THEN 1 ELSE 0 END) AS zero_ead_count,
    SUM(CASE WHEN exposure_at_default = 0 AND risk_weighted_asset <> 0 THEN 1 ELSE 0 END) AS zero_ead_nonzero_rwa,
    SUM(CASE WHEN exposure_at_default = 0 AND expected_loss <> 0 THEN 1 ELSE 0 END) AS zero_ead_nonzero_el,
    CASE
        -- If EAD is 0, then RWA and EL must also be 0
        WHEN SUM(CASE WHEN exposure_at_default = 0 AND risk_weighted_asset <> 0 THEN 1 ELSE 0 END) = 0
         AND SUM(CASE WHEN exposure_at_default = 0 AND expected_loss <> 0 THEN 1 ELSE 0 END) = 0
        THEN 'PASS'
        ELSE 'FAIL'
    END AS status
FROM barclays_mart.mart_credit_risk;

-- =============================================================================
-- TEST 5: Zero-dollar claims in healthcare context
-- Claims with $0 paid_amount are valid (e.g., denied claims, capitated)
-- =============================================================================
SELECT
    'ZERO_DOLLAR_CLAIMS' AS test_name,
    COUNT(*) AS total_claims,
    SUM(CASE WHEN paid_amount = 0 THEN 1 ELSE 0 END) AS zero_paid_claims,
    SUM(CASE WHEN allowed_amount = 0 THEN 1 ELSE 0 END) AS zero_allowed_claims,
    SUM(CASE WHEN charge_amount = 0 THEN 1 ELSE 0 END) AS zero_charge_claims,
    SUM(CASE WHEN paid_amount = 0 AND allowed_amount = 0 AND charge_amount = 0
        THEN 1 ELSE 0 END) AS fully_zero_claims,
    -- Zero paid but non-zero charge = denied or adjusted (valid)
    SUM(CASE WHEN paid_amount = 0 AND charge_amount > 0 THEN 1 ELSE 0 END) AS denied_pattern,
    CASE
        -- Fully zero claims (all amounts = 0) may indicate data quality issue
        WHEN SUM(CASE WHEN paid_amount = 0 AND allowed_amount = 0 AND charge_amount = 0
                  THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0) > 5
        THEN 'WARNING'
        ELSE 'PASS'
    END AS status
FROM barclays_dwh.fct_medical_claim;

-- =============================================================================
-- TEST 6: Zero-amount AML impact — should not trigger false alerts
-- Verify zero-amount transactions don't create spurious AML alerts
-- =============================================================================
SELECT
    'ZERO_AMOUNT_AML_ALERTS' AS test_name,
    COUNT(*) AS total_alerts,
    SUM(CASE WHEN a.total_amount = 0 THEN 1 ELSE 0 END) AS zero_amount_alerts,
    CASE
        WHEN SUM(CASE WHEN a.total_amount = 0 THEN 1 ELSE 0 END) = 0 THEN 'PASS'
        ELSE 'WARNING'
    END AS status
FROM barclays_mart.mart_aml_alerts a
WHERE a.alert_type IN ('STRUCTURING', 'VELOCITY_BREACH');
