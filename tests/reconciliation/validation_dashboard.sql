/******************************************************************************
 * validation_dashboard.sql
 *
 * Single query that produces a summary dashboard of ALL validation results.
 * Aggregates results from all test categories into a unified view.
 *
 * Output:
 *   - Pass/fail status for each category
 *   - Test counts and percentages
 *   - Overall migration validation health score
 *
 * Usage:
 *   Run after completing a full daily or monthly ETL cycle.
 *   Results can be consumed by BI tools (Sigma, Tableau, Looker, etc.)
 ******************************************************************************/

-- =============================================================================
-- VALIDATION DASHBOARD: Unified summary of all validation results
-- =============================================================================
WITH

-- Category 1: Row Count Reconciliation
row_count_results AS (
    SELECT
        'Row Count Reconciliation' AS category,
        COUNT(*) AS total_checks,
        SUM(CASE WHEN status = 'PASS' THEN 1 ELSE 0 END) AS passed,
        SUM(CASE WHEN status = 'WARNING' THEN 1 ELSE 0 END) AS warnings,
        SUM(CASE WHEN status = 'FAIL' THEN 1 ELSE 0 END) AS failed
    FROM barclays_dwh.etl_validation_results
    WHERE check_name LIKE 'ROW_COUNT_%'
      AND validation_date = CURRENT_DATE()
),

-- Category 2: Aggregate Reconciliation
aggregate_results AS (
    SELECT
        'Aggregate Reconciliation' AS category,
        COUNT(*) AS total_checks,
        SUM(CASE WHEN status = 'PASS' THEN 1 ELSE 0 END) AS passed,
        SUM(CASE WHEN status = 'WARNING' THEN 1 ELSE 0 END) AS warnings,
        SUM(CASE WHEN status = 'FAIL' THEN 1 ELSE 0 END) AS failed
    FROM barclays_dwh.etl_validation_results
    WHERE check_name LIKE 'AGG_%'
      AND validation_date = CURRENT_DATE()
),

-- Category 3: ADR Dedup Validation
adr_results AS (
    SELECT
        'ADR Dedup Validation' AS category,
        COUNT(*) AS total_checks,
        SUM(CASE WHEN status = 'PASS' THEN 1 ELSE 0 END) AS passed,
        SUM(CASE WHEN status = 'WARNING' THEN 1 ELSE 0 END) AS warnings,
        SUM(CASE WHEN status = 'FAIL' THEN 1 ELSE 0 END) AS failed
    FROM barclays_dwh.etl_validation_results
    WHERE check_name LIKE 'ADR_%'
      AND validation_date = CURRENT_DATE()
),

-- Category 4: Encounter Grouping
encounter_results AS (
    SELECT
        'Encounter Grouping' AS category,
        COUNT(*) AS total_checks,
        SUM(CASE WHEN status = 'PASS' THEN 1 ELSE 0 END) AS passed,
        SUM(CASE WHEN status = 'WARNING' THEN 1 ELSE 0 END) AS warnings,
        SUM(CASE WHEN status = 'FAIL' THEN 1 ELSE 0 END) AS failed
    FROM barclays_dwh.etl_validation_results
    WHERE check_name LIKE 'ENCOUNTER_%'
      AND validation_date = CURRENT_DATE()
),

-- Category 5: Tuva DQ Integration
tuva_results AS (
    SELECT
        'Tuva DQ Integration' AS category,
        COALESCE(total_tests, 0) AS total_checks,
        COALESCE(passed, 0) AS passed,
        COALESCE(warnings, 0) AS warnings,
        COALESCE(failed, 0) AS failed
    FROM barclays_dwh.tuva_dq_summary
    WHERE run_timestamp = (SELECT MAX(run_timestamp) FROM barclays_dwh.tuva_dq_summary)
),

-- Category 6: Edge Cases (NULL handling, dates, zero dollar, reversals, duplicates)
edge_case_results AS (
    SELECT
        'Edge Case Tests' AS category,
        COUNT(*) AS total_checks,
        SUM(CASE WHEN status = 'PASS' THEN 1 ELSE 0 END) AS passed,
        SUM(CASE WHEN status = 'WARNING' THEN 1 ELSE 0 END) AS warnings,
        SUM(CASE WHEN status = 'FAIL' THEN 1 ELSE 0 END) AS failed
    FROM barclays_dwh.etl_validation_results
    WHERE check_name LIKE ANY ('NULL_%', 'DATE_%', 'ZERO_%', 'REVERSAL_%', 'DUPLICATE_%', 'SCD2_%')
      AND validation_date = CURRENT_DATE()
),

-- Category 7: Pipeline Orchestration Health
pipeline_results AS (
    SELECT
        'Pipeline Orchestration' AS category,
        COUNT(*) AS total_checks,
        SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) AS passed,
        SUM(CASE WHEN status LIKE 'WARNING%' THEN 1 ELSE 0 END) AS warnings,
        SUM(CASE WHEN status IN ('FAIL', 'FAILED') THEN 1 ELSE 0 END) AS failed
    FROM barclays_dwh.etl_audit_log
    WHERE started_at::DATE = CURRENT_DATE()
),

-- Category 8: Snowpipe Ingestion Health
snowpipe_results AS (
    SELECT
        'Snowpipe Ingestion' AS category,
        COUNT(*) AS total_checks,
        SUM(CASE WHEN status = 'PASS' OR status = 'SUCCESS' THEN 1 ELSE 0 END) AS passed,
        SUM(CASE WHEN status = 'WARNING' THEN 1 ELSE 0 END) AS warnings,
        SUM(CASE WHEN status = 'FAIL' THEN 1 ELSE 0 END) AS failed
    FROM barclays_dwh.etl_validation_results
    WHERE check_name LIKE 'SNOWPIPE_%'
      AND validation_date = CURRENT_DATE()
),

-- Category 9: Regulatory Compliance
regulatory_results AS (
    SELECT
        'Regulatory Compliance' AS category,
        COUNT(*) AS total_checks,
        SUM(CASE WHEN status = 'PASS' THEN 1 ELSE 0 END) AS passed,
        SUM(CASE WHEN status = 'WARNING' THEN 1 ELSE 0 END) AS warnings,
        SUM(CASE WHEN status IN ('FAIL', 'BREACH') THEN 1 ELSE 0 END) AS failed
    FROM barclays_dwh.etl_validation_results
    WHERE check_name LIKE ANY ('CAPITAL_%', 'LEVERAGE_%', 'BASEL_%', 'PNL_%', 'COST_INCOME_%')
      AND validation_date >= DATE_TRUNC('month', CURRENT_DATE())
),

-- Combine all categories
all_categories AS (
    SELECT * FROM row_count_results
    UNION ALL SELECT * FROM aggregate_results
    UNION ALL SELECT * FROM adr_results
    UNION ALL SELECT * FROM encounter_results
    UNION ALL SELECT * FROM tuva_results
    UNION ALL SELECT * FROM edge_case_results
    UNION ALL SELECT * FROM pipeline_results
    UNION ALL SELECT * FROM snowpipe_results
    UNION ALL SELECT * FROM regulatory_results
)

-- =============================================================================
-- MAIN DASHBOARD OUTPUT
-- =============================================================================
SELECT
    '═══════════════════════════════════════════════════════════════' AS separator,
    '  MIGRATION VALIDATION DASHBOARD — ' || CURRENT_DATE()::STRING AS title,
    '═══════════════════════════════════════════════════════════════' AS separator2
FROM (SELECT 1)

UNION ALL

SELECT '', '', '' FROM (SELECT 1)

UNION ALL

SELECT
    category,
    CONCAT(
        passed::STRING, ' / ', total_checks::STRING, ' passed',
        CASE WHEN warnings > 0 THEN CONCAT(' (', warnings, ' warnings)') ELSE '' END,
        CASE WHEN failed > 0 THEN CONCAT(' [', failed, ' FAILED]') ELSE '' END
    ),
    CASE
        WHEN total_checks = 0 THEN '⊘ NO DATA'
        WHEN failed > 0 THEN '✗ FAIL'
        WHEN warnings > 0 THEN '⚠ WARNING'
        ELSE '✓ PASS'
    END
FROM all_categories
ORDER BY
    CASE
        WHEN failed > 0 THEN 1
        WHEN warnings > 0 THEN 2
        WHEN total_checks = 0 THEN 3
        ELSE 4
    END,
    category;

-- =============================================================================
-- OVERALL HEALTH SCORE
-- =============================================================================
SELECT
    'OVERALL_HEALTH_SCORE' AS metric,
    SUM(total_checks) AS total_validation_checks,
    SUM(passed) AS total_passed,
    SUM(warnings) AS total_warnings,
    SUM(failed) AS total_failed,
    CASE
        WHEN SUM(total_checks) = 0 THEN 0
        ELSE ROUND(SUM(passed) * 100.0 / SUM(total_checks), 2)
    END AS pass_rate_pct,
    CASE
        WHEN SUM(failed) = 0 AND SUM(warnings) = 0 THEN 'HEALTHY'
        WHEN SUM(failed) = 0 THEN 'DEGRADED'
        ELSE 'CRITICAL'
    END AS overall_status,
    CURRENT_TIMESTAMP() AS dashboard_generated_at
FROM all_categories;

-- =============================================================================
-- DETAILED FAILURE LOG (only failing checks)
-- =============================================================================
SELECT
    validation_date,
    check_name,
    status,
    expected_value,
    actual_value,
    details
FROM barclays_dwh.etl_validation_results
WHERE status IN ('FAIL', 'BREACH')
  AND validation_date >= DATEADD('day', -7, CURRENT_DATE())
ORDER BY validation_date DESC, check_name;

-- =============================================================================
-- TREND: Pass rate over last 30 days
-- =============================================================================
SELECT
    validation_date,
    COUNT(*) AS total_checks,
    SUM(CASE WHEN status = 'PASS' THEN 1 ELSE 0 END) AS passed,
    SUM(CASE WHEN status = 'FAIL' THEN 1 ELSE 0 END) AS failed,
    ROUND(SUM(CASE WHEN status = 'PASS' THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 2) AS pass_rate_pct
FROM barclays_dwh.etl_validation_results
WHERE validation_date >= DATEADD('day', -30, CURRENT_DATE())
GROUP BY validation_date
ORDER BY validation_date;
