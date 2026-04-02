/******************************************************************************
 * test_tuva_integration.sql
 *
 * Tuva Data Quality (DQ) Test Integration.
 *
 * The Tuva Project includes 600+ data quality tests covering:
 *   - Claims preprocessing (encounter grouping, service categorization)
 *   - Core model integrity (eligibility, medical_claim, pharmacy_claim)
 *   - Data mart validation (HCC risk scores, CCSR categories, PMPM)
 *   - Terminology normalization (ICD-10, LOINC, RxNorm, NPI)
 *
 * This file documents how to run the full Tuva DQ suite and provides
 * wrapper queries to capture and validate results.
 *
 * Source: COG-GTM/tuva (600+ tests in schema.yml and custom tests)
 ******************************************************************************/

-- =============================================================================
-- SECTION 1: How to run the full Tuva DQ test suite
-- =============================================================================
/*
 * Prerequisites:
 *   1. Tuva dbt project is configured with valid profile targeting Snowflake
 *   2. Seeds are loaded: dbt seed --full-refresh
 *   3. Models are built: dbt run
 *
 * Run all Tuva data quality tests:
 *   dbt test --select tag:dqi
 *
 * Run specific test categories:
 *   dbt test --select tag:claims_preprocessing
 *   dbt test --select tag:core
 *   dbt test --select tag:hcc
 *   dbt test --select tag:ccsr
 *   dbt test --select tag:pmpm
 *   dbt test --select tag:readmissions
 *
 * Capture results to JSON:
 *   dbt test --select tag:dqi --store-failures --target snowflake
 *
 * The --store-failures flag persists failing rows into the
 * <target_schema>_dbt_test__audit schema for investigation.
 *
 * Expected results (baseline for clean synthetic data):
 *   Total tests:     600+
 *   Expected PASS:   ~580+ (depends on data completeness)
 *   Expected WARN:   ~10-20 (data quality indicators, not failures)
 *   Expected FAIL:   0 (on clean synthetic data)
 *   Expected ERROR:  0
 */

-- =============================================================================
-- SECTION 2: Wrapper to capture dbt test results from Snowflake
-- After running: dbt test --select tag:dqi --store-failures
-- =============================================================================

-- View the most recent dbt test run results
-- (Requires elementary or dbt artifacts loaded into Snowflake)
SELECT
    test_name,
    test_type,
    status,
    failures,
    execution_time,
    compiled_sql
FROM (
    -- Option A: If using Elementary data observability
    SELECT
        test_name,
        test_type,
        status,
        failures,
        execution_time,
        compiled_sql
    FROM elementary.test_results
    WHERE run_started_at = (SELECT MAX(run_started_at) FROM elementary.test_results)

    -- Option B: If using dbt artifacts table
    -- SELECT
    --     node_id AS test_name,
    --     'generic' AS test_type,
    --     status,
    --     failures,
    --     execution_time,
    --     compiled_code AS compiled_sql
    -- FROM dbt_artifacts.test_results
    -- WHERE run_id = (SELECT MAX(run_id) FROM dbt_artifacts.test_results)
)
ORDER BY
    CASE status WHEN 'fail' THEN 1 WHEN 'warn' THEN 2 WHEN 'error' THEN 3 ELSE 4 END,
    test_name;

-- =============================================================================
-- SECTION 3: Tuva DQ test categories and expected counts
-- =============================================================================
/*
 * Test Category Breakdown:
 *
 * | Category                  | Tag                      | ~Tests | Description                                    |
 * |---------------------------|--------------------------|--------|------------------------------------------------|
 * | Input Layer Validation    | tag:input_layer          | ~50    | Schema tests on raw input tables               |
 * | Claims Preprocessing      | tag:claims_preprocessing | ~80    | Encounter grouping, service categorization     |
 * | Core Model Tests          | tag:core                 | ~120   | Eligibility, medical/pharmacy claims, condition|
 * | Terminology Normalization | tag:terminology          | ~60    | ICD-10, LOINC, NPI, HCPCS mapping             |
 * | CMS-HCC Risk Adjustment  | tag:hcc                  | ~50    | HCC risk score calculation validation          |
 * | CCSR Categories           | tag:ccsr                 | ~40    | Clinical classification validation             |
 * | PMPM Metrics              | tag:pmpm                 | ~30    | Per-member-per-month financial calculations    |
 * | Quality Measures          | tag:quality_measures     | ~40    | HEDIS and other quality measure logic          |
 * | Readmissions              | tag:readmissions         | ~20    | 30-day readmission calculations                |
 * | Data Quality Indicators   | tag:dqi                  | ~100   | Cross-cutting data quality checks              |
 * | Chronic Conditions        | tag:chronic_conditions   | ~30    | Chronic condition identification               |
 * |---------------------------|--------------------------|--------|------------------------------------------------|
 * | TOTAL                     |                          | ~620   |                                                |
 */

-- =============================================================================
-- SECTION 4: Quick validation queries to run directly in Snowflake
-- These replicate key Tuva DQ checks without needing to run dbt
-- =============================================================================

-- Check 1: Verify input layer tables are populated
SELECT
    'INPUT_LAYER_POPULATED' AS check_name,
    (SELECT COUNT(*) FROM tuva.input_layer.medical_claim) AS medical_claim_count,
    (SELECT COUNT(*) FROM tuva.input_layer.pharmacy_claim) AS pharmacy_claim_count,
    (SELECT COUNT(*) FROM tuva.input_layer.eligibility) AS eligibility_count,
    CASE
        WHEN (SELECT COUNT(*) FROM tuva.input_layer.medical_claim) > 0
         AND (SELECT COUNT(*) FROM tuva.input_layer.eligibility) > 0
        THEN 'PASS'
        ELSE 'FAIL'
    END AS status;

-- Check 2: Verify core models are built
SELECT
    'CORE_MODELS_BUILT' AS check_name,
    (SELECT COUNT(*) FROM tuva.core.medical_claim) AS core_medical_claim,
    (SELECT COUNT(*) FROM tuva.core.eligibility) AS core_eligibility,
    (SELECT COUNT(*) FROM tuva.core.condition) AS core_condition,
    (SELECT COUNT(*) FROM tuva.core.encounter) AS core_encounter,
    CASE
        WHEN (SELECT COUNT(*) FROM tuva.core.medical_claim) > 0
         AND (SELECT COUNT(*) FROM tuva.core.eligibility) > 0
        THEN 'PASS'
        ELSE 'FAIL'
    END AS status;

-- Check 3: Verify no orphan claims (claims without matching eligibility)
SELECT
    'ORPHAN_CLAIMS_CHECK' AS check_name,
    COUNT(*) AS orphan_claims,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'WARNING' END AS status
FROM tuva.core.medical_claim mc
LEFT JOIN tuva.core.eligibility e
    ON mc.patient_id = e.patient_id
    AND mc.claim_start_date BETWEEN e.enrollment_start_date AND e.enrollment_end_date
WHERE e.patient_id IS NULL;

-- Check 4: Verify HCC risk scores are calculated
SELECT
    'HCC_RISK_SCORES' AS check_name,
    COUNT(DISTINCT patient_id) AS patients_with_scores,
    AVG(risk_score) AS avg_risk_score,
    MIN(risk_score) AS min_risk_score,
    MAX(risk_score) AS max_risk_score,
    CASE
        WHEN COUNT(DISTINCT patient_id) > 0 AND AVG(risk_score) > 0 THEN 'PASS'
        WHEN COUNT(DISTINCT patient_id) = 0 THEN 'FAIL'
        ELSE 'WARNING'
    END AS status
FROM tuva.data_marts.cms_hcc__patient_risk_scores;

-- Check 5: Service category completeness
SELECT
    'SERVICE_CATEGORY_COVERAGE' AS check_name,
    COUNT(*) AS total_claims,
    SUM(CASE WHEN service_category_1 IS NOT NULL THEN 1 ELSE 0 END) AS has_category_1,
    SUM(CASE WHEN service_category_2 IS NOT NULL THEN 1 ELSE 0 END) AS has_category_2,
    ROUND(SUM(CASE WHEN service_category_1 IS NOT NULL THEN 1 ELSE 0 END) * 100.0
          / NULLIF(COUNT(*), 0), 2) AS pct_categorized,
    CASE
        WHEN SUM(CASE WHEN service_category_1 IS NOT NULL THEN 1 ELSE 0 END) * 100.0
             / NULLIF(COUNT(*), 0) >= 95 THEN 'PASS'
        WHEN SUM(CASE WHEN service_category_1 IS NOT NULL THEN 1 ELSE 0 END) * 100.0
             / NULLIF(COUNT(*), 0) >= 80 THEN 'WARNING'
        ELSE 'FAIL'
    END AS status
FROM tuva.claims_preprocessing.service_category_grouper;

-- =============================================================================
-- SECTION 5: Store Tuva DQ results for dashboard consumption
-- =============================================================================
/*
 * After running `dbt test --store-failures`, failing rows are stored in:
 *   <target_schema>_dbt_test__audit.<test_name>
 *
 * To aggregate results for the validation dashboard:
 *
 * CREATE OR REPLACE TABLE barclays_dwh.tuva_dq_summary AS
 * SELECT
 *     CURRENT_TIMESTAMP() AS run_timestamp,
 *     'tuva_dq' AS test_suite,
 *     COUNT(*) AS total_tests,
 *     SUM(CASE WHEN status = 'pass' THEN 1 ELSE 0 END) AS passed,
 *     SUM(CASE WHEN status = 'warn' THEN 1 ELSE 0 END) AS warnings,
 *     SUM(CASE WHEN status = 'fail' THEN 1 ELSE 0 END) AS failed,
 *     SUM(CASE WHEN status = 'error' THEN 1 ELSE 0 END) AS errors,
 *     ROUND(SUM(CASE WHEN status = 'pass' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS pass_rate
 * FROM elementary.test_results
 * WHERE run_started_at = (SELECT MAX(run_started_at) FROM elementary.test_results);
 */
