/******************************************************************************
 * test_adr_dedup_validation.sql
 *
 * ADR (Adjudication/Disposition Record) Deduplication Validation.
 *
 * In healthcare claims processing, when multiple claim lines exist for the
 * same service, an ADR priority order determines which record to keep.
 * This test validates the dedup logic used in the Tuva-integrated claims
 * pipeline.
 *
 * ADR Priority Order (business rule):
 *   1. PAID     — Claim was paid (highest priority)
 *   2. ADJUSTED — Claim was adjusted/modified
 *   3. DENIED   — Claim was denied (lowest priority)
 *
 * Tests:
 *   1. Verify priority order is correctly applied
 *   2. Compare claim counts before and after dedup
 *   3. Flag cases where different priority orders would produce different results
 *   4. Validate no duplicate claim_id+line combinations remain after dedup
 ******************************************************************************/

-- =============================================================================
-- TEST 1: Verify ADR priority order is correctly applied
-- For each claim_id + service_line, only the highest priority status should remain
-- =============================================================================
WITH adr_priority AS (
    -- Define the ADR priority order
    SELECT 'PAID'     AS claim_status, 1 AS priority UNION ALL
    SELECT 'ADJUSTED' AS claim_status, 2 AS priority UNION ALL
    SELECT 'DENIED'   AS claim_status, 3 AS priority
),

-- Raw claims before dedup (all statuses present)
raw_claims_with_priority AS (
    SELECT
        rc.claim_id,
        rc.claim_line_number,
        rc.claim_status,
        ap.priority,
        rc.paid_amount,
        rc.allowed_amount,
        rc.charge_amount,
        ROW_NUMBER() OVER (
            PARTITION BY rc.claim_id, rc.claim_line_number
            ORDER BY ap.priority ASC  -- Lowest priority number = highest priority
        ) AS dedup_rank
    FROM barclays_raw.medical_claim rc
    INNER JOIN adr_priority ap ON rc.claim_status = ap.claim_status
),

-- Expected dedup result: only rank 1 records
expected_dedup AS (
    SELECT *
    FROM raw_claims_with_priority
    WHERE dedup_rank = 1
),

-- Actual dedup result from the migrated pipeline
actual_dedup AS (
    SELECT
        claim_id,
        claim_line_number,
        claim_status,
        paid_amount,
        allowed_amount,
        charge_amount
    FROM barclays_dwh.fct_medical_claim  -- or the Tuva core medical_claim model
)

SELECT
    'ADR_PRIORITY_ORDER'                        AS test_name,
    COUNT(*)                                     AS total_deduped_claims,
    SUM(CASE WHEN e.claim_status = a.claim_status THEN 1 ELSE 0 END) AS matching_status,
    SUM(CASE WHEN e.claim_status <> a.claim_status THEN 1 ELSE 0 END) AS mismatched_status,
    CASE
        WHEN SUM(CASE WHEN e.claim_status <> a.claim_status THEN 1 ELSE 0 END) = 0 THEN 'PASS'
        ELSE 'FAIL'
    END                                          AS status
FROM expected_dedup e
INNER JOIN actual_dedup a
    ON e.claim_id = a.claim_id
    AND e.claim_line_number = a.claim_line_number;

-- =============================================================================
-- TEST 2: Compare claim counts before and after dedup
-- =============================================================================
SELECT
    'ADR_DEDUP_COUNTS'                                   AS test_name,
    (SELECT COUNT(*) FROM barclays_raw.medical_claim)    AS raw_claim_count,
    (SELECT COUNT(DISTINCT claim_id || '|' || claim_line_number)
     FROM barclays_raw.medical_claim)                    AS raw_unique_lines,
    (SELECT COUNT(*) FROM barclays_dwh.fct_medical_claim) AS deduped_claim_count,
    -- After dedup, count should equal unique claim_id + line combinations
    CASE
        WHEN (SELECT COUNT(*) FROM barclays_dwh.fct_medical_claim)
           = (SELECT COUNT(DISTINCT claim_id || '|' || claim_line_number)
              FROM barclays_raw.medical_claim) THEN 'PASS'
        WHEN (SELECT COUNT(*) FROM barclays_dwh.fct_medical_claim)
           < (SELECT COUNT(DISTINCT claim_id || '|' || claim_line_number)
              FROM barclays_raw.medical_claim) THEN 'WARNING'
        ELSE 'FAIL'
    END                                                  AS status;

-- =============================================================================
-- TEST 3: Flag cases where different priority orders produce different results
-- Identifies claim_id+line combinations that have multiple statuses
-- These are the "contentious" records where ADR priority actually matters
-- =============================================================================
WITH multi_status_claims AS (
    SELECT
        claim_id,
        claim_line_number,
        COUNT(DISTINCT claim_status) AS status_count,
        LISTAGG(DISTINCT claim_status, ', ') WITHIN GROUP (ORDER BY claim_status) AS statuses_present,
        SUM(CASE WHEN claim_status = 'PAID' THEN paid_amount ELSE 0 END)     AS paid_amount_if_paid,
        SUM(CASE WHEN claim_status = 'ADJUSTED' THEN paid_amount ELSE 0 END) AS paid_amount_if_adjusted,
        SUM(CASE WHEN claim_status = 'DENIED' THEN paid_amount ELSE 0 END)   AS paid_amount_if_denied
    FROM barclays_raw.medical_claim
    GROUP BY claim_id, claim_line_number
    HAVING COUNT(DISTINCT claim_status) > 1
)

SELECT
    'ADR_PRIORITY_IMPACT'                          AS test_name,
    COUNT(*)                                       AS multi_status_claim_lines,
    SUM(CASE WHEN statuses_present LIKE '%PAID%' AND statuses_present LIKE '%DENIED%'
        THEN 1 ELSE 0 END)                        AS paid_vs_denied_conflicts,
    SUM(CASE WHEN statuses_present LIKE '%PAID%' AND statuses_present LIKE '%ADJUSTED%'
        THEN 1 ELSE 0 END)                        AS paid_vs_adjusted_conflicts,
    SUM(paid_amount_if_paid - paid_amount_if_denied) AS total_amount_impact_paid_vs_denied,
    -- If we used Denied priority instead of Paid priority, how much would change?
    SUM(ABS(paid_amount_if_paid - paid_amount_if_denied)) AS absolute_impact,
    CASE
        WHEN COUNT(*) = 0 THEN 'PASS'
        ELSE 'INFO'  -- Multi-status claims exist; verify priority is correctly applied
    END                                            AS status
FROM multi_status_claims;

-- =============================================================================
-- TEST 4: Validate no duplicate claim_id + line combinations after dedup
-- =============================================================================
SELECT
    'ADR_NO_DUPLICATES_AFTER_DEDUP'                AS test_name,
    COUNT(*)                                       AS duplicate_combinations,
    CASE
        WHEN COUNT(*) = 0 THEN 'PASS'
        ELSE 'FAIL'
    END                                            AS status
FROM (
    SELECT
        claim_id,
        claim_line_number,
        COUNT(*) AS record_count
    FROM barclays_dwh.fct_medical_claim
    GROUP BY claim_id, claim_line_number
    HAVING COUNT(*) > 1
);

-- =============================================================================
-- TEST 5: Verify Paid > Adjusted > Denied precedence with explicit examples
-- =============================================================================
WITH precedence_check AS (
    SELECT
        a.claim_id,
        a.claim_line_number,
        a.claim_status AS kept_status,
        -- Check if there were higher-priority records that should have been kept instead
        EXISTS (
            SELECT 1 FROM barclays_raw.medical_claim r
            WHERE r.claim_id = a.claim_id
              AND r.claim_line_number = a.claim_line_number
              AND r.claim_status = 'PAID'
        ) AS had_paid_version,
        EXISTS (
            SELECT 1 FROM barclays_raw.medical_claim r
            WHERE r.claim_id = a.claim_id
              AND r.claim_line_number = a.claim_line_number
              AND r.claim_status = 'ADJUSTED'
        ) AS had_adjusted_version,
        EXISTS (
            SELECT 1 FROM barclays_raw.medical_claim r
            WHERE r.claim_id = a.claim_id
              AND r.claim_line_number = a.claim_line_number
              AND r.claim_status = 'DENIED'
        ) AS had_denied_version
    FROM barclays_dwh.fct_medical_claim a
)

SELECT
    'ADR_PRECEDENCE_VIOLATIONS'                    AS test_name,
    -- Cases where ADJUSTED was kept but PAID existed
    SUM(CASE WHEN kept_status = 'ADJUSTED' AND had_paid_version THEN 1 ELSE 0 END)
        AS adjusted_kept_over_paid,
    -- Cases where DENIED was kept but PAID or ADJUSTED existed
    SUM(CASE WHEN kept_status = 'DENIED' AND (had_paid_version OR had_adjusted_version) THEN 1 ELSE 0 END)
        AS denied_kept_over_higher,
    CASE
        WHEN SUM(CASE WHEN kept_status = 'ADJUSTED' AND had_paid_version THEN 1 ELSE 0 END) = 0
         AND SUM(CASE WHEN kept_status = 'DENIED' AND (had_paid_version OR had_adjusted_version) THEN 1 ELSE 0 END) = 0
        THEN 'PASS'
        ELSE 'FAIL'
    END                                            AS status
FROM precedence_check;
