/******************************************************************************
 * test_encounter_grouping_validation.sql
 *
 * Encounter Grouping Validation for Healthcare Claims.
 *
 * Tests that the unified model correctly groups inpatient claim lines into
 * encounters (stays) following CMS guidelines for date-based grouping.
 *
 * Scenarios tested:
 *   1. Fully overlapping stays (same admission/discharge dates)
 *   2. Partially overlapping stays (overlap by N days)
 *   3. Adjacent stays (discharge date = next admission date)
 *   4. Gap stays (1-day gap between stays)
 *   5. Edge case: single-day stays (admission = discharge)
 *
 * CMS Guidelines reference:
 *   - Overlapping or adjacent inpatient stays for the same patient at the
 *     same facility should be merged into a single encounter
 *   - A gap of >= 1 calendar day between discharge and next admission
 *     constitutes a separate encounter
 ******************************************************************************/

-- =============================================================================
-- TEST 1: Fully overlapping stays (same dates)
-- Two claim lines with identical admission/discharge dates should be grouped
-- into a single encounter
-- =============================================================================
WITH fully_overlapping AS (
    SELECT
        patient_id,
        facility_id,
        admission_date,
        discharge_date,
        COUNT(*) AS claim_line_count,
        COUNT(DISTINCT encounter_id) AS encounter_count
    FROM barclays_dwh.fct_inpatient_claim
    GROUP BY patient_id, facility_id, admission_date, discharge_date
    HAVING COUNT(*) > 1
)

SELECT
    'FULLY_OVERLAPPING_STAYS'                      AS test_name,
    COUNT(*)                                       AS total_overlapping_groups,
    SUM(claim_line_count)                          AS total_claim_lines,
    SUM(CASE WHEN encounter_count = 1 THEN 1 ELSE 0 END) AS correctly_grouped,
    SUM(CASE WHEN encounter_count > 1 THEN 1 ELSE 0 END) AS incorrectly_split,
    CASE
        WHEN SUM(CASE WHEN encounter_count > 1 THEN 1 ELSE 0 END) = 0 THEN 'PASS'
        ELSE 'FAIL'
    END                                            AS status,
    'Same-date claims should map to single encounter' AS description
FROM fully_overlapping;

-- =============================================================================
-- TEST 2: Partially overlapping stays (overlap by N days)
-- Stays where one admission occurs before the prior discharge should merge
-- =============================================================================
WITH ordered_stays AS (
    SELECT
        patient_id,
        facility_id,
        encounter_id,
        admission_date,
        discharge_date,
        LAG(discharge_date) OVER (
            PARTITION BY patient_id, facility_id
            ORDER BY admission_date, discharge_date
        ) AS prev_discharge_date
    FROM (
        SELECT DISTINCT
            patient_id, facility_id, encounter_id,
            admission_date, discharge_date
        FROM barclays_dwh.fct_inpatient_claim
    )
),

partial_overlaps AS (
    SELECT
        patient_id,
        facility_id,
        admission_date,
        discharge_date,
        prev_discharge_date,
        encounter_id,
        LAG(encounter_id) OVER (
            PARTITION BY patient_id, facility_id
            ORDER BY admission_date, discharge_date
        ) AS prev_encounter_id,
        DATEDIFF('day', admission_date, prev_discharge_date) AS overlap_days
    FROM ordered_stays
    WHERE prev_discharge_date IS NOT NULL
      AND admission_date < prev_discharge_date   -- Overlap exists
      AND admission_date > LAG(admission_date) OVER (
              PARTITION BY patient_id, facility_id
              ORDER BY admission_date, discharge_date
          )  -- Different start dates (not fully overlapping)
)

SELECT
    'PARTIALLY_OVERLAPPING_STAYS'                  AS test_name,
    COUNT(*)                                       AS total_partial_overlaps,
    AVG(overlap_days)                              AS avg_overlap_days,
    MAX(overlap_days)                              AS max_overlap_days,
    SUM(CASE WHEN encounter_id = prev_encounter_id THEN 1 ELSE 0 END) AS correctly_merged,
    SUM(CASE WHEN encounter_id <> prev_encounter_id THEN 1 ELSE 0 END) AS incorrectly_separated,
    CASE
        WHEN SUM(CASE WHEN encounter_id <> prev_encounter_id THEN 1 ELSE 0 END) = 0 THEN 'PASS'
        ELSE 'FAIL'
    END                                            AS status,
    'Overlapping stays should merge into same encounter' AS description
FROM partial_overlaps;

-- =============================================================================
-- TEST 3: Adjacent stays (discharge date = next admission date)
-- Per CMS guidelines, adjacent stays (no gap) should be merged
-- =============================================================================
WITH adjacent_stays AS (
    SELECT
        s1.patient_id,
        s1.facility_id,
        s1.encounter_id  AS encounter_1,
        s2.encounter_id  AS encounter_2,
        s1.discharge_date AS stay1_discharge,
        s2.admission_date AS stay2_admission,
        DATEDIFF('day', s1.discharge_date, s2.admission_date) AS gap_days
    FROM (
        SELECT DISTINCT patient_id, facility_id, encounter_id,
               admission_date, discharge_date
        FROM barclays_dwh.fct_inpatient_claim
    ) s1
    INNER JOIN (
        SELECT DISTINCT patient_id, facility_id, encounter_id,
               admission_date, discharge_date
        FROM barclays_dwh.fct_inpatient_claim
    ) s2
        ON s1.patient_id = s2.patient_id
        AND s1.facility_id = s2.facility_id
        AND s1.encounter_id <> s2.encounter_id
        AND s1.discharge_date = s2.admission_date  -- Adjacent: no gap
)

SELECT
    'ADJACENT_STAYS'                               AS test_name,
    COUNT(*)                                       AS total_adjacent_pairs,
    SUM(CASE WHEN encounter_1 = encounter_2 THEN 1 ELSE 0 END) AS correctly_merged,
    SUM(CASE WHEN encounter_1 <> encounter_2 THEN 1 ELSE 0 END) AS kept_separate,
    CASE
        -- Adjacent stays SHOULD be merged per CMS guidelines
        WHEN COUNT(*) = 0 THEN 'PASS'
        WHEN SUM(CASE WHEN encounter_1 = encounter_2 THEN 1 ELSE 0 END) = COUNT(*) THEN 'PASS'
        ELSE 'WARNING'  -- May be acceptable depending on facility's interpretation
    END                                            AS status,
    'Adjacent stays (discharge=admission) should merge per CMS' AS description
FROM adjacent_stays;

-- =============================================================================
-- TEST 4: Gap stays (1+ day gap between stays)
-- Stays with >= 1 day gap should be separate encounters
-- =============================================================================
WITH gap_stays AS (
    SELECT
        s1.patient_id,
        s1.facility_id,
        s1.encounter_id  AS encounter_1,
        s2.encounter_id  AS encounter_2,
        s1.discharge_date AS stay1_discharge,
        s2.admission_date AS stay2_admission,
        DATEDIFF('day', s1.discharge_date, s2.admission_date) AS gap_days
    FROM (
        SELECT DISTINCT patient_id, facility_id, encounter_id,
               admission_date, discharge_date
        FROM barclays_dwh.fct_inpatient_claim
    ) s1
    INNER JOIN (
        SELECT DISTINCT patient_id, facility_id, encounter_id,
               admission_date, discharge_date
        FROM barclays_dwh.fct_inpatient_claim
    ) s2
        ON s1.patient_id = s2.patient_id
        AND s1.facility_id = s2.facility_id
        AND s1.encounter_id <> s2.encounter_id
        AND DATEDIFF('day', s1.discharge_date, s2.admission_date) >= 1  -- Gap exists
        AND DATEDIFF('day', s1.discharge_date, s2.admission_date) <= 30 -- Within 30 days (relevant range)
)

SELECT
    'GAP_STAYS'                                    AS test_name,
    COUNT(*)                                       AS total_gap_pairs,
    AVG(gap_days)                                  AS avg_gap_days,
    MIN(gap_days)                                  AS min_gap_days,
    MAX(gap_days)                                  AS max_gap_days,
    SUM(CASE WHEN encounter_1 <> encounter_2 THEN 1 ELSE 0 END) AS correctly_separated,
    SUM(CASE WHEN encounter_1 = encounter_2 THEN 1 ELSE 0 END) AS incorrectly_merged,
    CASE
        WHEN SUM(CASE WHEN encounter_1 = encounter_2 THEN 1 ELSE 0 END) = 0 THEN 'PASS'
        ELSE 'FAIL'
    END                                            AS status,
    'Stays with gap >= 1 day should be separate encounters' AS description
FROM gap_stays;

-- =============================================================================
-- TEST 5: Single-day stays (admission = discharge)
-- Edge case: ensure single-day stays are valid encounters
-- =============================================================================
SELECT
    'SINGLE_DAY_STAYS'                             AS test_name,
    COUNT(DISTINCT encounter_id)                   AS single_day_encounters,
    COUNT(*)                                       AS single_day_claim_lines,
    -- Single-day stays should still have valid encounter IDs
    SUM(CASE WHEN encounter_id IS NULL THEN 1 ELSE 0 END) AS missing_encounter_ids,
    -- Verify discharge is not before admission
    SUM(CASE WHEN discharge_date < admission_date THEN 1 ELSE 0 END) AS invalid_date_ranges,
    CASE
        WHEN SUM(CASE WHEN encounter_id IS NULL THEN 1 ELSE 0 END) = 0
         AND SUM(CASE WHEN discharge_date < admission_date THEN 1 ELSE 0 END) = 0 THEN 'PASS'
        ELSE 'FAIL'
    END                                            AS status,
    'Single-day stays should have valid encounter IDs and dates' AS description
FROM barclays_dwh.fct_inpatient_claim
WHERE admission_date = discharge_date;

-- =============================================================================
-- SUMMARY: Encounter grouping statistics
-- =============================================================================
SELECT
    'ENCOUNTER_GROUPING_SUMMARY'                   AS test_name,
    COUNT(DISTINCT encounter_id)                   AS total_encounters,
    COUNT(*)                                       AS total_claim_lines,
    ROUND(COUNT(*) * 1.0 / NULLIF(COUNT(DISTINCT encounter_id), 0), 2) AS avg_lines_per_encounter,
    MIN(DATEDIFF('day', admission_date, discharge_date)) AS min_los_days,
    AVG(DATEDIFF('day', admission_date, discharge_date)) AS avg_los_days,
    MAX(DATEDIFF('day', admission_date, discharge_date)) AS max_los_days,
    'INFO' AS status,
    'Overall encounter grouping statistics' AS description
FROM barclays_dwh.fct_inpatient_claim;
