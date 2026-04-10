/*******************************************************************************
 * sp_diagnosis_code_flatten
 *
 * Flattens the 25 individual ICD diagnosis code columns from the raw
 * medical claims table into a normalized row-per-diagnosis format.
 *
 * Input:  RAW_MEDICAL_CLAIM with icd_diagnosis_code_1 through _25
 * Output: Volatile table or permanent table with one row per claim per
 *         diagnosis code, with position number (1-25).
 *
 * This uses a manual UNION ALL approach since Teradata's UNPIVOT support
 * varies by version. Each UNION ALL leg picks one of the 25 columns.
 *
 * Teradata-specific features:
 *   - CREATE VOLATILE TABLE
 *   - UNION ALL for manual unpivot
 *   - QUALIFY ROW_NUMBER()
 *   - ACTIVITY_COUNT
 *   - SQLSTATE error handling
 ******************************************************************************/

REPLACE PROCEDURE CLAIMS_DWH.sp_diagnosis_code_flatten (
    IN p_start_date  DATE,
    IN p_end_date    DATE
)
BEGIN
    -- Local variable declarations
    DECLARE v_batch_id       BIGINT;
    DECLARE v_row_count      INTEGER;
    DECLARE v_sqlstate       CHAR(5);

    -- Error handler
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        SET v_sqlstate = SQLSTATE;
        INSERT INTO CLAIMS_DWH.ETL_ERROR_LOG (
            procedure_name, error_code, error_state, error_ts, batch_id
        ) VALUES (
            'sp_diagnosis_code_flatten', SQLCODE, v_sqlstate, CURRENT_TIMESTAMP, v_batch_id
        );
    END;

    -- Generate batch ID
    SET v_batch_id = CAST(
        CAST(p_start_date AS FORMAT 'YYYYMMDD') || '030' AS BIGINT
    );

    ---------------------------------------------------------------------------
    -- STEP 1: Flatten diagnosis codes using UNION ALL
    -- Each leg extracts one of the 25 ICD diagnosis code columns
    -- NULL codes are filtered out to avoid empty rows
    ---------------------------------------------------------------------------
    CREATE VOLATILE TABLE VT_DIAGNOSIS_FLAT AS (
        SELECT claim_id, claim_line_number, member_id, service_date_from,
               1 AS diagnosis_position, icd_diagnosis_code_1 AS icd_diagnosis_code,
               CASE WHEN diagnosis_position = 1 THEN 'PRIMARY' ELSE 'SECONDARY' END AS diagnosis_type
        FROM CLAIMS_RAW.RAW_MEDICAL_CLAIM
        WHERE service_date_from BETWEEN p_start_date AND p_end_date
          AND icd_diagnosis_code_1 IS NOT NULL

        UNION ALL
        SELECT claim_id, claim_line_number, member_id, service_date_from,
               2, icd_diagnosis_code_2, 'SECONDARY'
        FROM CLAIMS_RAW.RAW_MEDICAL_CLAIM
        WHERE service_date_from BETWEEN p_start_date AND p_end_date
          AND icd_diagnosis_code_2 IS NOT NULL

        UNION ALL
        SELECT claim_id, claim_line_number, member_id, service_date_from,
               3, icd_diagnosis_code_3, 'SECONDARY'
        FROM CLAIMS_RAW.RAW_MEDICAL_CLAIM
        WHERE service_date_from BETWEEN p_start_date AND p_end_date
          AND icd_diagnosis_code_3 IS NOT NULL

        UNION ALL
        SELECT claim_id, claim_line_number, member_id, service_date_from,
               4, icd_diagnosis_code_4, 'SECONDARY'
        FROM CLAIMS_RAW.RAW_MEDICAL_CLAIM
        WHERE service_date_from BETWEEN p_start_date AND p_end_date
          AND icd_diagnosis_code_4 IS NOT NULL

        UNION ALL
        SELECT claim_id, claim_line_number, member_id, service_date_from,
               5, icd_diagnosis_code_5, 'SECONDARY'
        FROM CLAIMS_RAW.RAW_MEDICAL_CLAIM
        WHERE service_date_from BETWEEN p_start_date AND p_end_date
          AND icd_diagnosis_code_5 IS NOT NULL

        UNION ALL
        SELECT claim_id, claim_line_number, member_id, service_date_from,
               6, icd_diagnosis_code_6, 'SECONDARY'
        FROM CLAIMS_RAW.RAW_MEDICAL_CLAIM
        WHERE service_date_from BETWEEN p_start_date AND p_end_date
          AND icd_diagnosis_code_6 IS NOT NULL

        UNION ALL
        SELECT claim_id, claim_line_number, member_id, service_date_from,
               7, icd_diagnosis_code_7, 'SECONDARY'
        FROM CLAIMS_RAW.RAW_MEDICAL_CLAIM
        WHERE service_date_from BETWEEN p_start_date AND p_end_date
          AND icd_diagnosis_code_7 IS NOT NULL

        UNION ALL
        SELECT claim_id, claim_line_number, member_id, service_date_from,
               8, icd_diagnosis_code_8, 'SECONDARY'
        FROM CLAIMS_RAW.RAW_MEDICAL_CLAIM
        WHERE service_date_from BETWEEN p_start_date AND p_end_date
          AND icd_diagnosis_code_8 IS NOT NULL

        UNION ALL
        SELECT claim_id, claim_line_number, member_id, service_date_from,
               9, icd_diagnosis_code_9, 'SECONDARY'
        FROM CLAIMS_RAW.RAW_MEDICAL_CLAIM
        WHERE service_date_from BETWEEN p_start_date AND p_end_date
          AND icd_diagnosis_code_9 IS NOT NULL

        UNION ALL
        SELECT claim_id, claim_line_number, member_id, service_date_from,
               10, icd_diagnosis_code_10, 'SECONDARY'
        FROM CLAIMS_RAW.RAW_MEDICAL_CLAIM
        WHERE service_date_from BETWEEN p_start_date AND p_end_date
          AND icd_diagnosis_code_10 IS NOT NULL

        UNION ALL
        SELECT claim_id, claim_line_number, member_id, service_date_from,
               11, icd_diagnosis_code_11, 'SECONDARY'
        FROM CLAIMS_RAW.RAW_MEDICAL_CLAIM
        WHERE service_date_from BETWEEN p_start_date AND p_end_date
          AND icd_diagnosis_code_11 IS NOT NULL

        UNION ALL
        SELECT claim_id, claim_line_number, member_id, service_date_from,
               12, icd_diagnosis_code_12, 'SECONDARY'
        FROM CLAIMS_RAW.RAW_MEDICAL_CLAIM
        WHERE service_date_from BETWEEN p_start_date AND p_end_date
          AND icd_diagnosis_code_12 IS NOT NULL

        UNION ALL
        SELECT claim_id, claim_line_number, member_id, service_date_from,
               13, icd_diagnosis_code_13, 'SECONDARY'
        FROM CLAIMS_RAW.RAW_MEDICAL_CLAIM
        WHERE service_date_from BETWEEN p_start_date AND p_end_date
          AND icd_diagnosis_code_13 IS NOT NULL

        UNION ALL
        SELECT claim_id, claim_line_number, member_id, service_date_from,
               14, icd_diagnosis_code_14, 'SECONDARY'
        FROM CLAIMS_RAW.RAW_MEDICAL_CLAIM
        WHERE service_date_from BETWEEN p_start_date AND p_end_date
          AND icd_diagnosis_code_14 IS NOT NULL

        UNION ALL
        SELECT claim_id, claim_line_number, member_id, service_date_from,
               15, icd_diagnosis_code_15, 'SECONDARY'
        FROM CLAIMS_RAW.RAW_MEDICAL_CLAIM
        WHERE service_date_from BETWEEN p_start_date AND p_end_date
          AND icd_diagnosis_code_15 IS NOT NULL

        UNION ALL
        SELECT claim_id, claim_line_number, member_id, service_date_from,
               16, icd_diagnosis_code_16, 'SECONDARY'
        FROM CLAIMS_RAW.RAW_MEDICAL_CLAIM
        WHERE service_date_from BETWEEN p_start_date AND p_end_date
          AND icd_diagnosis_code_16 IS NOT NULL

        UNION ALL
        SELECT claim_id, claim_line_number, member_id, service_date_from,
               17, icd_diagnosis_code_17, 'SECONDARY'
        FROM CLAIMS_RAW.RAW_MEDICAL_CLAIM
        WHERE service_date_from BETWEEN p_start_date AND p_end_date
          AND icd_diagnosis_code_17 IS NOT NULL

        UNION ALL
        SELECT claim_id, claim_line_number, member_id, service_date_from,
               18, icd_diagnosis_code_18, 'SECONDARY'
        FROM CLAIMS_RAW.RAW_MEDICAL_CLAIM
        WHERE service_date_from BETWEEN p_start_date AND p_end_date
          AND icd_diagnosis_code_18 IS NOT NULL

        UNION ALL
        SELECT claim_id, claim_line_number, member_id, service_date_from,
               19, icd_diagnosis_code_19, 'SECONDARY'
        FROM CLAIMS_RAW.RAW_MEDICAL_CLAIM
        WHERE service_date_from BETWEEN p_start_date AND p_end_date
          AND icd_diagnosis_code_19 IS NOT NULL

        UNION ALL
        SELECT claim_id, claim_line_number, member_id, service_date_from,
               20, icd_diagnosis_code_20, 'SECONDARY'
        FROM CLAIMS_RAW.RAW_MEDICAL_CLAIM
        WHERE service_date_from BETWEEN p_start_date AND p_end_date
          AND icd_diagnosis_code_20 IS NOT NULL

        UNION ALL
        SELECT claim_id, claim_line_number, member_id, service_date_from,
               21, icd_diagnosis_code_21, 'SECONDARY'
        FROM CLAIMS_RAW.RAW_MEDICAL_CLAIM
        WHERE service_date_from BETWEEN p_start_date AND p_end_date
          AND icd_diagnosis_code_21 IS NOT NULL

        UNION ALL
        SELECT claim_id, claim_line_number, member_id, service_date_from,
               22, icd_diagnosis_code_22, 'SECONDARY'
        FROM CLAIMS_RAW.RAW_MEDICAL_CLAIM
        WHERE service_date_from BETWEEN p_start_date AND p_end_date
          AND icd_diagnosis_code_22 IS NOT NULL

        UNION ALL
        SELECT claim_id, claim_line_number, member_id, service_date_from,
               23, icd_diagnosis_code_23, 'SECONDARY'
        FROM CLAIMS_RAW.RAW_MEDICAL_CLAIM
        WHERE service_date_from BETWEEN p_start_date AND p_end_date
          AND icd_diagnosis_code_23 IS NOT NULL

        UNION ALL
        SELECT claim_id, claim_line_number, member_id, service_date_from,
               24, icd_diagnosis_code_24, 'SECONDARY'
        FROM CLAIMS_RAW.RAW_MEDICAL_CLAIM
        WHERE service_date_from BETWEEN p_start_date AND p_end_date
          AND icd_diagnosis_code_24 IS NOT NULL

        UNION ALL
        SELECT claim_id, claim_line_number, member_id, service_date_from,
               25, icd_diagnosis_code_25, 'SECONDARY'
        FROM CLAIMS_RAW.RAW_MEDICAL_CLAIM
        WHERE service_date_from BETWEEN p_start_date AND p_end_date
          AND icd_diagnosis_code_25 IS NOT NULL
    ) WITH DATA
    PRIMARY INDEX (claim_id, claim_line_number, diagnosis_position)
    ON COMMIT PRESERVE ROWS;

    SET v_row_count = ACTIVITY_COUNT;

    ---------------------------------------------------------------------------
    -- STEP 2: Deduplicate - if the same diagnosis code appears in multiple
    -- positions for the same claim, keep only the lowest position
    ---------------------------------------------------------------------------
    CREATE VOLATILE TABLE VT_DIAGNOSIS_DEDUPED AS (
        SELECT
            claim_id,
            claim_line_number,
            member_id,
            service_date_from,
            diagnosis_position,
            icd_diagnosis_code,
            CASE WHEN diagnosis_position = 1 THEN 'PRIMARY'
                 WHEN diagnosis_position <= 3 THEN 'ADMITTING'
                 ELSE 'SECONDARY'
            END AS diagnosis_type,
            /* Extract the ICD-10 chapter from the first character */
            CASE
                WHEN SUBSTR(icd_diagnosis_code, 1, 1) BETWEEN 'A' AND 'B' THEN 'INFECTIOUS'
                WHEN SUBSTR(icd_diagnosis_code, 1, 1) = 'C' THEN 'NEOPLASM'
                WHEN SUBSTR(icd_diagnosis_code, 1, 1) = 'D' THEN 'BLOOD/NEOPLASM'
                WHEN SUBSTR(icd_diagnosis_code, 1, 1) = 'E' THEN 'ENDOCRINE'
                WHEN SUBSTR(icd_diagnosis_code, 1, 1) = 'F' THEN 'MENTAL'
                WHEN SUBSTR(icd_diagnosis_code, 1, 1) = 'G' THEN 'NERVOUS'
                WHEN SUBSTR(icd_diagnosis_code, 1, 1) BETWEEN 'H' AND 'H' THEN 'EYE/EAR'
                WHEN SUBSTR(icd_diagnosis_code, 1, 1) = 'I' THEN 'CIRCULATORY'
                WHEN SUBSTR(icd_diagnosis_code, 1, 1) = 'J' THEN 'RESPIRATORY'
                WHEN SUBSTR(icd_diagnosis_code, 1, 1) = 'K' THEN 'DIGESTIVE'
                WHEN SUBSTR(icd_diagnosis_code, 1, 1) = 'L' THEN 'SKIN'
                WHEN SUBSTR(icd_diagnosis_code, 1, 1) = 'M' THEN 'MUSCULOSKELETAL'
                WHEN SUBSTR(icd_diagnosis_code, 1, 1) = 'N' THEN 'GENITOURINARY'
                WHEN SUBSTR(icd_diagnosis_code, 1, 1) = 'O' THEN 'PREGNANCY'
                WHEN SUBSTR(icd_diagnosis_code, 1, 1) BETWEEN 'R' AND 'R' THEN 'SYMPTOMS'
                WHEN SUBSTR(icd_diagnosis_code, 1, 1) BETWEEN 'S' AND 'T' THEN 'INJURY'
                WHEN SUBSTR(icd_diagnosis_code, 1, 1) = 'Z' THEN 'FACTORS'
                ELSE 'OTHER'
            END AS icd_chapter_category
        FROM VT_DIAGNOSIS_FLAT
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY claim_id, claim_line_number, icd_diagnosis_code
            ORDER BY diagnosis_position ASC
        ) = 1
    ) WITH DATA
    PRIMARY INDEX (claim_id, claim_line_number, diagnosis_position)
    ON COMMIT PRESERVE ROWS;

    SET v_row_count = ACTIVITY_COUNT;

    ---------------------------------------------------------------------------
    -- STEP 3: Clean up
    ---------------------------------------------------------------------------
    DROP TABLE VT_DIAGNOSIS_FLAT;

    /* VT_DIAGNOSIS_DEDUPED is left available for downstream procedures
       to consume within the same session. It will be automatically dropped
       when the session ends. */

END;
