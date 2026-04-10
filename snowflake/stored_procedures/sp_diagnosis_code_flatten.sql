/*******************************************************************************
 * Healthcare Claims - Diagnosis Code Flattening (Snowflake-specific)
 *
 * Handles the hybrid diagnosis code storage unique to Snowflake:
 *   1. VARIANT column (diagnosis_codes) - JSON array
 *   2. Individual columns (icd_diagnosis_code_1..10)
 *
 * Uses LATERAL FLATTEN to explode the VARIANT JSON array, then COALESCEs
 * with the individual columns to produce a unified, deduplicated list.
 *
 * This procedure creates/refreshes a denormalized diagnosis lookup table
 * that downstream analytics can join against.
 ******************************************************************************/

CREATE OR REPLACE PROCEDURE CLAIMS_DW.WAREHOUSE.SP_DIAGNOSIS_CODE_FLATTEN(
    P_START_DATE     DATE,
    P_END_DATE       DATE
)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
COMMENT = 'Flattens hybrid VARIANT + individual diagnosis columns using LATERAL FLATTEN'
AS
$$
DECLARE
    v_rows_affected INTEGER DEFAULT 0;
    v_result        VARCHAR DEFAULT '';
BEGIN

    -- =========================================================================
    -- Step 1: Create or replace the diagnosis lookup table
    -- =========================================================================
    CREATE TABLE IF NOT EXISTS CLAIMS_DW.WAREHOUSE.CLAIM_DIAGNOSIS_XREF
    (
        claim_id                VARCHAR(30)      NOT NULL,
        claim_line_number       INTEGER,
        member_id               VARCHAR(20)      NOT NULL,
        diagnosis_code          VARCHAR(10)      NOT NULL,
        diagnosis_position      INTEGER          NOT NULL
                                COMMENT '1-based position of the diagnosis code',
        diagnosis_source        VARCHAR(20)      NOT NULL
                                COMMENT 'VARIANT_ARRAY, INDIVIDUAL_COLUMN, or BOTH',
        start_date              DATE,
        etl_load_timestamp      TIMESTAMP_NTZ    DEFAULT CURRENT_TIMESTAMP()
    )
    CLUSTER BY (member_id, diagnosis_code)
    COMMENT = 'Flattened diagnosis code cross-reference from hybrid storage';

    -- =========================================================================
    -- Step 2: Extract diagnosis codes from VARIANT JSON array
    --         using LATERAL FLATTEN
    -- =========================================================================
    CREATE OR REPLACE TEMPORARY TABLE CLAIMS_DW.WAREHOUSE.TMP_DX_FROM_VARIANT AS
    SELECT
        mc.claim_id,
        mc.claim_line_number,
        mc.member_id,
        TRIM(f.value::VARCHAR, '"')                     AS diagnosis_code,
        f.index + 1                                     AS diagnosis_position,
        'VARIANT_ARRAY'                                 AS diagnosis_source,
        mc.start_date
    FROM CLAIMS_DW.RAW.RAW_MEDICAL_CLAIM mc,
        LATERAL FLATTEN(input => mc.diagnosis_codes, OUTER => TRUE) f
    WHERE mc.start_date BETWEEN :P_START_DATE AND :P_END_DATE
      AND mc.diagnosis_codes IS NOT NULL
      AND f.value IS NOT NULL
      AND TRIM(f.value::VARCHAR, '"') != '';

    -- =========================================================================
    -- Step 3: Extract diagnosis codes from individual columns
    --         (icd_diagnosis_code_1 through icd_diagnosis_code_10)
    -- =========================================================================
    CREATE OR REPLACE TEMPORARY TABLE CLAIMS_DW.WAREHOUSE.TMP_DX_FROM_INDIVIDUAL AS
    SELECT claim_id, claim_line_number, member_id, diagnosis_code,
           diagnosis_position, 'INDIVIDUAL_COLUMN' AS diagnosis_source, start_date
    FROM (
        SELECT claim_id, claim_line_number, member_id, start_date,
               icd_diagnosis_code_1  AS dx, 1  AS pos
        FROM CLAIMS_DW.RAW.RAW_MEDICAL_CLAIM
        WHERE start_date BETWEEN :P_START_DATE AND :P_END_DATE
        UNION ALL
        SELECT claim_id, claim_line_number, member_id, start_date,
               icd_diagnosis_code_2, 2
        FROM CLAIMS_DW.RAW.RAW_MEDICAL_CLAIM
        WHERE start_date BETWEEN :P_START_DATE AND :P_END_DATE
        UNION ALL
        SELECT claim_id, claim_line_number, member_id, start_date,
               icd_diagnosis_code_3, 3
        FROM CLAIMS_DW.RAW.RAW_MEDICAL_CLAIM
        WHERE start_date BETWEEN :P_START_DATE AND :P_END_DATE
        UNION ALL
        SELECT claim_id, claim_line_number, member_id, start_date,
               icd_diagnosis_code_4, 4
        FROM CLAIMS_DW.RAW.RAW_MEDICAL_CLAIM
        WHERE start_date BETWEEN :P_START_DATE AND :P_END_DATE
        UNION ALL
        SELECT claim_id, claim_line_number, member_id, start_date,
               icd_diagnosis_code_5, 5
        FROM CLAIMS_DW.RAW.RAW_MEDICAL_CLAIM
        WHERE start_date BETWEEN :P_START_DATE AND :P_END_DATE
        UNION ALL
        SELECT claim_id, claim_line_number, member_id, start_date,
               icd_diagnosis_code_6, 6
        FROM CLAIMS_DW.RAW.RAW_MEDICAL_CLAIM
        WHERE start_date BETWEEN :P_START_DATE AND :P_END_DATE
        UNION ALL
        SELECT claim_id, claim_line_number, member_id, start_date,
               icd_diagnosis_code_7, 7
        FROM CLAIMS_DW.RAW.RAW_MEDICAL_CLAIM
        WHERE start_date BETWEEN :P_START_DATE AND :P_END_DATE
        UNION ALL
        SELECT claim_id, claim_line_number, member_id, start_date,
               icd_diagnosis_code_8, 8
        FROM CLAIMS_DW.RAW.RAW_MEDICAL_CLAIM
        WHERE start_date BETWEEN :P_START_DATE AND :P_END_DATE
        UNION ALL
        SELECT claim_id, claim_line_number, member_id, start_date,
               icd_diagnosis_code_9, 9
        FROM CLAIMS_DW.RAW.RAW_MEDICAL_CLAIM
        WHERE start_date BETWEEN :P_START_DATE AND :P_END_DATE
        UNION ALL
        SELECT claim_id, claim_line_number, member_id, start_date,
               icd_diagnosis_code_10, 10
        FROM CLAIMS_DW.RAW.RAW_MEDICAL_CLAIM
        WHERE start_date BETWEEN :P_START_DATE AND :P_END_DATE
    ) unpivoted (claim_id, claim_line_number, member_id, start_date, dx, pos)
    WHERE dx IS NOT NULL AND TRIM(dx) != '';

    -- =========================================================================
    -- Step 4: COALESCE between VARIANT and individual columns
    --         Prefer VARIANT array when both exist; flag source as BOTH
    -- =========================================================================
    CREATE OR REPLACE TEMPORARY TABLE CLAIMS_DW.WAREHOUSE.TMP_DX_MERGED AS
    SELECT
        COALESCE(v.claim_id, i.claim_id)                AS claim_id,
        COALESCE(v.claim_line_number, i.claim_line_number) AS claim_line_number,
        COALESCE(v.member_id, i.member_id)              AS member_id,
        COALESCE(v.diagnosis_code, i.diagnosis_code)    AS diagnosis_code,
        COALESCE(v.diagnosis_position, i.diagnosis_position) AS diagnosis_position,
        CASE
            WHEN v.diagnosis_code IS NOT NULL AND i.diagnosis_code IS NOT NULL
                THEN 'BOTH'
            WHEN v.diagnosis_code IS NOT NULL
                THEN 'VARIANT_ARRAY'
            ELSE 'INDIVIDUAL_COLUMN'
        END                                             AS diagnosis_source,
        COALESCE(v.start_date, i.start_date)            AS start_date
    FROM CLAIMS_DW.WAREHOUSE.TMP_DX_FROM_VARIANT v
    FULL OUTER JOIN CLAIMS_DW.WAREHOUSE.TMP_DX_FROM_INDIVIDUAL i
        ON  v.claim_id = i.claim_id
        AND v.claim_line_number = i.claim_line_number
        AND v.diagnosis_position = i.diagnosis_position;

    -- =========================================================================
    -- Step 5: Merge into the cross-reference table
    -- =========================================================================
    -- Delete existing records for the date range to avoid duplicates
    DELETE FROM CLAIMS_DW.WAREHOUSE.CLAIM_DIAGNOSIS_XREF
    WHERE start_date BETWEEN :P_START_DATE AND :P_END_DATE;

    INSERT INTO CLAIMS_DW.WAREHOUSE.CLAIM_DIAGNOSIS_XREF
        (claim_id, claim_line_number, member_id, diagnosis_code,
         diagnosis_position, diagnosis_source, start_date, etl_load_timestamp)
    SELECT DISTINCT
        claim_id, claim_line_number, member_id, diagnosis_code,
        diagnosis_position, diagnosis_source, start_date, CURRENT_TIMESTAMP()
    FROM CLAIMS_DW.WAREHOUSE.TMP_DX_MERGED
    WHERE diagnosis_code IS NOT NULL;

    LET v_rows_affected := SQLROWCOUNT;

    -- Clean up temp tables
    DROP TABLE IF EXISTS CLAIMS_DW.WAREHOUSE.TMP_DX_FROM_VARIANT;
    DROP TABLE IF EXISTS CLAIMS_DW.WAREHOUSE.TMP_DX_FROM_INDIVIDUAL;
    DROP TABLE IF EXISTS CLAIMS_DW.WAREHOUSE.TMP_DX_MERGED;

    v_result := 'Diagnosis code flatten completed. Rows inserted: ' || v_rows_affected::VARCHAR;
    RETURN v_result;

EXCEPTION
    WHEN OTHER THEN
        DROP TABLE IF EXISTS CLAIMS_DW.WAREHOUSE.TMP_DX_FROM_VARIANT;
        DROP TABLE IF EXISTS CLAIMS_DW.WAREHOUSE.TMP_DX_FROM_INDIVIDUAL;
        DROP TABLE IF EXISTS CLAIMS_DW.WAREHOUSE.TMP_DX_MERGED;
        RETURN 'ERROR in SP_DIAGNOSIS_CODE_FLATTEN: ' || SQLERRM;
END;
$$;
