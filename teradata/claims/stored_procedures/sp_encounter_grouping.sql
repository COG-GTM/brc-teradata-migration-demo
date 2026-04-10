/*******************************************************************************
 * sp_encounter_grouping
 *
 * Groups medical claims into encounters (episodes of care) by merging
 * overlapping service date ranges for the same member.
 *
 * !! IMPORTANT - INTENTIONAL LOGIC DRIFT !!
 * This Teradata version uses a NAIVE approach:
 *   - Simple GROUP BY member_id, service_date_from
 *   - Does NOT perform true overlap detection of date ranges
 *
 * The correct approach should use NORMALIZE ON PERIOD to merge overlapping
 * date ranges. This naive approach will miss encounters that span multiple
 * days with different start dates but overlapping ranges.
 *
 * This creates a deliberate logic drift for Phase 3 migration validation
 * to detect.
 *
 * Teradata-specific features:
 *   - NORMALIZE ON PERIOD
 *   - PERIOD data types
 *   - CREATE VOLATILE TABLE
 *   - QUALIFY ROW_NUMBER()
 *   - ACTIVITY_COUNT
 ******************************************************************************/

REPLACE PROCEDURE CLAIMS_DWH.sp_encounter_grouping (
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
            'sp_encounter_grouping', SQLCODE, v_sqlstate, CURRENT_TIMESTAMP, v_batch_id
        );
    END;

    -- Generate batch ID
    SET v_batch_id = CAST(
        CAST(p_start_date AS FORMAT 'YYYYMMDD') || '020' AS BIGINT
    );

    ---------------------------------------------------------------------------
    -- STEP 1: Build encounter groups using NAIVE approach
    --
    -- !! INTENTIONAL LOGIC DRIFT !!
    -- This uses a simple GROUP BY member_id, service_date_from which does
    -- NOT properly merge overlapping date ranges.
    --
    -- Example of what this misses:
    --   Claim A: member_id=1, service_date_from=2024-01-05, service_date_to=2024-01-10
    --   Claim B: member_id=1, service_date_from=2024-01-08, service_date_to=2024-01-12
    -- These should be ONE encounter (overlapping), but the naive GROUP BY
    -- treats them as separate because service_date_from differs.
    --
    -- Correct approach would use:
    --   NORMALIZE ON PERIOD(service_date_from, service_date_to + 1)
    -- to properly merge overlapping date ranges.
    ---------------------------------------------------------------------------
    CREATE VOLATILE TABLE VT_ENCOUNTER_GROUPS AS (
        SELECT
            dm.member_sk,
            fc.member_id,
            fc.service_date_from,
            /* Naive: just take the max service_date_to for the same start date */
            MAX(fc.service_date_to)  AS encounter_end_date,
            fc.claim_type,
            MIN(fc.claim_id)         AS first_claim_id,
            /* Build encounter_id from member + date (naive key) */
            fc.member_id || '_' || CAST(CAST(fc.service_date_from AS FORMAT 'YYYYMMDD') AS VARCHAR(8))
                AS encounter_id,
            COUNT(DISTINCT fc.claim_id)       AS claim_count,
            COUNT(*)                          AS claim_line_count,
            SUM(ZEROIFNULL(fc.paid_amount))   AS total_paid_amount,
            SUM(ZEROIFNULL(fc.charge_amount)) AS total_charge_amount,
            SUM(ZEROIFNULL(fc.allowed_amount)) AS total_allowed_amount,
            SUM(ZEROIFNULL(fc.coinsurance) + ZEROIFNULL(fc.copay)
                + ZEROIFNULL(fc.deductible))  AS total_member_oop
        FROM CLAIMS_STG.V_MEDICAL_CLAIM_CURRENT fc
        INNER JOIN CLAIMS_DWH.DIM_MEMBER dm
            ON fc.member_id = dm.member_id AND dm.is_current = 'Y'
        WHERE fc.service_date_from BETWEEN p_start_date AND p_end_date
        /* !! NAIVE: GROUP BY service_date_from instead of proper overlap merge !! */
        GROUP BY
            dm.member_sk,
            fc.member_id,
            fc.service_date_from,
            fc.claim_type
    ) WITH DATA
    PRIMARY INDEX (encounter_id)
    ON COMMIT PRESERVE ROWS;

    SET v_row_count = ACTIVITY_COUNT;

    ---------------------------------------------------------------------------
    -- STEP 2: Determine encounter type based on claim type
    ---------------------------------------------------------------------------
    CREATE VOLATILE TABLE VT_ENCOUNTER_TYPED AS (
        SELECT
            eg.encounter_id,
            eg.member_sk,
            eg.service_date_from     AS encounter_start_date,
            eg.encounter_end_date,
            PERIOD(eg.service_date_from,
                   COALESCE(eg.encounter_end_date, eg.service_date_from) + 1)
                AS encounter_period,
            CASE eg.claim_type
                WHEN 'I' THEN 'INPATIENT'
                WHEN 'O' THEN 'OUTPATIENT'
                WHEN 'P' THEN 'PROFESSIONAL'
                ELSE 'OUTPATIENT'
            END AS encounter_type,
            eg.claim_count,
            eg.claim_line_count,
            eg.total_paid_amount,
            eg.total_charge_amount,
            eg.total_allowed_amount,
            eg.total_member_oop,
            CASE
                WHEN eg.claim_type = 'I' AND eg.encounter_end_date IS NOT NULL
                    THEN (eg.encounter_end_date - eg.service_date_from)
                ELSE 0
            END AS length_of_stay,
            eg.first_claim_id
        FROM VT_ENCOUNTER_GROUPS eg
    ) WITH DATA
    PRIMARY INDEX (encounter_id)
    ON COMMIT PRESERVE ROWS;

    ---------------------------------------------------------------------------
    -- STEP 3: Look up primary diagnosis and DRG from the first claim
    ---------------------------------------------------------------------------
    CREATE VOLATILE TABLE VT_ENCOUNTER_ENRICHED AS (
        SELECT
            et.encounter_id,
            et.member_sk,
            et.encounter_start_date,
            et.encounter_end_date,
            et.encounter_period,
            et.encounter_type,
            mc.icd_diagnosis_code_1  AS primary_diagnosis_code,
            mc.ms_drg                AS primary_ms_drg,
            fp.provider_sk           AS facility_provider_sk,
            rp.provider_sk           AS attending_provider_sk,
            et.claim_count,
            et.claim_line_count,
            et.total_paid_amount,
            et.total_charge_amount,
            et.total_allowed_amount,
            et.total_member_oop,
            et.length_of_stay,
            mc.payer_id
        FROM VT_ENCOUNTER_TYPED et
        LEFT JOIN CLAIMS_RAW.RAW_MEDICAL_CLAIM mc
            ON et.first_claim_id = mc.claim_id
           AND mc.claim_line_number = 1
        LEFT JOIN CLAIMS_DWH.DIM_PROVIDER fp
            ON mc.facility_npi = fp.npi AND fp.is_current = 'Y'
        LEFT JOIN CLAIMS_DWH.DIM_PROVIDER rp
            ON mc.rendering_npi = rp.npi AND rp.is_current = 'Y'
    ) WITH DATA
    PRIMARY INDEX (encounter_id)
    ON COMMIT PRESERVE ROWS;

    ---------------------------------------------------------------------------
    -- STEP 4: Delete existing encounters for the date range and re-insert
    ---------------------------------------------------------------------------
    DELETE FROM CLAIMS_DWH.FCT_ENCOUNTER
    WHERE encounter_start_date BETWEEN p_start_date AND p_end_date;

    INSERT INTO CLAIMS_DWH.FCT_ENCOUNTER (
        encounter_id, member_sk,
        encounter_start_date, encounter_end_date, encounter_period,
        encounter_type, primary_diagnosis_code, primary_ms_drg,
        facility_provider_sk, attending_provider_sk,
        claim_count, claim_line_count,
        total_paid_amount, total_charge_amount, total_allowed_amount,
        total_member_oop, length_of_stay, payer_id,
        etl_batch_id, etl_loaded_ts
    )
    SELECT
        encounter_id, member_sk,
        encounter_start_date, encounter_end_date, encounter_period,
        encounter_type, primary_diagnosis_code, primary_ms_drg,
        facility_provider_sk, attending_provider_sk,
        claim_count, claim_line_count,
        total_paid_amount, total_charge_amount, total_allowed_amount,
        total_member_oop, length_of_stay, payer_id,
        v_batch_id, CURRENT_TIMESTAMP(6)
    FROM VT_ENCOUNTER_ENRICHED;

    SET v_row_count = ACTIVITY_COUNT;

    ---------------------------------------------------------------------------
    -- STEP 5: Update encounter_id on the medical claim fact table
    ---------------------------------------------------------------------------
    UPDATE fc
    FROM CLAIMS_DWH.FCT_MEDICAL_CLAIM fc,
         VT_ENCOUNTER_ENRICHED ee
    SET encounter_id = ee.encounter_id
    WHERE fc.member_sk = ee.member_sk
      AND CAST(CAST(ee.encounter_start_date AS FORMAT 'YYYYMMDD') AS INTEGER)
          = fc.service_date_from_key;

    ---------------------------------------------------------------------------
    -- STEP 6: Collect statistics and clean up
    ---------------------------------------------------------------------------
    COLLECT STATISTICS ON CLAIMS_DWH.FCT_ENCOUNTER COLUMN (encounter_id);
    COLLECT STATISTICS ON CLAIMS_DWH.FCT_ENCOUNTER COLUMN (member_sk);

    DROP TABLE VT_ENCOUNTER_ENRICHED;
    DROP TABLE VT_ENCOUNTER_TYPED;
    DROP TABLE VT_ENCOUNTER_GROUPS;

END;
