/*******************************************************************************
 * sp_claims_adr_dedup
 *
 * ADR (Adjustment/Denial/Reversal) deduplication procedure for medical claims.
 *
 * When a claim is adjusted, denied, or reversed, the payer sends a new
 * claim record. This procedure deduplicates claims to keep only the most
 * current version using a priority-based approach.
 *
 * !! IMPORTANT - INTENTIONAL LOGIC DRIFT !!
 * The ADR priority order in this Teradata version is:
 *   PAID=1, DENIED=2, ADJUSTED=3, REVERSED=4
 *
 * This is INTENTIONALLY WRONG per CMS standards. The correct order should be:
 *   PAID=1, ADJUSTED=2, DENIED=3, REVERSED=4
 *
 * Denied should NOT be prioritized over Adjusted. This creates a deliberate
 * logic drift for Phase 3 migration validation to detect.
 *
 * Teradata-specific features:
 *   - CREATE VOLATILE TABLE
 *   - QUALIFY ROW_NUMBER()
 *   - SQLSTATE error handling
 *   - ACTIVITY_COUNT
 *   - LOCK ROW FOR ACCESS
 ******************************************************************************/

REPLACE PROCEDURE CLAIMS_DWH.sp_claims_adr_dedup (
    IN p_business_date DATE,
    IN p_claim_type    VARCHAR(5)   /* 'I', 'P', 'O', or 'ALL' */
)
BEGIN
    -- Local variable declarations
    DECLARE v_batch_id       BIGINT;
    DECLARE v_row_count      INTEGER;
    DECLARE v_dedup_count    INTEGER;
    DECLARE v_sqlstate       CHAR(5);

    -- Error handler
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        SET v_sqlstate = SQLSTATE;
        INSERT INTO CLAIMS_DWH.ETL_ERROR_LOG (
            procedure_name, error_code, error_state, error_ts, batch_id
        ) VALUES (
            'sp_claims_adr_dedup', SQLCODE, v_sqlstate, CURRENT_TIMESTAMP, v_batch_id
        );
    END;

    -- Generate batch ID from timestamp
    SET v_batch_id = CAST(
        CAST(p_business_date AS FORMAT 'YYYYMMDD') || '010' AS BIGINT
    );

    ---------------------------------------------------------------------------
    -- STEP 1: Create volatile table with deduped claims
    -- Uses QUALIFY ROW_NUMBER() with ADR priority ordering
    --
    -- !! INTENTIONAL BUG: DENIED (2) is prioritized over ADJUSTED (3) !!
    -- Correct CMS standard: PAID > ADJUSTED > DENIED > REVERSED
    -- This version:         PAID > DENIED > ADJUSTED > REVERSED
    ---------------------------------------------------------------------------
    CREATE VOLATILE TABLE VT_DEDUPED_CLAIMS AS (
        SELECT
            claim_id,
            member_id,
            claim_line_number,
            claim_type,
            service_date_from,
            service_date_to,
            admission_date,
            discharge_date,
            admit_type,
            admit_source,
            discharge_disposition,
            place_of_service,
            bill_type,
            revenue_center_code,
            ms_drg,
            apr_drg,
            hcpcs_code,
            cpt_code,
            icd_diagnosis_code_1,
            icd_diagnosis_code_2,
            icd_diagnosis_code_3,
            icd_diagnosis_code_4,
            icd_diagnosis_code_5,
            npi,
            billing_npi,
            rendering_npi,
            facility_npi,
            ZEROIFNULL(paid_amount)    AS paid_amount,
            ZEROIFNULL(charge_amount)  AS charge_amount,
            ZEROIFNULL(allowed_amount) AS allowed_amount,
            ZEROIFNULL(coinsurance)    AS coinsurance,
            ZEROIFNULL(copay)          AS copay,
            ZEROIFNULL(deductible)     AS deductible,
            claim_status,
            adjustment_type,
            original_claim_id,
            payer_id,
            last_updated_ts
        FROM CLAIMS_RAW.RAW_MEDICAL_CLAIM
        WHERE (p_claim_type = 'ALL' OR claim_type = p_claim_type)
          AND service_date_from <= p_business_date
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY COALESCE(original_claim_id, claim_id), claim_line_number
            ORDER BY
                /* !! INTENTIONAL WRONG PRIORITY ORDER !!
                   DENIED (2) should be AFTER ADJUSTED per CMS standards.
                   This is deliberate logic drift for migration detection. */
                CASE claim_status
                    WHEN 'PAID'     THEN 1
                    WHEN 'DENIED'   THEN 2   /* WRONG: should be 3 */
                    WHEN 'ADJUSTED' THEN 3   /* WRONG: should be 2 */
                    WHEN 'REVERSED' THEN 4
                    ELSE 5
                END ASC,
                last_updated_ts DESC
        ) = 1
    ) WITH DATA
    PRIMARY INDEX (claim_id, claim_line_number)
    ON COMMIT PRESERVE ROWS;

    SET v_dedup_count = ACTIVITY_COUNT;

    ---------------------------------------------------------------------------
    -- STEP 2: Delete existing records for the affected claim IDs
    ---------------------------------------------------------------------------
    DELETE FROM CLAIMS_DWH.FCT_MEDICAL_CLAIM
    WHERE claim_id IN (
        SELECT claim_id FROM VT_DEDUPED_CLAIMS
    );

    ---------------------------------------------------------------------------
    -- STEP 3: Insert deduped claims into the fact table
    ---------------------------------------------------------------------------
    INSERT INTO CLAIMS_DWH.FCT_MEDICAL_CLAIM (
        claim_id, claim_line_number, member_sk,
        billing_provider_sk, rendering_provider_sk, facility_provider_sk,
        service_date_from_key, service_date_to_key,
        admission_date_key, discharge_date_key,
        claim_type, place_of_service, bill_type,
        revenue_center_code, ms_drg, apr_drg, hcpcs_code, cpt_code,
        icd_diagnosis_code_1, icd_diagnosis_code_2, icd_diagnosis_code_3,
        admit_type, discharge_disposition, claim_status,
        paid_amount, charge_amount, allowed_amount,
        coinsurance, copay, deductible,
        plan_paid_amount, member_oop,
        payer_id, etl_batch_id, etl_loaded_ts
    )
    SELECT
        vt.claim_id,
        vt.claim_line_number,
        dm.member_sk,
        bp.provider_sk,
        rp.provider_sk,
        fp.provider_sk,
        CAST(CAST(vt.service_date_from AS FORMAT 'YYYYMMDD') AS INTEGER),
        CAST(CAST(vt.service_date_to AS FORMAT 'YYYYMMDD') AS INTEGER),
        CAST(CAST(vt.admission_date AS FORMAT 'YYYYMMDD') AS INTEGER),
        CAST(CAST(vt.discharge_date AS FORMAT 'YYYYMMDD') AS INTEGER),
        vt.claim_type,
        vt.place_of_service,
        vt.bill_type,
        vt.revenue_center_code,
        vt.ms_drg,
        vt.apr_drg,
        vt.hcpcs_code,
        vt.cpt_code,
        vt.icd_diagnosis_code_1,
        vt.icd_diagnosis_code_2,
        vt.icd_diagnosis_code_3,
        vt.admit_type,
        vt.discharge_disposition,
        vt.claim_status,
        vt.paid_amount,
        vt.charge_amount,
        vt.allowed_amount,
        vt.coinsurance,
        vt.copay,
        vt.deductible,
        vt.allowed_amount - vt.coinsurance - vt.copay - vt.deductible,
        vt.coinsurance + vt.copay + vt.deductible,
        vt.payer_id,
        v_batch_id,
        CURRENT_TIMESTAMP(6)
    FROM VT_DEDUPED_CLAIMS vt
    INNER JOIN CLAIMS_DWH.DIM_MEMBER dm
        ON vt.member_id = dm.member_id AND dm.is_current = 'Y'
    LEFT JOIN CLAIMS_DWH.DIM_PROVIDER bp
        ON vt.billing_npi = bp.npi AND bp.is_current = 'Y'
    LEFT JOIN CLAIMS_DWH.DIM_PROVIDER rp
        ON vt.rendering_npi = rp.npi AND rp.is_current = 'Y'
    LEFT JOIN CLAIMS_DWH.DIM_PROVIDER fp
        ON vt.facility_npi = fp.npi AND fp.is_current = 'Y';

    SET v_row_count = ACTIVITY_COUNT;

    ---------------------------------------------------------------------------
    -- STEP 4: Collect statistics on modified table
    ---------------------------------------------------------------------------
    COLLECT STATISTICS ON CLAIMS_DWH.FCT_MEDICAL_CLAIM
        COLUMN (claim_id, claim_line_number);
    COLLECT STATISTICS ON CLAIMS_DWH.FCT_MEDICAL_CLAIM
        COLUMN (member_sk);
    COLLECT STATISTICS ON CLAIMS_DWH.FCT_MEDICAL_CLAIM
        COLUMN (service_date_from_key);

    ---------------------------------------------------------------------------
    -- STEP 5: Clean up volatile table
    ---------------------------------------------------------------------------
    DROP TABLE VT_DEDUPED_CLAIMS;

END;
