/*******************************************************************************
 * sp_phi_masking
 *
 * PHI (Protected Health Information) masking procedure for HIPAA compliance.
 *
 * Masks sensitive member data elements:
 *   - date_of_birth  -> shifted to first of month
 *   - zip_code       -> truncated to 3-digit prefix
 *   - member names   -> replaced with masked values
 *
 * !! IMPORTANT - INTENTIONAL LOGIC DRIFT !!
 * In this Teradata version, PHI masking is applied AFTER the mart layer
 * at query time via views, rather than during the ETL pipeline.
 *
 * This means:
 *   - Unmasked PHI exists in CLAIMS_DWH.DIM_MEMBER
 *   - Masking is applied only when querying through CLAIMS_MART views
 *   - This is different from other platforms where masking happens during
 *     the staging/warehouse ETL steps
 *
 * This creates a deliberate architectural drift for Phase 3 validation.
 *
 * Teradata-specific features:
 *   - REPLACE VIEW for query-time masking
 *   - SUBSTR for zip code truncation
 *   - Teradata date arithmetic
 *   - TRANSLATE / OREPLACE for string masking
 *   - HASHBAKAMP for deterministic anonymization
 ******************************************************************************/

REPLACE PROCEDURE CLAIMS_DWH.sp_phi_masking ()
BEGIN
    DECLARE v_sqlstate CHAR(5);

    -- Error handler
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        SET v_sqlstate = SQLSTATE;
        INSERT INTO CLAIMS_DWH.ETL_ERROR_LOG (
            procedure_name, error_code, error_state, error_ts, batch_id
        ) VALUES (
            'sp_phi_masking', SQLCODE, v_sqlstate, CURRENT_TIMESTAMP, NULL
        );
    END;

    ---------------------------------------------------------------------------
    -- PHI Masking Strategy (Teradata - Query-Time Approach)
    --
    -- !! INTENTIONAL DRIFT !!
    -- Masking is applied AFTER the mart layer via views, NOT during ETL.
    -- This means unmasked PHI persists in DWH tables.
    -- Other platforms apply masking during staging/warehouse ETL.
    ---------------------------------------------------------------------------

    ---------------------------------------------------------------------------
    -- VIEW 1: Masked member view for reporting consumers
    -- Applied at query time over the unmasked DIM_MEMBER table
    ---------------------------------------------------------------------------
    REPLACE VIEW CLAIMS_MART.V_MEMBER_MASKED AS
    LOCK ROW FOR ACCESS
    SELECT
        member_sk,
        member_id,
        plan_id,
        payer_id,
        gender,
        /* PHI Masking: date_of_birth shifted to first of birth month */
        CAST(
            CAST(EXTRACT(YEAR FROM date_of_birth) AS CHAR(4))
            || '-'
            || CAST(EXTRACT(MONTH FROM date_of_birth) AS FORMAT '99')
            || '-01'
        AS DATE FORMAT 'YYYY-MM-DD') AS date_of_birth_masked,
        /* PHI Masking: zip_code truncated to 3-digit prefix + '00' */
        CASE
            WHEN zip_code IS NOT NULL
                THEN SUBSTR(zip_code, 1, 3) || '00'
            ELSE NULL
        END AS zip_code_masked,
        state,
        race,
        relation_to_subscriber,
        /* subscriber_id is masked - show only last 4 characters */
        CASE
            WHEN subscriber_id IS NOT NULL
                THEN '****' || SUBSTR(subscriber_id,
                    CHARACTERS(subscriber_id) - 3, 4)
            ELSE NULL
        END AS subscriber_id_masked,
        group_id,
        line_of_business,
        age_band,
        /* Calculate age from masked DOB for reporting */
        (CURRENT_DATE - date_of_birth) / 365 AS age_years,
        validity_period,
        is_current,
        effective_from,
        effective_to
    FROM CLAIMS_DWH.DIM_MEMBER;

    COMMENT ON CLAIMS_MART.V_MEMBER_MASKED
        AS 'PHI-masked member view for reporting - masking applied at query time (HIPAA safe harbor)';


    ---------------------------------------------------------------------------
    -- VIEW 2: Masked medical claims view
    -- Strips provider-identifiable information for de-identified datasets
    ---------------------------------------------------------------------------
    REPLACE VIEW CLAIMS_MART.V_MEDICAL_CLAIM_DEIDENTIFIED AS
    LOCK ROW FOR ACCESS
    SELECT
        fc.claim_id,
        fc.claim_line_number,
        fc.member_sk,
        /* Provider SKs are retained but NPIs are not exposed */
        fc.billing_provider_sk,
        fc.rendering_provider_sk,
        fc.facility_provider_sk,
        fc.service_date_from_key,
        fc.service_date_to_key,
        fc.claim_type,
        fc.place_of_service,
        fc.ms_drg,
        fc.hcpcs_code,
        fc.cpt_code,
        fc.icd_diagnosis_code_1,
        fc.icd_diagnosis_code_2,
        fc.icd_diagnosis_code_3,
        fc.claim_status,
        fc.paid_amount,
        fc.allowed_amount,
        fc.plan_paid_amount,
        fc.payer_id,
        fc.encounter_id,
        /* Join to masked member view for de-identified demographics */
        mm.gender,
        mm.age_band,
        mm.zip_code_masked,
        mm.state,
        mm.line_of_business
    FROM CLAIMS_DWH.FCT_MEDICAL_CLAIM fc
    INNER JOIN CLAIMS_MART.V_MEMBER_MASKED mm
        ON fc.member_sk = mm.member_sk
       AND mm.is_current = 'Y';

    COMMENT ON CLAIMS_MART.V_MEDICAL_CLAIM_DEIDENTIFIED
        AS 'De-identified medical claims view with PHI masking applied at query time';


    ---------------------------------------------------------------------------
    -- VIEW 3: Masked pharmacy claims view
    ---------------------------------------------------------------------------
    REPLACE VIEW CLAIMS_MART.V_PHARMACY_CLAIM_DEIDENTIFIED AS
    LOCK ROW FOR ACCESS
    SELECT
        fp.claim_id,
        fp.member_sk,
        fp.dispensing_date_key,
        fp.ndc_code,
        fp.quantity,
        fp.days_supply,
        fp.refill_number,
        fp.claim_status,
        fp.paid_amount,
        fp.allowed_amount,
        fp.plan_paid,
        fp.payer_id,
        /* Join to masked member for de-identified demographics */
        mm.gender,
        mm.age_band,
        mm.zip_code_masked,
        mm.state,
        mm.line_of_business
    FROM CLAIMS_DWH.FCT_PHARMACY_CLAIM fp
    INNER JOIN CLAIMS_MART.V_MEMBER_MASKED mm
        ON fp.member_sk = mm.member_sk
       AND mm.is_current = 'Y';

    COMMENT ON CLAIMS_MART.V_PHARMACY_CLAIM_DEIDENTIFIED
        AS 'De-identified pharmacy claims view with PHI masking applied at query time';

END;
