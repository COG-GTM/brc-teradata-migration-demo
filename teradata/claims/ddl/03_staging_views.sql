/*******************************************************************************
 * Healthcare Claims Teradata Migration Demo - Staging Views
 *
 * Teradata-specific features used:
 *   - REPLACE VIEW
 *   - QUALIFY ROW_NUMBER()
 *   - ZEROIFNULL / NULLIFZERO
 *   - LOCK ROW FOR ACCESS (dirty reads for performance)
 *   - Teradata date arithmetic (date - date = integer days)
 *   - CASESPECIFIC / NOT CASESPECIFIC
 *
 * Views:
 *   V_MEMBER_LATEST           - deduplicated member eligibility (latest record)
 *   V_MEDICAL_CLAIM_CURRENT   - current medical claims with ADR dedup logic
 *   V_PHARMACY_CLAIM_CURRENT  - current pharmacy claims with ADR dedup logic
 ******************************************************************************/

DATABASE CLAIMS_STG;

-- =============================================================================
-- V_MEMBER_LATEST: deduplicated member eligibility (latest record per member)
-- Uses QUALIFY ROW_NUMBER() to pick the most recent enrollment record
-- =============================================================================
REPLACE VIEW CLAIMS_STG.V_MEMBER_LATEST AS
LOCK ROW FOR ACCESS
SELECT
    member_id,
    plan_id,
    enrollment_start_date,
    enrollment_end_date,
    payer_id,
    gender,
    date_of_birth,
    race,
    zip_code,
    state,
    relation_to_subscriber,
    subscriber_id,
    group_id,
    line_of_business,
    /* Teradata date arithmetic: date - date yields integer days */
    CASE
        WHEN enrollment_end_date IS NOT NULL
            THEN (enrollment_end_date - enrollment_start_date)
        ELSE (CURRENT_DATE - enrollment_start_date)
    END AS enrollment_duration_days,
    CASE
        WHEN enrollment_end_date IS NULL OR enrollment_end_date >= CURRENT_DATE
            THEN 'ACTIVE'
        ELSE 'TERMINATED'
    END AS derived_enrollment_status,
    last_updated_ts
FROM CLAIMS_RAW.RAW_MEMBER_ELIGIBILITY
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY member_id
    ORDER BY enrollment_start_date DESC, last_updated_ts DESC
) = 1;

COMMENT ON CLAIMS_STG.V_MEMBER_LATEST
    AS 'Latest member eligibility record - deduped using QUALIFY ROW_NUMBER()';


-- =============================================================================
-- V_MEDICAL_CLAIM_CURRENT: current medical claims with ADR dedup logic
-- ADR priority: PAID > ADJUSTED > DENIED > REVERSED
-- Uses QUALIFY ROW_NUMBER() with CASE-based priority ordering
-- =============================================================================
REPLACE VIEW CLAIMS_STG.V_MEDICAL_CLAIM_CURRENT AS
LOCK ROW FOR ACCESS
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
    /* Derive net paid (allowed minus member responsibility) */
    ZEROIFNULL(allowed_amount) - ZEROIFNULL(coinsurance)
        - ZEROIFNULL(copay) - ZEROIFNULL(deductible) AS plan_paid_amount,
    last_updated_ts
FROM CLAIMS_RAW.RAW_MEDICAL_CLAIM
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY COALESCE(original_claim_id, claim_id), claim_line_number
    ORDER BY
        CASE claim_status
            WHEN 'PAID'     THEN 1
            WHEN 'ADJUSTED' THEN 2
            WHEN 'DENIED'   THEN 3
            WHEN 'REVERSED' THEN 4
            ELSE 5
        END ASC,
        last_updated_ts DESC
) = 1;

COMMENT ON CLAIMS_STG.V_MEDICAL_CLAIM_CURRENT
    AS 'Current medical claims with ADR dedup - priority: PAID > ADJUSTED > DENIED > REVERSED';


-- =============================================================================
-- V_PHARMACY_CLAIM_CURRENT: current pharmacy claims with ADR dedup logic
-- Same ADR priority pattern as medical claims
-- =============================================================================
REPLACE VIEW CLAIMS_STG.V_PHARMACY_CLAIM_CURRENT AS
LOCK ROW FOR ACCESS
SELECT
    claim_id,
    member_id,
    dispensing_date,
    ndc_code,
    ZEROIFNULL(quantity)       AS quantity,
    days_supply,
    refill_number,
    dispensing_npi,
    prescribing_npi,
    ZEROIFNULL(paid_amount)    AS paid_amount,
    ZEROIFNULL(charge_amount)  AS charge_amount,
    ZEROIFNULL(allowed_amount) AS allowed_amount,
    ZEROIFNULL(copay)          AS copay,
    ZEROIFNULL(coinsurance)    AS coinsurance,
    ZEROIFNULL(deductible)     AS deductible,
    ZEROIFNULL(plan_paid)      AS plan_paid,
    claim_status,
    payer_id,
    /* Derive member out-of-pocket */
    ZEROIFNULL(copay) + ZEROIFNULL(coinsurance) + ZEROIFNULL(deductible) AS member_oop,
    last_updated_ts
FROM CLAIMS_RAW.RAW_PHARMACY_CLAIM
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY claim_id
    ORDER BY
        CASE claim_status
            WHEN 'PAID'     THEN 1
            WHEN 'ADJUSTED' THEN 2
            WHEN 'DENIED'   THEN 3
            WHEN 'REVERSED' THEN 4
            ELSE 5
        END ASC,
        last_updated_ts DESC
) = 1;

COMMENT ON CLAIMS_STG.V_PHARMACY_CLAIM_CURRENT
    AS 'Current pharmacy claims with ADR dedup - priority: PAID > ADJUSTED > DENIED > REVERSED';
