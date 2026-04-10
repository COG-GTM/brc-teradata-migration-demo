/*******************************************************************************
 * Healthcare Claims Data Warehouse - Staging Views (Snowflake)
 *
 * Snowflake-specific features used:
 *   - QUALIFY ROW_NUMBER() - native in Snowflake (no subquery needed)
 *   - CREATE OR REPLACE VIEW with COPY GRANTS
 *   - COALESCE, IFF, TRY_CAST
 *
 * IMPORTANT - ADR Dedup Priority (Snowflake-specific drift):
 *   PAID = 1, ADJUSTED = 2, REVERSED = 3, DENIED = 4
 *
 *   Teradata:    PAID=1, ADJUSTED=2, DENIED=3, REVERSED=4
 *   Databricks:  PAID=1, DENIED=2, ADJUSTED=3, REVERSED=4
 *   Snowflake:   PAID=1, ADJUSTED=2, REVERSED=3, DENIED=4
 *
 *   This INTENTIONAL difference puts Reversed before Denied,
 *   creating a subtle but real cross-platform drift.
 ******************************************************************************/

USE DATABASE CLAIMS_DW;
USE SCHEMA STAGING;

-- =============================================================================
-- V_MEMBER_LATEST: deduplicated member eligibility (latest record per member)
-- Uses QUALIFY ROW_NUMBER() - native in Snowflake
-- =============================================================================
CREATE OR REPLACE VIEW CLAIMS_DW.STAGING.V_MEMBER_LATEST
    COPY GRANTS
    COMMENT = 'Latest member eligibility record - deduped using QUALIFY ROW_NUMBER()'
AS
SELECT
    member_id,
    subscriber_id,
    person_number,
    first_name,
    last_name,
    date_of_birth,
    gender,
    ssn_encrypted,
    address_line_1,
    address_line_2,
    city,
    state_code,
    zip_code,
    phone_number,
    email,
    plan_code,
    plan_name,
    product_type,
    line_of_business,
    group_number,
    group_name,
    eligibility_start_date,
    eligibility_end_date,
    pcp_provider_id,
    pcp_provider_name,
    coverage_type,
    relationship_code,
    cobra_flag,
    DATEDIFF('year', date_of_birth, CURRENT_DATE())    AS member_age,
    CASE
        WHEN eligibility_end_date IS NULL
             OR eligibility_end_date >= CURRENT_DATE()   THEN TRUE
        ELSE FALSE
    END                                                  AS is_currently_eligible,
    source_system,
    load_timestamp
FROM CLAIMS_DW.RAW.RAW_MEMBER_ELIGIBILITY
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY member_id
    ORDER BY load_timestamp DESC, eligibility_start_date DESC
) = 1;


-- =============================================================================
-- V_MEDICAL_CLAIM_CURRENT: ADR-deduped medical claims
--
-- ADR Priority (SNOWFLAKE-SPECIFIC - DIFFERENT FROM BOTH TD AND DATABRICKS):
--   PAID     = 1   (highest priority - keep paid version)
--   ADJUSTED = 2
--   REVERSED = 3   << NOTE: Reversed before Denied (drift!)
--   DENIED   = 4
-- =============================================================================
CREATE OR REPLACE VIEW CLAIMS_DW.STAGING.V_MEDICAL_CLAIM_CURRENT
    COPY GRANTS
    COMMENT = 'ADR-deduped medical claims - priority: PAID>ADJUSTED>REVERSED>DENIED (Snowflake-specific order)'
AS
SELECT
    claim_id,
    claim_line_number,
    member_id,
    subscriber_id,
    start_date,
    end_date,
    admission_date,
    discharge_date,
    status_code,
    claim_type,
    place_of_service,
    bill_type,
    rendering_provider_id,
    rendering_provider_npi,
    billing_provider_id,
    billing_provider_npi,
    facility_id,
    cpt_code,
    cpt_modifier_1,
    cpt_modifier_2,
    revenue_code,
    drg_code,
    -- Hybrid diagnosis code columns
    diagnosis_codes,
    icd_diagnosis_code_1,
    icd_diagnosis_code_2,
    icd_diagnosis_code_3,
    icd_diagnosis_code_4,
    icd_diagnosis_code_5,
    icd_diagnosis_code_6,
    icd_diagnosis_code_7,
    icd_diagnosis_code_8,
    icd_diagnosis_code_9,
    icd_diagnosis_code_10,
    -- Financial
    billed_amount,
    allowed_amount,
    paid_amount,
    net_paid_amount,
    copay_amount,
    coinsurance_amount,
    deductible_amount,
    cob_amount,
    withhold_amount,
    units,
    original_claim_id,
    adjustment_reason_code,
    adjudication_date,
    -- Derived ADR priority
    CASE UPPER(status_code)
        WHEN 'PAID'     THEN 1
        WHEN 'ADJUSTED' THEN 2
        WHEN 'REVERSED' THEN 3
        WHEN 'DENIED'   THEN 4
        ELSE 99
    END                                                  AS adr_priority,
    source_system,
    load_timestamp
FROM CLAIMS_DW.RAW.RAW_MEDICAL_CLAIM
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY COALESCE(original_claim_id, claim_id), claim_line_number
    ORDER BY
        -- ADR priority: PAID=1, ADJUSTED=2, REVERSED=3, DENIED=4
        CASE UPPER(status_code)
            WHEN 'PAID'     THEN 1
            WHEN 'ADJUSTED' THEN 2
            WHEN 'REVERSED' THEN 3
            WHEN 'DENIED'   THEN 4
            ELSE 99
        END ASC,
        adjudication_date DESC NULLS LAST,
        load_timestamp DESC
) = 1;


-- =============================================================================
-- V_PHARMACY_CLAIM_CURRENT: ADR-deduped pharmacy claims
-- Uses the same Snowflake-specific ADR priority as medical claims
-- =============================================================================
CREATE OR REPLACE VIEW CLAIMS_DW.STAGING.V_PHARMACY_CLAIM_CURRENT
    COPY GRANTS
    COMMENT = 'ADR-deduped pharmacy claims - priority: PAID>ADJUSTED>REVERSED>DENIED'
AS
SELECT
    claim_id,
    member_id,
    subscriber_id,
    fill_date,
    written_date,
    status_code,
    ndc_code,
    drug_name,
    generic_name,
    therapeutic_class_code,
    therapeutic_class_desc,
    gpi_code,
    dea_schedule,
    prescribing_provider_id,
    prescribing_provider_npi,
    pharmacy_id,
    pharmacy_npi,
    pharmacy_name,
    mail_order_flag,
    quantity_dispensed,
    days_supply,
    refill_number,
    daw_code,
    compound_flag,
    formulary_flag,
    prior_auth_flag,
    billed_amount,
    allowed_amount,
    paid_amount,
    net_paid_amount,
    copay_amount,
    coinsurance_amount,
    deductible_amount,
    ingredient_cost,
    dispensing_fee,
    sales_tax,
    original_claim_id,
    icd_diagnosis_code,
    source_system,
    load_timestamp
FROM CLAIMS_DW.RAW.RAW_PHARMACY_CLAIM
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY COALESCE(original_claim_id, claim_id)
    ORDER BY
        CASE UPPER(status_code)
            WHEN 'PAID'     THEN 1
            WHEN 'ADJUSTED' THEN 2
            WHEN 'REVERSED' THEN 3
            WHEN 'DENIED'   THEN 4
            ELSE 99
        END ASC,
        load_timestamp DESC
) = 1;
