-- ============================================================================
-- Databricks Healthcare Claims Data Warehouse - Staging Tables
-- ============================================================================
-- Staging layer tables are materialized (not views) and contain cleansed,
-- deduplicated, and PHI-masked data.
--
-- IMPORTANT: PHI masking is applied at the STAGING layer (before warehouse).
-- This is different from Teradata, which masks PHI after marts. This design
-- ensures no downstream consumer ever sees unmasked PII/PHI data.
--
-- ADR (Adjustment/Denial/Reversal) deduplication uses CORRECT CMS priority:
--   PAID=1, ADJUSTED=2, DENIED=3, REVERSED=4
-- This keeps the most financially relevant version of each claim.
--
-- Platform: Databricks SQL / Delta Lake
-- ============================================================================

USE SCHEMA claims_staging;

-- ---------------------------------------------------------------------------
-- Table: stg_member_latest
-- Purpose: Latest/current version of each member's eligibility record.
--          Deduplicates by member_id keeping the most recent coverage period.
--          PHI fields are masked using SHA-256 hashing and truncation.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS stg_member_latest (
    member_id                   STRING          NOT NULL    COMMENT 'Unique member identifier',
    member_first_name_masked    STRING                      COMMENT 'SHA-256 masked member first name',
    member_last_name_masked     STRING                      COMMENT 'SHA-256 masked member last name',
    date_of_birth_masked        DATE                        COMMENT 'Masked date of birth (year + Jan 1)',
    gender                      STRING                      COMMENT 'Member gender code (M/F/U)',
    ssn_masked                  STRING                      COMMENT 'SHA-256 masked SSN',
    state_code                  STRING                      COMMENT 'Two-letter state code',
    zip_code_3digit             STRING                      COMMENT 'ZIP code truncated to first 3 digits for de-identification',
    plan_id                     STRING                      COMMENT 'Current health plan identifier',
    plan_name                   STRING                      COMMENT 'Current health plan name',
    plan_type                   STRING                      COMMENT 'Plan type (HMO/PPO/EPO/POS/HDHP)',
    line_of_business            STRING                      COMMENT 'Line of business (Commercial/Medicare/Medicaid)',
    group_id                    STRING                      COMMENT 'Current employer group identifier',
    group_name                  STRING                      COMMENT 'Current employer group name',
    coverage_start_date         DATE                        COMMENT 'Current coverage period start date',
    coverage_end_date           DATE                        COMMENT 'Current coverage period end date',
    enrollment_status           STRING                      COMMENT 'Current enrollment status',
    pcp_provider_id             STRING                      COMMENT 'Primary care provider NPI',
    pcp_provider_name           STRING                      COMMENT 'Primary care provider name',
    subscriber_id               STRING                      COMMENT 'Subscriber/policyholder identifier',
    relationship_code           STRING                      COMMENT 'Relationship to subscriber',
    risk_score                  DOUBLE                      COMMENT 'CMS-HCC risk adjustment score',
    source_system               STRING                      COMMENT 'Source system identifier',
    effective_date              DATE                        COMMENT 'Date this staging record became effective',
    staging_timestamp           TIMESTAMP                   COMMENT 'Timestamp when record was processed into staging'
)
USING DELTA
COMMENT 'Latest member eligibility records with PHI masking applied. One row per member.'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'quality' = 'silver'
);

-- ---------------------------------------------------------------------------
-- Table: stg_medical_claim_current
-- Purpose: Current/most relevant version of each medical claim after ADR
--          deduplication. When multiple versions of a claim exist (original,
--          adjustment, denial, reversal), we keep the highest-priority version.
--
--          ADR Dedup Priority (CORRECT CMS order):
--            1 = PAID      (highest priority - final adjudicated payment)
--            2 = ADJUSTED  (correction to a paid claim)
--            3 = DENIED    (claim denied by payer)
--            4 = REVERSED  (lowest priority - claim voided)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS stg_medical_claim_current (
    claim_id                    STRING          NOT NULL    COMMENT 'Unique claim identifier',
    claim_line_number           INT                         COMMENT 'Line number within the claim',
    patient_id                  STRING          NOT NULL    COMMENT 'Patient identifier (PHI-safe)',
    claim_type                  STRING                      COMMENT 'Claim type (PROFESSIONAL/INSTITUTIONAL/DENTAL/VISION)',
    claim_status                STRING                      COMMENT 'Claim adjudication status after ADR dedup',
    claim_status_priority       INT                         COMMENT 'ADR priority: PAID=1, ADJUSTED=2, DENIED=3, REVERSED=4',
    claim_submission_date       DATE                        COMMENT 'Date claim was submitted',
    claim_adjudication_date     DATE                        COMMENT 'Date claim was adjudicated',
    claim_start_date            DATE                        COMMENT 'Service start date',
    claim_end_date              DATE                        COMMENT 'Service end date',
    admission_date              DATE                        COMMENT 'Inpatient admission date',
    discharge_date              DATE                        COMMENT 'Inpatient discharge date',
    discharge_status_code       STRING                      COMMENT 'Discharge status code',
    place_of_service_code       STRING                      COMMENT 'CMS place of service code',
    type_of_bill_code           STRING                      COMMENT 'Type of bill code',
    revenue_code                STRING                      COMMENT 'Revenue center code',
    diagnosis_codes             ARRAY<STRING>               COMMENT 'Array of ICD-10 diagnosis codes',
    principal_diagnosis_code    STRING                      COMMENT 'Principal/primary diagnosis code',
    procedure_code              STRING                      COMMENT 'CPT/HCPCS procedure code',
    procedure_code_type         STRING                      COMMENT 'Procedure code type',
    procedure_modifier_1        STRING                      COMMENT 'Procedure modifier 1',
    procedure_modifier_2        STRING                      COMMENT 'Procedure modifier 2',
    drg_code                    STRING                      COMMENT 'Diagnosis Related Group code',
    rendering_provider_npi      STRING                      COMMENT 'Rendering provider NPI',
    rendering_provider_name     STRING                      COMMENT 'Rendering provider name',
    rendering_provider_specialty STRING                     COMMENT 'Provider specialty code',
    billing_provider_npi        STRING                      COMMENT 'Billing provider NPI',
    facility_npi                STRING                      COMMENT 'Facility NPI',
    billed_amount               DOUBLE                      COMMENT 'Total billed amount',
    allowed_amount              DOUBLE                      COMMENT 'Allowed/negotiated amount',
    paid_amount                 DOUBLE                      COMMENT 'Amount paid by payer',
    member_liability_amount     DOUBLE                      COMMENT 'Total member responsibility',
    copay_amount                DOUBLE                      COMMENT 'Copayment amount',
    coinsurance_amount          DOUBLE                      COMMENT 'Coinsurance amount',
    deductible_amount           DOUBLE                      COMMENT 'Deductible amount',
    units_of_service            DOUBLE                      COMMENT 'Number of service units',
    plan_id                     STRING                      COMMENT 'Health plan identifier',
    line_of_business            STRING                      COMMENT 'Line of business',
    network_status              STRING                      COMMENT 'Network status',
    original_claim_id           STRING                      COMMENT 'Original claim ID for adjusted/reversed claims',
    source_system               STRING                      COMMENT 'Source system identifier',
    staging_timestamp           TIMESTAMP                   COMMENT 'Timestamp when record was processed into staging'
)
USING DELTA
COMMENT 'ADR-deduplicated medical claims. Priority: PAID > ADJUSTED > DENIED > REVERSED (CMS standard).'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'quality' = 'silver'
);

-- ---------------------------------------------------------------------------
-- Table: stg_pharmacy_claim_current
-- Purpose: Current/most relevant version of each pharmacy claim after ADR
--          deduplication. Same priority logic as medical claims.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS stg_pharmacy_claim_current (
    claim_id                    STRING          NOT NULL    COMMENT 'Unique pharmacy claim identifier',
    claim_line_number           INT                         COMMENT 'Line number within the claim',
    patient_id                  STRING          NOT NULL    COMMENT 'Patient identifier (PHI-safe)',
    claim_status                STRING                      COMMENT 'Claim status after ADR dedup',
    claim_status_priority       INT                         COMMENT 'ADR priority: PAID=1, ADJUSTED=2, DENIED=3, REVERSED=4',
    fill_date                   DATE                        COMMENT 'Prescription fill date',
    written_date                DATE                        COMMENT 'Date prescription was written',
    ndc_code                    STRING          NOT NULL    COMMENT 'National Drug Code (11-digit)',
    drug_name                   STRING                      COMMENT 'Drug brand or generic name',
    generic_indicator           STRING                      COMMENT 'Generic/brand indicator (G/B)',
    therapeutic_class_code      STRING                      COMMENT 'Therapeutic class code',
    therapeutic_class_name      STRING                      COMMENT 'Therapeutic class description',
    formulary_status            STRING                      COMMENT 'Formulary tier/status',
    quantity_dispensed           DOUBLE                      COMMENT 'Quantity dispensed',
    days_supply                 INT                         COMMENT 'Days supply dispensed',
    refill_number               INT                         COMMENT 'Refill number',
    prescriber_npi              STRING                      COMMENT 'Prescribing provider NPI',
    prescriber_name             STRING                      COMMENT 'Prescribing provider name',
    pharmacy_npi                STRING                      COMMENT 'Dispensing pharmacy NPI',
    pharmacy_name               STRING                      COMMENT 'Dispensing pharmacy name',
    pharmacy_type               STRING                      COMMENT 'Pharmacy type',
    billed_amount               DOUBLE                      COMMENT 'Total billed amount',
    allowed_amount              DOUBLE                      COMMENT 'Allowed/negotiated amount',
    paid_amount                 DOUBLE                      COMMENT 'Amount paid by payer',
    member_liability_amount     DOUBLE                      COMMENT 'Total member responsibility',
    copay_amount                DOUBLE                      COMMENT 'Copayment amount',
    coinsurance_amount          DOUBLE                      COMMENT 'Coinsurance amount',
    deductible_amount           DOUBLE                      COMMENT 'Deductible amount',
    ingredient_cost             DOUBLE                      COMMENT 'Ingredient cost',
    dispensing_fee              DOUBLE                      COMMENT 'Dispensing fee',
    original_claim_id           STRING                      COMMENT 'Original claim ID for adjustments',
    plan_id                     STRING                      COMMENT 'Health plan identifier',
    line_of_business            STRING                      COMMENT 'Line of business',
    source_system               STRING                      COMMENT 'Source system identifier',
    staging_timestamp           TIMESTAMP                   COMMENT 'Timestamp when record was processed into staging'
)
USING DELTA
COMMENT 'ADR-deduplicated pharmacy claims. Priority: PAID > ADJUSTED > DENIED > REVERSED (CMS standard).'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'quality' = 'silver'
);
