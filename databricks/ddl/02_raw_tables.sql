-- ============================================================================
-- Databricks Healthcare Claims Data Warehouse - Raw Tables (Delta Lake)
-- ============================================================================
-- Raw landing tables for healthcare claims data. These tables receive data
-- directly from Auto Loader (cloudFiles) or batch ingestion processes.
--
-- KEY STRUCTURAL DIFFERENCE vs. Teradata:
--   - Diagnosis codes are stored as ARRAY<STRING> (JSON array) in a single
--     column `diagnosis_codes` instead of 25 individual columns
--     (icd_diagnosis_code_1..25). This leverages Delta Lake's native support
--     for complex types and simplifies downstream processing.
--
-- NAMING DRIFT vs. Teradata:
--   - Teradata: service_date_from / service_date_to
--     Databricks: claim_start_date / claim_end_date
--   - Teradata: member_id (all tables)
--     Databricks: patient_id (some tables use patient_id for clarity)
--
-- Platform: Databricks SQL / Delta Lake
-- ============================================================================

USE SCHEMA claims_raw;

-- ---------------------------------------------------------------------------
-- Table: raw_member_eligibility
-- Purpose: Raw member eligibility and enrollment records from the enrollment
--          system. Each row represents a coverage period for a member under
--          a specific plan.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw_member_eligibility (
    member_id                   STRING          NOT NULL    COMMENT 'Unique member identifier from source system',
    member_first_name           STRING                      COMMENT 'Member first name (PHI - will be masked in staging)',
    member_last_name            STRING                      COMMENT 'Member last name (PHI - will be masked in staging)',
    date_of_birth               DATE                        COMMENT 'Member date of birth (PHI - will be masked in staging)',
    gender                      STRING                      COMMENT 'Member gender code (M/F/U)',
    ssn                         STRING                      COMMENT 'Social Security Number (PHI - will be masked in staging)',
    address_line_1              STRING                      COMMENT 'Street address line 1 (PHI)',
    address_line_2              STRING                      COMMENT 'Street address line 2 (PHI)',
    city                        STRING                      COMMENT 'City of residence',
    state_code                  STRING                      COMMENT 'Two-letter state code',
    zip_code                    STRING                      COMMENT 'ZIP code (PHI - will be truncated to 3 digits in staging)',
    phone_number                STRING                      COMMENT 'Phone number (PHI - will be masked in staging)',
    email_address               STRING                      COMMENT 'Email address (PHI - will be masked in staging)',
    plan_id                     STRING                      COMMENT 'Health plan identifier',
    plan_name                   STRING                      COMMENT 'Health plan display name',
    plan_type                   STRING                      COMMENT 'Plan type (HMO/PPO/EPO/POS/HDHP)',
    line_of_business            STRING                      COMMENT 'Line of business (Commercial/Medicare/Medicaid)',
    group_id                    STRING                      COMMENT 'Employer group identifier',
    group_name                  STRING                      COMMENT 'Employer group name',
    coverage_start_date         DATE                        COMMENT 'Start date of coverage period',
    coverage_end_date           DATE                        COMMENT 'End date of coverage period',
    enrollment_status           STRING                      COMMENT 'Enrollment status (ACTIVE/TERMINATED/COBRA/PENDING)',
    pcp_provider_id             STRING                      COMMENT 'Primary care provider NPI',
    pcp_provider_name           STRING                      COMMENT 'Primary care provider name',
    subscriber_id               STRING                      COMMENT 'Subscriber/policyholder identifier',
    relationship_code           STRING                      COMMENT 'Relationship to subscriber (SELF/SPOUSE/CHILD/OTHER)',
    medicare_beneficiary_id     STRING                      COMMENT 'Medicare Beneficiary Identifier (MBI) if applicable',
    medicaid_id                 STRING                      COMMENT 'Medicaid recipient ID if applicable',
    risk_score                  DOUBLE                      COMMENT 'CMS-HCC risk adjustment score',
    source_system               STRING                      COMMENT 'Source system identifier',
    source_file_name            STRING                      COMMENT 'Name of the ingested source file',
    ingestion_timestamp         TIMESTAMP                   COMMENT 'Timestamp when record was ingested into raw layer',
    record_hash                 STRING                      COMMENT 'SHA-256 hash of record for change detection'
)
USING DELTA
COMMENT 'Raw member eligibility and enrollment records from source enrollment systems.'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'quality' = 'bronze'
);

-- ---------------------------------------------------------------------------
-- Table: raw_medical_claim
-- Purpose: Raw medical/professional/institutional claims from claims
--          adjudication system. IMPORTANT: Diagnosis codes are stored as
--          ARRAY<STRING> instead of individual columns - this is a key
--          structural difference from the Teradata implementation.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw_medical_claim (
    claim_id                    STRING          NOT NULL    COMMENT 'Unique claim identifier',
    claim_line_number           INT                         COMMENT 'Line number within the claim',
    patient_id                  STRING          NOT NULL    COMMENT 'Patient/member identifier (maps to member_id in eligibility)',
    subscriber_id               STRING                      COMMENT 'Subscriber/policyholder identifier',
    claim_type                  STRING                      COMMENT 'Claim type (PROFESSIONAL/INSTITUTIONAL/DENTAL/VISION)',
    claim_status                STRING                      COMMENT 'Claim adjudication status (PAID/ADJUSTED/DENIED/REVERSED)',
    claim_submission_date       DATE                        COMMENT 'Date claim was submitted by provider',
    claim_adjudication_date     DATE                        COMMENT 'Date claim was adjudicated by payer',
    claim_start_date            DATE                        COMMENT 'Service start date (Teradata: service_date_from)',
    claim_end_date              DATE                        COMMENT 'Service end date (Teradata: service_date_to)',
    admission_date              DATE                        COMMENT 'Inpatient admission date (institutional claims)',
    discharge_date              DATE                        COMMENT 'Inpatient discharge date (institutional claims)',
    discharge_status_code       STRING                      COMMENT 'Discharge status code',
    place_of_service_code       STRING                      COMMENT 'CMS place of service code',
    type_of_bill_code           STRING                      COMMENT 'Type of bill code (institutional claims)',
    revenue_code                STRING                      COMMENT 'Revenue center code (institutional claims)',
    -- -----------------------------------------------------------------------
    -- STRUCTURAL DRIFT: Diagnosis codes as ARRAY<STRING> instead of 25
    -- individual columns (icd_diagnosis_code_1..25 in Teradata)
    -- -----------------------------------------------------------------------
    diagnosis_codes             ARRAY<STRING>               COMMENT 'Array of ICD-10 diagnosis codes (replaces 25 individual columns in Teradata)',
    diagnosis_code_type         STRING                      COMMENT 'Diagnosis code type (ICD-10-CM)',
    principal_diagnosis_code    STRING                      COMMENT 'Principal/primary diagnosis code',
    admitting_diagnosis_code    STRING                      COMMENT 'Admitting diagnosis code (institutional claims)',
    procedure_code              STRING                      COMMENT 'CPT/HCPCS procedure code',
    procedure_code_type         STRING                      COMMENT 'Procedure code type (CPT/HCPCS/ICD-10-PCS)',
    procedure_modifier_1        STRING                      COMMENT 'Procedure modifier 1',
    procedure_modifier_2        STRING                      COMMENT 'Procedure modifier 2',
    procedure_modifier_3        STRING                      COMMENT 'Procedure modifier 3',
    procedure_modifier_4        STRING                      COMMENT 'Procedure modifier 4',
    drg_code                    STRING                      COMMENT 'Diagnosis Related Group code',
    ndc_code                    STRING                      COMMENT 'National Drug Code (if applicable)',
    rendering_provider_npi      STRING                      COMMENT 'Rendering provider National Provider Identifier',
    rendering_provider_name     STRING                      COMMENT 'Rendering provider name',
    rendering_provider_specialty STRING                     COMMENT 'Rendering provider specialty code',
    billing_provider_npi        STRING                      COMMENT 'Billing provider NPI',
    billing_provider_name       STRING                      COMMENT 'Billing provider name',
    billing_provider_tax_id     STRING                      COMMENT 'Billing provider tax ID',
    facility_npi                STRING                      COMMENT 'Facility NPI (institutional claims)',
    facility_name               STRING                      COMMENT 'Facility name',
    referring_provider_npi      STRING                      COMMENT 'Referring provider NPI',
    billed_amount               DOUBLE                      COMMENT 'Total billed/charged amount',
    allowed_amount              DOUBLE                      COMMENT 'Allowed/negotiated amount',
    paid_amount                 DOUBLE                      COMMENT 'Amount paid by payer',
    member_liability_amount     DOUBLE                      COMMENT 'Total member responsibility',
    copay_amount                DOUBLE                      COMMENT 'Member copayment amount',
    coinsurance_amount          DOUBLE                      COMMENT 'Member coinsurance amount',
    deductible_amount           DOUBLE                      COMMENT 'Member deductible amount',
    cob_amount                  DOUBLE                      COMMENT 'Coordination of benefits amount',
    units_of_service            DOUBLE                      COMMENT 'Number of service units',
    days_of_service             INT                         COMMENT 'Number of days of service',
    authorization_number        STRING                      COMMENT 'Prior authorization number',
    referral_number             STRING                      COMMENT 'Referral number',
    original_claim_id           STRING                      COMMENT 'Original claim ID for adjustments/reversals',
    adjustment_sequence_number  INT                         COMMENT 'Adjustment/resubmission sequence number',
    plan_id                     STRING                      COMMENT 'Health plan identifier',
    line_of_business            STRING                      COMMENT 'Line of business',
    network_status              STRING                      COMMENT 'Network status (IN_NETWORK/OUT_OF_NETWORK)',
    benefit_code                STRING                      COMMENT 'Benefit category code',
    source_system               STRING                      COMMENT 'Source system identifier',
    source_file_name            STRING                      COMMENT 'Name of the ingested source file',
    ingestion_timestamp         TIMESTAMP                   COMMENT 'Timestamp when record was ingested into raw layer',
    record_hash                 STRING                      COMMENT 'SHA-256 hash of record for change detection'
)
USING DELTA
COMMENT 'Raw medical claims with diagnosis codes stored as ARRAY<STRING> (structural drift from Teradata 25-column layout).'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'quality' = 'bronze'
);

-- ---------------------------------------------------------------------------
-- Table: raw_pharmacy_claim
-- Purpose: Raw pharmacy/prescription drug claims from PBM (Pharmacy Benefit
--          Manager) feed or claims adjudication system.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw_pharmacy_claim (
    claim_id                    STRING          NOT NULL    COMMENT 'Unique pharmacy claim identifier',
    claim_line_number           INT                         COMMENT 'Line number within the claim',
    patient_id                  STRING          NOT NULL    COMMENT 'Patient/member identifier (maps to member_id in eligibility)',
    subscriber_id               STRING                      COMMENT 'Subscriber/policyholder identifier',
    claim_status                STRING                      COMMENT 'Claim status (PAID/ADJUSTED/DENIED/REVERSED)',
    fill_date                   DATE                        COMMENT 'Prescription fill date',
    written_date                DATE                        COMMENT 'Date prescription was written',
    ndc_code                    STRING          NOT NULL    COMMENT 'National Drug Code (11-digit)',
    drug_name                   STRING                      COMMENT 'Drug brand or generic name',
    generic_indicator           STRING                      COMMENT 'Generic/brand indicator (G/B)',
    therapeutic_class_code      STRING                      COMMENT 'Therapeutic class code (GPI/ATC)',
    therapeutic_class_name      STRING                      COMMENT 'Therapeutic class description',
    formulary_status            STRING                      COMMENT 'Formulary tier/status',
    quantity_dispensed           DOUBLE                      COMMENT 'Quantity dispensed',
    days_supply                 INT                         COMMENT 'Days supply dispensed',
    refill_number               INT                         COMMENT 'Refill number (0 = original fill)',
    daw_code                    STRING                      COMMENT 'Dispense As Written code',
    compound_code               STRING                      COMMENT 'Compound drug indicator',
    prescriber_npi              STRING                      COMMENT 'Prescribing provider NPI',
    prescriber_name             STRING                      COMMENT 'Prescribing provider name',
    prescriber_specialty        STRING                      COMMENT 'Prescribing provider specialty',
    pharmacy_npi                STRING                      COMMENT 'Dispensing pharmacy NPI',
    pharmacy_name               STRING                      COMMENT 'Dispensing pharmacy name',
    pharmacy_type               STRING                      COMMENT 'Pharmacy type (RETAIL/MAIL_ORDER/SPECIALTY)',
    pharmacy_zip_code           STRING                      COMMENT 'Pharmacy ZIP code',
    billed_amount               DOUBLE                      COMMENT 'Total billed amount',
    allowed_amount              DOUBLE                      COMMENT 'Allowed/negotiated amount',
    paid_amount                 DOUBLE                      COMMENT 'Amount paid by payer',
    member_liability_amount     DOUBLE                      COMMENT 'Total member responsibility',
    copay_amount                DOUBLE                      COMMENT 'Member copayment amount',
    coinsurance_amount          DOUBLE                      COMMENT 'Member coinsurance amount',
    deductible_amount           DOUBLE                      COMMENT 'Member deductible amount',
    ingredient_cost             DOUBLE                      COMMENT 'Ingredient cost of the drug',
    dispensing_fee              DOUBLE                      COMMENT 'Pharmacy dispensing fee',
    sales_tax                   DOUBLE                      COMMENT 'Sales tax amount',
    original_claim_id           STRING                      COMMENT 'Original claim ID for adjustments',
    adjustment_sequence_number  INT                         COMMENT 'Adjustment/resubmission sequence number',
    plan_id                     STRING                      COMMENT 'Health plan identifier',
    line_of_business            STRING                      COMMENT 'Line of business',
    benefit_code                STRING                      COMMENT 'Benefit category code',
    prior_auth_required         STRING                      COMMENT 'Prior authorization required flag (Y/N)',
    prior_auth_number           STRING                      COMMENT 'Prior authorization number',
    source_system               STRING                      COMMENT 'Source system identifier',
    source_file_name            STRING                      COMMENT 'Name of the ingested source file',
    ingestion_timestamp         TIMESTAMP                   COMMENT 'Timestamp when record was ingested into raw layer',
    record_hash                 STRING                      COMMENT 'SHA-256 hash of record for change detection'
)
USING DELTA
COMMENT 'Raw pharmacy/prescription drug claims from PBM feed or claims adjudication system.'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'quality' = 'bronze'
);
