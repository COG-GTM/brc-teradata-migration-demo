-- ============================================================================
-- Databricks Healthcare Claims Data Warehouse - Warehouse Tables
-- ============================================================================
-- Conformed star schema with SCD Type 2 dimensions and fact tables.
-- Uses Delta Lake MERGE for SCD Type 2 processing and proper window
-- functions for encounter overlap detection.
--
-- Platform: Databricks SQL / Delta Lake
-- ============================================================================

USE SCHEMA claims_warehouse;

-- ---------------------------------------------------------------------------
-- Table: dim_member (SCD Type 2)
-- Purpose: Slowly Changing Dimension Type 2 for member demographics and
--          enrollment. Maintains full history of member attribute changes.
--          Updated via Delta Lake MERGE operations.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_member (
    member_key                  BIGINT          GENERATED ALWAYS AS IDENTITY
                                                COMMENT 'Surrogate key for dimension',
    member_id                   STRING          NOT NULL    COMMENT 'Natural/business key from source system',
    member_first_name_masked    STRING                      COMMENT 'SHA-256 masked first name',
    member_last_name_masked     STRING                      COMMENT 'SHA-256 masked last name',
    date_of_birth_masked        DATE                        COMMENT 'Masked date of birth',
    gender                      STRING                      COMMENT 'Gender code (M/F/U)',
    state_code                  STRING                      COMMENT 'State of residence',
    zip_code_3digit             STRING                      COMMENT 'ZIP code truncated to 3 digits',
    plan_id                     STRING                      COMMENT 'Health plan identifier',
    plan_name                   STRING                      COMMENT 'Health plan name',
    plan_type                   STRING                      COMMENT 'Plan type (HMO/PPO/EPO/POS/HDHP)',
    line_of_business            STRING                      COMMENT 'Line of business (Commercial/Medicare/Medicaid)',
    group_id                    STRING                      COMMENT 'Employer group identifier',
    group_name                  STRING                      COMMENT 'Employer group name',
    enrollment_status           STRING                      COMMENT 'Enrollment status at time of record',
    pcp_provider_id             STRING                      COMMENT 'Primary care provider NPI',
    risk_score                  DOUBLE                      COMMENT 'CMS-HCC risk adjustment score',
    -- SCD Type 2 tracking columns
    effective_start_date        DATE            NOT NULL    COMMENT 'Date this version became effective',
    effective_end_date          DATE                        COMMENT 'Date this version expired (NULL = current)',
    is_current                  BOOLEAN         NOT NULL    COMMENT 'Flag indicating if this is the current version',
    record_hash                 STRING                      COMMENT 'Hash of tracked attributes for change detection',
    created_timestamp           TIMESTAMP                   COMMENT 'Timestamp when this version was created',
    updated_timestamp           TIMESTAMP                   COMMENT 'Timestamp of last update to this version'
)
USING DELTA
COMMENT 'SCD Type 2 member dimension with full history. Updated via Delta Lake MERGE.'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'quality' = 'gold'
);

-- ---------------------------------------------------------------------------
-- Example: SCD Type 2 MERGE statement for dim_member
-- This pattern is used by the PySpark notebook to maintain history.
-- ---------------------------------------------------------------------------
-- MERGE INTO claims_warehouse.dim_member AS target
-- USING (
--     SELECT
--         s.*,
--         SHA2(CONCAT_WS('|', s.plan_id, s.plan_type, s.line_of_business,
--                         s.enrollment_status, s.pcp_provider_id,
--                         CAST(s.risk_score AS STRING)), 256) AS new_hash
--     FROM claims_staging.stg_member_latest s
-- ) AS source
-- ON target.member_id = source.member_id AND target.is_current = TRUE
-- WHEN MATCHED AND target.record_hash != source.new_hash THEN
--     UPDATE SET
--         target.effective_end_date = CURRENT_DATE(),
--         target.is_current = FALSE,
--         target.updated_timestamp = CURRENT_TIMESTAMP()
-- WHEN NOT MATCHED THEN
--     INSERT (member_id, member_first_name_masked, member_last_name_masked,
--             date_of_birth_masked, gender, state_code, zip_code_3digit,
--             plan_id, plan_name, plan_type, line_of_business, group_id,
--             group_name, enrollment_status, pcp_provider_id, risk_score,
--             effective_start_date, effective_end_date, is_current,
--             record_hash, created_timestamp, updated_timestamp)
--     VALUES (source.member_id, source.member_first_name_masked,
--             source.member_last_name_masked, source.date_of_birth_masked,
--             source.gender, source.state_code, source.zip_code_3digit,
--             source.plan_id, source.plan_name, source.plan_type,
--             source.line_of_business, source.group_id, source.group_name,
--             source.enrollment_status, source.pcp_provider_id,
--             source.risk_score, CURRENT_DATE(), NULL, TRUE,
--             source.new_hash, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP());

-- ---------------------------------------------------------------------------
-- Table: dim_provider
-- Purpose: Provider dimension containing rendering, billing, prescribing,
--          and facility provider attributes. Type 1 (overwrite) for most
--          attributes.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_provider (
    provider_key                BIGINT          GENERATED ALWAYS AS IDENTITY
                                                COMMENT 'Surrogate key for dimension',
    provider_npi                STRING          NOT NULL    COMMENT 'National Provider Identifier (natural key)',
    provider_name               STRING                      COMMENT 'Provider display name',
    provider_type               STRING                      COMMENT 'Provider type (INDIVIDUAL/ORGANIZATION)',
    provider_specialty          STRING                      COMMENT 'Primary specialty code',
    provider_specialty_desc     STRING                      COMMENT 'Specialty description',
    provider_taxonomy_code      STRING                      COMMENT 'Healthcare provider taxonomy code',
    provider_tax_id             STRING                      COMMENT 'Tax identification number',
    provider_address_line_1     STRING                      COMMENT 'Practice address line 1',
    provider_city               STRING                      COMMENT 'Practice city',
    provider_state              STRING                      COMMENT 'Practice state',
    provider_zip_code           STRING                      COMMENT 'Practice ZIP code',
    provider_phone              STRING                      COMMENT 'Practice phone number',
    network_status              STRING                      COMMENT 'Default network status (IN_NETWORK/OUT_OF_NETWORK)',
    accepting_new_patients      STRING                      COMMENT 'Accepting new patients flag (Y/N)',
    effective_date              DATE                        COMMENT 'Date provider record became effective',
    termination_date            DATE                        COMMENT 'Date provider was terminated (NULL = active)',
    is_active                   BOOLEAN         NOT NULL    COMMENT 'Active status flag',
    created_timestamp           TIMESTAMP                   COMMENT 'Record creation timestamp',
    updated_timestamp           TIMESTAMP                   COMMENT 'Last update timestamp'
)
USING DELTA
COMMENT 'Provider dimension with individual and organizational healthcare provider attributes.'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'quality' = 'gold'
);

-- ---------------------------------------------------------------------------
-- Table: dim_date
-- Purpose: Standard date dimension for time-based analysis. Pre-populated
--          with dates from 2010-01-01 through 2035-12-31.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_date (
    date_key                    INT             NOT NULL    COMMENT 'Date key in YYYYMMDD format',
    full_date                   DATE            NOT NULL    COMMENT 'Full date value',
    year                        INT                         COMMENT 'Calendar year (e.g. 2024)',
    quarter                     INT                         COMMENT 'Calendar quarter (1-4)',
    month                       INT                         COMMENT 'Calendar month (1-12)',
    month_name                  STRING                      COMMENT 'Month name (January, February, ...)',
    month_abbrev                STRING                      COMMENT 'Month abbreviation (Jan, Feb, ...)',
    day_of_month                INT                         COMMENT 'Day of month (1-31)',
    day_of_week                 INT                         COMMENT 'Day of week (1=Monday, 7=Sunday)',
    day_name                    STRING                      COMMENT 'Day name (Monday, Tuesday, ...)',
    week_of_year                INT                         COMMENT 'ISO week of year (1-53)',
    is_weekend                  BOOLEAN                     COMMENT 'TRUE if Saturday or Sunday',
    is_holiday                  BOOLEAN                     COMMENT 'TRUE if US federal holiday',
    holiday_name                STRING                      COMMENT 'Holiday name if applicable',
    fiscal_year                 INT                         COMMENT 'Fiscal year (Oct-Sep)',
    fiscal_quarter              INT                         COMMENT 'Fiscal quarter (1-4)',
    fiscal_month                INT                         COMMENT 'Fiscal month (1-12, starting Oct)',
    year_month                  STRING                      COMMENT 'Year-month string (YYYY-MM)',
    year_quarter                STRING                      COMMENT 'Year-quarter string (YYYY-Q#)',
    first_day_of_month          DATE                        COMMENT 'First day of the month',
    last_day_of_month           DATE                        COMMENT 'Last day of the month',
    first_day_of_quarter        DATE                        COMMENT 'First day of the quarter',
    last_day_of_quarter         DATE                        COMMENT 'Last day of the quarter'
)
USING DELTA
COMMENT 'Standard date dimension for time-based analysis across all claims data.'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'quality' = 'gold'
);

-- ---------------------------------------------------------------------------
-- Table: fct_medical_claim
-- Purpose: Fact table for medical claims at the claim-line level. References
--          dimension tables via surrogate and natural keys.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fct_medical_claim (
    claim_fact_key              BIGINT          GENERATED ALWAYS AS IDENTITY
                                                COMMENT 'Surrogate key for fact row',
    claim_id                    STRING          NOT NULL    COMMENT 'Claim identifier (degenerate dimension)',
    claim_line_number           INT                         COMMENT 'Claim line number',
    member_key                  BIGINT                      COMMENT 'FK to dim_member (current version at time of service)',
    patient_id                  STRING          NOT NULL    COMMENT 'Patient identifier (natural key)',
    rendering_provider_key      BIGINT                      COMMENT 'FK to dim_provider for rendering provider',
    billing_provider_key        BIGINT                      COMMENT 'FK to dim_provider for billing provider',
    facility_provider_key       BIGINT                      COMMENT 'FK to dim_provider for facility',
    service_start_date_key      INT                         COMMENT 'FK to dim_date for service start',
    service_end_date_key        INT                         COMMENT 'FK to dim_date for service end',
    claim_type                  STRING                      COMMENT 'Claim type',
    claim_status                STRING                      COMMENT 'Claim status after ADR dedup',
    place_of_service_code       STRING                      COMMENT 'Place of service code',
    type_of_bill_code           STRING                      COMMENT 'Type of bill code',
    revenue_code                STRING                      COMMENT 'Revenue center code',
    diagnosis_codes             ARRAY<STRING>               COMMENT 'Array of diagnosis codes',
    principal_diagnosis_code    STRING                      COMMENT 'Principal diagnosis code',
    procedure_code              STRING                      COMMENT 'CPT/HCPCS procedure code',
    procedure_code_type         STRING                      COMMENT 'Procedure code type',
    drg_code                    STRING                      COMMENT 'DRG code',
    plan_id                     STRING                      COMMENT 'Health plan identifier',
    line_of_business            STRING                      COMMENT 'Line of business',
    network_status              STRING                      COMMENT 'Network status',
    -- Measures
    billed_amount               DOUBLE                      COMMENT 'Billed/charged amount',
    allowed_amount              DOUBLE                      COMMENT 'Allowed/negotiated amount',
    paid_amount                 DOUBLE                      COMMENT 'Payer paid amount',
    member_liability_amount     DOUBLE                      COMMENT 'Total member responsibility',
    copay_amount                DOUBLE                      COMMENT 'Copayment amount',
    coinsurance_amount          DOUBLE                      COMMENT 'Coinsurance amount',
    deductible_amount           DOUBLE                      COMMENT 'Deductible amount',
    units_of_service            DOUBLE                      COMMENT 'Service units',
    -- Metadata
    encounter_id                STRING                      COMMENT 'Encounter group ID (from encounter grouping logic)',
    created_timestamp           TIMESTAMP                   COMMENT 'Fact record creation timestamp'
)
USING DELTA
COMMENT 'Medical claims fact table at claim-line grain with dimension keys and financial measures.'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'quality' = 'gold'
);

-- ---------------------------------------------------------------------------
-- Table: fct_pharmacy_claim
-- Purpose: Fact table for pharmacy claims at the claim-line level.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fct_pharmacy_claim (
    claim_fact_key              BIGINT          GENERATED ALWAYS AS IDENTITY
                                                COMMENT 'Surrogate key for fact row',
    claim_id                    STRING          NOT NULL    COMMENT 'Pharmacy claim identifier',
    claim_line_number           INT                         COMMENT 'Claim line number',
    member_key                  BIGINT                      COMMENT 'FK to dim_member',
    patient_id                  STRING          NOT NULL    COMMENT 'Patient identifier',
    prescriber_provider_key     BIGINT                      COMMENT 'FK to dim_provider for prescriber',
    pharmacy_provider_key       BIGINT                      COMMENT 'FK to dim_provider for pharmacy',
    fill_date_key               INT                         COMMENT 'FK to dim_date for fill date',
    claim_status                STRING                      COMMENT 'Claim status after ADR dedup',
    ndc_code                    STRING                      COMMENT 'National Drug Code',
    drug_name                   STRING                      COMMENT 'Drug name',
    generic_indicator           STRING                      COMMENT 'Generic/brand indicator',
    therapeutic_class_code      STRING                      COMMENT 'Therapeutic class code',
    therapeutic_class_name      STRING                      COMMENT 'Therapeutic class name',
    formulary_status            STRING                      COMMENT 'Formulary tier/status',
    quantity_dispensed           DOUBLE                      COMMENT 'Quantity dispensed',
    days_supply                 INT                         COMMENT 'Days supply',
    refill_number               INT                         COMMENT 'Refill number',
    pharmacy_type               STRING                      COMMENT 'Pharmacy type',
    plan_id                     STRING                      COMMENT 'Health plan identifier',
    line_of_business            STRING                      COMMENT 'Line of business',
    -- Measures
    billed_amount               DOUBLE                      COMMENT 'Billed amount',
    allowed_amount              DOUBLE                      COMMENT 'Allowed amount',
    paid_amount                 DOUBLE                      COMMENT 'Payer paid amount',
    member_liability_amount     DOUBLE                      COMMENT 'Member responsibility',
    copay_amount                DOUBLE                      COMMENT 'Copayment',
    coinsurance_amount          DOUBLE                      COMMENT 'Coinsurance',
    deductible_amount           DOUBLE                      COMMENT 'Deductible',
    ingredient_cost             DOUBLE                      COMMENT 'Ingredient cost',
    dispensing_fee              DOUBLE                      COMMENT 'Dispensing fee',
    -- Metadata
    created_timestamp           TIMESTAMP                   COMMENT 'Fact record creation timestamp'
)
USING DELTA
COMMENT 'Pharmacy claims fact table at claim-line grain with drug details and financial measures.'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'quality' = 'gold'
);

-- ---------------------------------------------------------------------------
-- Table: fct_encounter
-- Purpose: Encounter-level fact table grouping overlapping medical claims
--          into logical encounters. Uses proper gap-and-island algorithm
--          with window functions for correct overlap detection.
--
--          This is the CORRECT implementation using:
--          - LAG/LEAD window functions to detect date overlaps
--          - Running MAX of end dates to identify truly contiguous service periods
--          - Unlike Teradata's naive 30-day grouping, this detects actual overlaps
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fct_encounter (
    encounter_key               BIGINT          GENERATED ALWAYS AS IDENTITY
                                                COMMENT 'Surrogate key for encounter',
    encounter_id                STRING          NOT NULL    COMMENT 'Generated encounter group identifier',
    patient_id                  STRING          NOT NULL    COMMENT 'Patient identifier',
    member_key                  BIGINT                      COMMENT 'FK to dim_member (at encounter start)',
    encounter_type              STRING                      COMMENT 'Encounter type (INPATIENT/OUTPATIENT/EMERGENCY/OFFICE)',
    encounter_start_date        DATE                        COMMENT 'Earliest service start date in encounter',
    encounter_end_date          DATE                        COMMENT 'Latest service end date in encounter',
    encounter_start_date_key    INT                         COMMENT 'FK to dim_date for encounter start',
    encounter_end_date_key      INT                         COMMENT 'FK to dim_date for encounter end',
    length_of_encounter_days    INT                         COMMENT 'Total days from start to end of encounter',
    claim_count                 INT                         COMMENT 'Number of claims in this encounter',
    claim_line_count            INT                         COMMENT 'Number of claim lines in this encounter',
    principal_diagnosis_code    STRING                      COMMENT 'Principal diagnosis from earliest claim',
    diagnosis_codes_all         ARRAY<STRING>               COMMENT 'All unique diagnosis codes across encounter',
    primary_provider_npi        STRING                      COMMENT 'Primary rendering provider NPI',
    facility_npi                STRING                      COMMENT 'Primary facility NPI',
    -- Aggregated financial measures
    total_billed_amount         DOUBLE                      COMMENT 'Sum of billed amounts across encounter',
    total_allowed_amount        DOUBLE                      COMMENT 'Sum of allowed amounts across encounter',
    total_paid_amount           DOUBLE                      COMMENT 'Sum of paid amounts across encounter',
    total_member_liability      DOUBLE                      COMMENT 'Sum of member liability across encounter',
    plan_id                     STRING                      COMMENT 'Health plan identifier',
    line_of_business            STRING                      COMMENT 'Line of business',
    -- Metadata
    grouping_method             STRING          DEFAULT 'gap_and_island'
                                                COMMENT 'Algorithm used for encounter grouping',
    created_timestamp           TIMESTAMP                   COMMENT 'Record creation timestamp'
)
USING DELTA
COMMENT 'Encounter-level facts grouping overlapping claims using gap-and-island algorithm with proper window functions.'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'quality' = 'gold'
);
