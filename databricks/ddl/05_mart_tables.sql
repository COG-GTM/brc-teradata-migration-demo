-- ============================================================================
-- Databricks Healthcare Claims Data Warehouse - Mart Tables
-- ============================================================================
-- Pre-aggregated and denormalized tables optimized for specific reporting
-- and analytics use cases. These tables power dashboards, reports, and
-- downstream analytics tools.
--
-- Platform: Databricks SQL / Delta Lake
-- ============================================================================

USE SCHEMA claims_mart;

-- ---------------------------------------------------------------------------
-- Table: mart_member_months
-- Purpose: One row per member per enrolled month. Used for calculating
--          PMPM (Per Member Per Month) metrics, enrollment counts, and
--          member month denominators for utilization rates.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS mart_member_months (
    member_month_key            BIGINT          GENERATED ALWAYS AS IDENTITY
                                                COMMENT 'Surrogate key',
    member_id                   STRING          NOT NULL    COMMENT 'Member identifier',
    member_key                  BIGINT                      COMMENT 'FK to dim_member (version active during this month)',
    enrollment_month            DATE                        COMMENT 'First day of the enrollment month',
    enrollment_year             INT                         COMMENT 'Enrollment year',
    enrollment_month_number     INT                         COMMENT 'Enrollment month number (1-12)',
    enrollment_year_month       STRING                      COMMENT 'Year-month string (YYYY-MM)',
    plan_id                     STRING                      COMMENT 'Health plan identifier for this month',
    plan_type                   STRING                      COMMENT 'Plan type during this month',
    line_of_business            STRING                      COMMENT 'Line of business during this month',
    group_id                    STRING                      COMMENT 'Employer group during this month',
    gender                      STRING                      COMMENT 'Member gender',
    age_at_month                INT                         COMMENT 'Member age at the start of the month',
    age_band                    STRING                      COMMENT 'Age band (0-17/18-25/26-34/35-44/45-54/55-64/65+)',
    state_code                  STRING                      COMMENT 'State of residence during this month',
    zip_code_3digit             STRING                      COMMENT 'ZIP code (3-digit) during this month',
    risk_score                  DOUBLE                      COMMENT 'Risk score during this month',
    pcp_provider_id             STRING                      COMMENT 'PCP NPI during this month',
    enrollment_status           STRING                      COMMENT 'Enrollment status during this month',
    is_enrolled                 BOOLEAN         NOT NULL    COMMENT 'TRUE if member was enrolled for this month',
    created_timestamp           TIMESTAMP                   COMMENT 'Record creation timestamp'
)
USING DELTA
COMMENT 'Member month enrollment table - one row per member per enrolled month for PMPM metrics.'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'quality' = 'gold'
);

-- ---------------------------------------------------------------------------
-- Table: mart_claim_summary
-- Purpose: Aggregated claim summary by member, time period, and service
--          category. Supports PMPM calculations, utilization analysis,
--          and cost trending.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS mart_claim_summary (
    claim_summary_key           BIGINT          GENERATED ALWAYS AS IDENTITY
                                                COMMENT 'Surrogate key',
    patient_id                  STRING          NOT NULL    COMMENT 'Patient identifier',
    member_key                  BIGINT                      COMMENT 'FK to dim_member',
    summary_month               DATE                        COMMENT 'First day of the summary month',
    summary_year                INT                         COMMENT 'Summary year',
    summary_month_number        INT                         COMMENT 'Summary month number (1-12)',
    summary_year_month          STRING                      COMMENT 'Year-month string (YYYY-MM)',
    claim_category              STRING                      COMMENT 'Claim category (MEDICAL/PHARMACY)',
    service_category            STRING                      COMMENT 'Service category (INPATIENT/OUTPATIENT/PROFESSIONAL/EMERGENCY/PHARMACY/OTHER)',
    plan_id                     STRING                      COMMENT 'Health plan identifier',
    line_of_business            STRING                      COMMENT 'Line of business',
    network_status              STRING                      COMMENT 'Network status (IN_NETWORK/OUT_OF_NETWORK)',
    -- Count measures
    claim_count                 INT                         COMMENT 'Number of unique claims',
    claim_line_count            INT                         COMMENT 'Number of claim lines',
    service_day_count           INT                         COMMENT 'Number of distinct service days',
    -- Financial measures
    total_billed_amount         DOUBLE                      COMMENT 'Total billed amount',
    total_allowed_amount        DOUBLE                      COMMENT 'Total allowed amount',
    total_paid_amount           DOUBLE                      COMMENT 'Total payer paid amount',
    total_member_liability      DOUBLE                      COMMENT 'Total member liability',
    total_copay_amount          DOUBLE                      COMMENT 'Total copayment amount',
    total_coinsurance_amount    DOUBLE                      COMMENT 'Total coinsurance amount',
    total_deductible_amount     DOUBLE                      COMMENT 'Total deductible amount',
    avg_paid_per_claim          DOUBLE                      COMMENT 'Average paid amount per claim',
    avg_paid_per_service_day    DOUBLE                      COMMENT 'Average paid amount per service day',
    -- Metadata
    created_timestamp           TIMESTAMP                   COMMENT 'Record creation timestamp'
)
USING DELTA
COMMENT 'Monthly claim summary by member and service category for PMPM and utilization analysis.'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'quality' = 'gold'
);

-- ---------------------------------------------------------------------------
-- Table: mart_encounter_summary
-- Purpose: Aggregated encounter-level summary for utilization reporting.
--          Includes length of stay analysis, readmission flags, and cost
--          per encounter metrics.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS mart_encounter_summary (
    encounter_summary_key       BIGINT          GENERATED ALWAYS AS IDENTITY
                                                COMMENT 'Surrogate key',
    patient_id                  STRING          NOT NULL    COMMENT 'Patient identifier',
    member_key                  BIGINT                      COMMENT 'FK to dim_member',
    encounter_id                STRING          NOT NULL    COMMENT 'Encounter group identifier',
    encounter_type              STRING                      COMMENT 'Encounter type (INPATIENT/OUTPATIENT/EMERGENCY/OFFICE)',
    encounter_start_date        DATE                        COMMENT 'Encounter start date',
    encounter_end_date          DATE                        COMMENT 'Encounter end date',
    encounter_month             DATE                        COMMENT 'First day of the encounter start month',
    encounter_year_month        STRING                      COMMENT 'Year-month of encounter start',
    length_of_stay_days         INT                         COMMENT 'Length of stay in days',
    claim_count                 INT                         COMMENT 'Number of claims in encounter',
    principal_diagnosis_code    STRING                      COMMENT 'Principal diagnosis code',
    principal_diagnosis_desc    STRING                      COMMENT 'Principal diagnosis description',
    diagnosis_category          STRING                      COMMENT 'High-level diagnosis category (MDC/body system)',
    primary_procedure_code      STRING                      COMMENT 'Primary procedure code',
    drg_code                    STRING                      COMMENT 'DRG code (inpatient)',
    primary_provider_npi        STRING                      COMMENT 'Primary rendering provider NPI',
    facility_npi                STRING                      COMMENT 'Facility NPI',
    plan_id                     STRING                      COMMENT 'Health plan identifier',
    line_of_business            STRING                      COMMENT 'Line of business',
    -- Financial measures
    total_billed_amount         DOUBLE                      COMMENT 'Total billed for encounter',
    total_allowed_amount        DOUBLE                      COMMENT 'Total allowed for encounter',
    total_paid_amount           DOUBLE                      COMMENT 'Total paid for encounter',
    total_member_liability      DOUBLE                      COMMENT 'Total member liability for encounter',
    -- Readmission analysis
    is_readmission_30day        BOOLEAN                     COMMENT 'TRUE if readmitted within 30 days of prior discharge',
    prior_encounter_id          STRING                      COMMENT 'Prior encounter ID if readmission',
    days_since_prior_discharge  INT                         COMMENT 'Days since prior encounter discharge',
    -- Metadata
    created_timestamp           TIMESTAMP                   COMMENT 'Record creation timestamp'
)
USING DELTA
COMMENT 'Encounter-level summary with LOS, readmission flags, and cost metrics for utilization reporting.'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'quality' = 'gold'
);

-- ---------------------------------------------------------------------------
-- Table: mart_quality_measures
-- Purpose: HEDIS (Healthcare Effectiveness Data and Information Set) quality
--          measures and other quality metrics at the member-measure level.
--          Supports value-based care reporting and Stars ratings.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS mart_quality_measures (
    quality_measure_key         BIGINT          GENERATED ALWAYS AS IDENTITY
                                                COMMENT 'Surrogate key',
    patient_id                  STRING          NOT NULL    COMMENT 'Patient identifier',
    member_key                  BIGINT                      COMMENT 'FK to dim_member',
    measure_id                  STRING          NOT NULL    COMMENT 'Quality measure identifier (e.g., BCS, CDC-HBA1C)',
    measure_name                STRING                      COMMENT 'Quality measure full name',
    measure_category            STRING                      COMMENT 'Measure category (Preventive/Chronic/Behavioral/Access)',
    measurement_year            INT                         COMMENT 'Measurement year',
    measurement_period_start    DATE                        COMMENT 'Start of measurement period',
    measurement_period_end      DATE                        COMMENT 'End of measurement period',
    -- Measure status
    is_eligible                 BOOLEAN                     COMMENT 'TRUE if member is eligible/denominator for this measure',
    is_numerator_compliant      BOOLEAN                     COMMENT 'TRUE if member meets numerator criteria',
    is_excluded                 BOOLEAN                     COMMENT 'TRUE if member is excluded from measure',
    exclusion_reason            STRING                      COMMENT 'Reason for exclusion if applicable',
    -- Eligibility details
    denominator_criteria_met    STRING                      COMMENT 'Which denominator criteria were met',
    numerator_criteria_met      STRING                      COMMENT 'Which numerator criteria were met',
    -- Key dates
    last_qualifying_service_date DATE                       COMMENT 'Date of last service meeting numerator criteria',
    last_qualifying_claim_id    STRING                      COMMENT 'Claim ID of last qualifying service',
    -- Demographics for stratification
    age_at_measurement          INT                         COMMENT 'Member age during measurement period',
    gender                      STRING                      COMMENT 'Member gender for stratification',
    line_of_business            STRING                      COMMENT 'Line of business for stratification',
    plan_id                     STRING                      COMMENT 'Health plan identifier',
    -- Gap closure
    gap_status                  STRING                      COMMENT 'Gap status (OPEN/CLOSED/PENDING)',
    gap_identified_date         DATE                        COMMENT 'Date gap was first identified',
    gap_closed_date             DATE                        COMMENT 'Date gap was closed (numerator met)',
    days_to_close_gap           INT                         COMMENT 'Days between gap identification and closure',
    -- Metadata
    created_timestamp           TIMESTAMP                   COMMENT 'Record creation timestamp',
    updated_timestamp           TIMESTAMP                   COMMENT 'Last update timestamp'
)
USING DELTA
COMMENT 'HEDIS quality measures at the member-measure level for value-based care and Stars reporting.'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'quality' = 'gold'
);
