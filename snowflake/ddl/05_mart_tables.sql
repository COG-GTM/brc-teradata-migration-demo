/*******************************************************************************
 * Healthcare Claims Data Warehouse - Mart Tables (Snowflake)
 *
 * Pre-aggregated reporting tables for analytics and dashboards.
 *
 * Snowflake-specific features used:
 *   - CLUSTER BY for query performance
 *   - COPY GRANTS
 *   - VARIANT for flexible quality measure storage
 ******************************************************************************/

USE DATABASE CLAIMS_DW;
USE SCHEMA MART;

-- =============================================================================
-- MART_MEMBER_MONTHS: monthly member enrollment summary
-- One row per member per month they were eligible
-- =============================================================================
CREATE OR REPLACE TABLE CLAIMS_DW.MART.MART_MEMBER_MONTHS
(
    member_month_key            INTEGER          NOT NULL AUTOINCREMENT,
    member_id                   VARCHAR(20)      NOT NULL,
    member_key                  INTEGER          NOT NULL,
    year_month                  VARCHAR(7)       NOT NULL
                                COMMENT 'YYYY-MM format',
    calendar_year               INTEGER          NOT NULL,
    calendar_month              INTEGER          NOT NULL,
    -- Member attributes (as of that month)
    age_as_of_month             INTEGER,
    age_band                    VARCHAR(20),
    gender                      VARCHAR(1),
    state_code                  VARCHAR(2),
    zip_code                    VARCHAR(10),
    plan_code                   VARCHAR(20),
    product_type                VARCHAR(20),
    line_of_business            VARCHAR(20),
    group_number                VARCHAR(30),
    pcp_provider_id             VARCHAR(20),
    coverage_type               VARCHAR(20),
    -- Enrollment flags
    is_enrolled                 BOOLEAN          NOT NULL DEFAULT TRUE,
    member_month_count          INTEGER          DEFAULT 1
                                COMMENT 'Always 1 - useful for SUM aggregation',
    days_enrolled_in_month      INTEGER,
    -- Utilization summary for the month
    medical_claim_count         INTEGER          DEFAULT 0,
    pharmacy_claim_count        INTEGER          DEFAULT 0,
    encounter_count             INTEGER          DEFAULT 0,
    inpatient_admission_count   INTEGER          DEFAULT 0,
    ed_visit_count              INTEGER          DEFAULT 0,
    office_visit_count          INTEGER          DEFAULT 0,
    -- Cost summary for the month
    total_medical_paid          NUMBER(18,2)     DEFAULT 0,
    total_medical_allowed       NUMBER(18,2)     DEFAULT 0,
    total_pharmacy_paid         NUMBER(18,2)     DEFAULT 0,
    total_pharmacy_allowed      NUMBER(18,2)     DEFAULT 0,
    total_paid                  NUMBER(18,2)     DEFAULT 0,
    total_allowed               NUMBER(18,2)     DEFAULT 0,
    total_member_liability      NUMBER(18,2)     DEFAULT 0,
    -- PMPM metrics
    medical_pmpm                NUMBER(18,2)
                                COMMENT 'Per Member Per Month medical cost',
    pharmacy_pmpm               NUMBER(18,2),
    total_pmpm                  NUMBER(18,2),
    -- Audit
    etl_load_timestamp          TIMESTAMP_NTZ    DEFAULT CURRENT_TIMESTAMP()
)
DATA_RETENTION_TIME_IN_DAYS = 30
CLUSTER BY (member_id, year_month)
COPY GRANTS
COMMENT = 'Monthly member enrollment summary with utilization and cost metrics';


-- =============================================================================
-- MART_CLAIM_SUMMARY: aggregated claim summary by various dimensions
-- =============================================================================
CREATE OR REPLACE TABLE CLAIMS_DW.MART.MART_CLAIM_SUMMARY
(
    claim_summary_key           INTEGER          NOT NULL AUTOINCREMENT,
    summary_period              VARCHAR(7)       NOT NULL
                                COMMENT 'YYYY-MM format',
    calendar_year               INTEGER          NOT NULL,
    calendar_month              INTEGER          NOT NULL,
    claim_category              VARCHAR(20)      NOT NULL
                                COMMENT 'MEDICAL or PHARMACY',
    line_of_business            VARCHAR(20),
    product_type                VARCHAR(20),
    state_code                  VARCHAR(2),
    claim_type                  VARCHAR(10)      COMMENT 'P=Professional, I=Institutional (medical only)',
    place_of_service            VARCHAR(5),
    status_code                 VARCHAR(20),
    -- Volume metrics
    total_claims                INTEGER          NOT NULL DEFAULT 0,
    total_claim_lines           INTEGER          DEFAULT 0,
    unique_members              INTEGER          DEFAULT 0,
    unique_providers            INTEGER          DEFAULT 0,
    -- Financial metrics
    total_billed_amount         NUMBER(18,2)     DEFAULT 0,
    total_allowed_amount        NUMBER(18,2)     DEFAULT 0,
    total_paid_amount           NUMBER(18,2)     DEFAULT 0,
    total_net_paid_amount       NUMBER(18,2)     DEFAULT 0,
    total_copay                 NUMBER(18,2)     DEFAULT 0,
    total_coinsurance           NUMBER(18,2)     DEFAULT 0,
    total_deductible            NUMBER(18,2)     DEFAULT 0,
    total_member_liability      NUMBER(18,2)     DEFAULT 0,
    -- Averages
    avg_paid_per_claim          NUMBER(18,2),
    avg_allowed_per_claim       NUMBER(18,2),
    avg_member_liability        NUMBER(18,2),
    -- Denial / reversal metrics
    denied_claim_count          INTEGER          DEFAULT 0,
    reversed_claim_count        INTEGER          DEFAULT 0,
    denial_rate                 NUMBER(8,4)
                                COMMENT 'denied_claims / total_claims',
    -- Audit
    etl_load_timestamp          TIMESTAMP_NTZ    DEFAULT CURRENT_TIMESTAMP()
)
DATA_RETENTION_TIME_IN_DAYS = 30
CLUSTER BY (summary_period, claim_category)
COPY GRANTS
COMMENT = 'Aggregated claim summary by period, category, LOB, and geography';


-- =============================================================================
-- MART_ENCOUNTER_SUMMARY: encounter-level aggregation
-- =============================================================================
CREATE OR REPLACE TABLE CLAIMS_DW.MART.MART_ENCOUNTER_SUMMARY
(
    encounter_summary_key       INTEGER          NOT NULL AUTOINCREMENT,
    summary_period              VARCHAR(7)       NOT NULL
                                COMMENT 'YYYY-MM format',
    calendar_year               INTEGER          NOT NULL,
    calendar_month              INTEGER          NOT NULL,
    encounter_type              VARCHAR(20),
    line_of_business            VARCHAR(20),
    product_type                VARCHAR(20),
    state_code                  VARCHAR(2),
    drg_code                    VARCHAR(10),
    primary_diagnosis_code      VARCHAR(10),
    -- Volume
    total_encounters            INTEGER          NOT NULL DEFAULT 0,
    unique_members              INTEGER          DEFAULT 0,
    unique_providers            INTEGER          DEFAULT 0,
    -- Length of stay
    total_length_of_stay        INTEGER          DEFAULT 0,
    avg_length_of_stay          NUMBER(10,2),
    median_length_of_stay       NUMBER(10,2),
    max_length_of_stay          INTEGER,
    -- Cost
    total_paid_amount           NUMBER(18,2)     DEFAULT 0,
    total_allowed_amount        NUMBER(18,2)     DEFAULT 0,
    total_member_liability      NUMBER(18,2)     DEFAULT 0,
    avg_paid_per_encounter      NUMBER(18,2),
    avg_allowed_per_encounter   NUMBER(18,2),
    -- Readmission metrics
    readmission_count_30day     INTEGER          DEFAULT 0,
    readmission_rate_30day      NUMBER(8,4),
    -- Audit
    etl_load_timestamp          TIMESTAMP_NTZ    DEFAULT CURRENT_TIMESTAMP()
)
DATA_RETENTION_TIME_IN_DAYS = 30
CLUSTER BY (summary_period, encounter_type)
COPY GRANTS
COMMENT = 'Encounter-level aggregation with LOS and readmission metrics';


-- =============================================================================
-- MART_QUALITY_MEASURES: HEDIS and quality measure tracking
-- =============================================================================
CREATE OR REPLACE TABLE CLAIMS_DW.MART.MART_QUALITY_MEASURES
(
    quality_measure_key         INTEGER          NOT NULL AUTOINCREMENT,
    measure_id                  VARCHAR(30)      NOT NULL
                                COMMENT 'e.g., HEDIS-CDC-A1C, HEDIS-BCS, HEDIS-COL',
    measure_name                VARCHAR(200)     NOT NULL,
    measure_category            VARCHAR(50)
                                COMMENT 'Preventive, Chronic, Behavioral, Utilization',
    measurement_year            INTEGER          NOT NULL,
    measurement_period          VARCHAR(7)
                                COMMENT 'YYYY-MM for monthly tracking',
    line_of_business            VARCHAR(20),
    product_type                VARCHAR(20),
    state_code                  VARCHAR(2),
    -- Denominator / Numerator
    denominator_count           INTEGER          NOT NULL DEFAULT 0
                                COMMENT 'Eligible population for the measure',
    numerator_count             INTEGER          NOT NULL DEFAULT 0
                                COMMENT 'Members meeting the measure',
    exclusion_count             INTEGER          DEFAULT 0,
    -- Rate
    compliance_rate             NUMBER(8,4)
                                COMMENT 'numerator / denominator',
    target_rate                 NUMBER(8,4)
                                COMMENT 'Benchmark / target compliance rate',
    rate_gap                    NUMBER(8,4)
                                COMMENT 'target_rate - compliance_rate',
    star_rating                 NUMBER(3,1)
                                COMMENT 'CMS Star Rating equivalent (1.0-5.0)',
    -- Flexible detail storage (Snowflake VARIANT)
    measure_details             VARIANT
                                COMMENT 'JSON with measure-specific attributes and sub-metrics',
    -- Audit
    etl_load_timestamp          TIMESTAMP_NTZ    DEFAULT CURRENT_TIMESTAMP()
)
DATA_RETENTION_TIME_IN_DAYS = 30
CLUSTER BY (measurement_year, measure_id)
COPY GRANTS
COMMENT = 'HEDIS and quality measure tracking with compliance rates and star ratings';
