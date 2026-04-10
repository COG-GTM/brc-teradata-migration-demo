/*******************************************************************************
 * Healthcare Claims Teradata Migration Demo - Mart Tables
 *
 * Teradata-specific features used:
 *   - MULTISET tables
 *   - PRIMARY INDEX (hash distribution)
 *   - PARTITION BY RANGE_N (PPI)
 *   - COMPRESS values
 *   - COLLECT STATISTICS
 *
 * Tables:
 *   MART_MEMBER_MONTHS       - member month enrollment summary
 *   MART_CLAIM_SUMMARY       - claims aggregation by type/month
 *   MART_ENCOUNTER_SUMMARY   - encounter-level aggregation
 *   MART_QUALITY_MEASURES    - HEDIS-like quality measure results
 ******************************************************************************/

DATABASE CLAIMS_MART;

-- =============================================================================
-- MART_MEMBER_MONTHS: member month enrollment summary
-- One row per member per month of enrollment
-- Used for PMPM (Per Member Per Month) calculations
-- =============================================================================
CREATE MULTISET TABLE CLAIMS_MART.MART_MEMBER_MONTHS, FALLBACK,
    NO BEFORE JOURNAL,
    NO AFTER JOURNAL,
    CHECKSUM = DEFAULT,
    DEFAULT MERGEBLOCKRATIO
(
    member_id               VARCHAR(20)      CHARACTER SET LATIN NOT CASESPECIFIC NOT NULL,
    year_month              INTEGER          NOT NULL,
    year_number             SMALLINT         NOT NULL,
    month_number            SMALLINT         NOT NULL,
    plan_id                 VARCHAR(20)      CHARACTER SET LATIN NOT CASESPECIFIC,
    payer_id                VARCHAR(20)      CHARACTER SET LATIN NOT CASESPECIFIC,
    line_of_business        VARCHAR(20)      CHARACTER SET LATIN NOT CASESPECIFIC
                            COMPRESS ('COMMERCIAL', 'MEDICARE', 'MEDICAID', 'EXCHANGE'),
    gender                  CHAR(1)          CHARACTER SET LATIN NOT CASESPECIFIC
                            COMPRESS ('M', 'F', 'U'),
    age_at_month            SMALLINT,
    age_band                VARCHAR(10)      CHARACTER SET LATIN NOT CASESPECIFIC
                            COMPRESS ('0-17', '18-25', '26-34', '35-44', '45-54', '55-64', '65+'),
    state                   CHAR(2)          CHARACTER SET LATIN NOT CASESPECIFIC,
    group_id                VARCHAR(20)      CHARACTER SET LATIN NOT CASESPECIFIC,
    enrollment_days_in_month SMALLINT,
    is_full_month           CHAR(1)          CHARACTER SET LATIN NOT CASESPECIFIC
                            COMPRESS ('Y', 'N'),
    medical_claim_count     INTEGER          DEFAULT 0,
    pharmacy_claim_count    INTEGER          DEFAULT 0,
    medical_paid_amount     DECIMAL(18,2)    DEFAULT 0.00,
    pharmacy_paid_amount    DECIMAL(18,2)    DEFAULT 0.00,
    total_paid_amount       DECIMAL(18,2)    DEFAULT 0.00,
    medical_allowed_amount  DECIMAL(18,2)    DEFAULT 0.00,
    pharmacy_allowed_amount DECIMAL(18,2)    DEFAULT 0.00,
    total_allowed_amount    DECIMAL(18,2)    DEFAULT 0.00,
    member_oop_amount       DECIMAL(18,2)    DEFAULT 0.00,
    etl_batch_id            BIGINT,
    etl_loaded_ts           TIMESTAMP(6)     DEFAULT CURRENT_TIMESTAMP(6)
)
PRIMARY INDEX (member_id, year_month)
PARTITION BY RANGE_N(
    year_month BETWEEN 201501 AND 203012
    EACH 1
);

COMMENT ON TABLE CLAIMS_MART.MART_MEMBER_MONTHS
    AS 'Member month enrollment mart for PMPM and utilization reporting';

COLLECT STATISTICS
    COLUMN (member_id, year_month),
    COLUMN (member_id),
    COLUMN (year_month),
    COLUMN (year_number),
    COLUMN (line_of_business),
    COLUMN (payer_id),
    COLUMN (state),
    COLUMN (age_band),
    COLUMN (PARTITION)
ON CLAIMS_MART.MART_MEMBER_MONTHS;


-- =============================================================================
-- MART_CLAIM_SUMMARY: claims aggregation by type, month, and payer
-- Used for executive dashboards and trend analysis
-- =============================================================================
CREATE MULTISET TABLE CLAIMS_MART.MART_CLAIM_SUMMARY, FALLBACK,
    NO BEFORE JOURNAL,
    NO AFTER JOURNAL,
    CHECKSUM = DEFAULT,
    DEFAULT MERGEBLOCKRATIO
(
    year_month              INTEGER          NOT NULL,
    year_number             SMALLINT         NOT NULL,
    month_number            SMALLINT         NOT NULL,
    claim_type              VARCHAR(20)      CHARACTER SET LATIN NOT CASESPECIFIC
                            COMPRESS ('INPATIENT', 'OUTPATIENT', 'PROFESSIONAL', 'PHARMACY'),
    payer_id                VARCHAR(20)      CHARACTER SET LATIN NOT CASESPECIFIC,
    line_of_business        VARCHAR(20)      CHARACTER SET LATIN NOT CASESPECIFIC
                            COMPRESS ('COMMERCIAL', 'MEDICARE', 'MEDICAID', 'EXCHANGE'),
    state                   CHAR(2)          CHARACTER SET LATIN NOT CASESPECIFIC,
    member_count            INTEGER,
    claim_count             INTEGER,
    claim_line_count        INTEGER,
    total_charge_amount     DECIMAL(18,2),
    total_allowed_amount    DECIMAL(18,2),
    total_paid_amount       DECIMAL(18,2),
    total_member_oop        DECIMAL(18,2),
    avg_paid_per_claim      DECIMAL(18,2),
    avg_allowed_per_claim   DECIMAL(18,2),
    denied_claim_count      INTEGER          DEFAULT 0,
    reversed_claim_count    INTEGER          DEFAULT 0,
    denial_rate             DECIMAL(7,4),
    member_months           INTEGER,
    pmpm_paid               DECIMAL(18,2),
    pmpm_allowed            DECIMAL(18,2),
    etl_batch_id            BIGINT,
    etl_loaded_ts           TIMESTAMP(6)     DEFAULT CURRENT_TIMESTAMP(6)
)
PRIMARY INDEX (year_month, claim_type, payer_id);

COMMENT ON TABLE CLAIMS_MART.MART_CLAIM_SUMMARY
    AS 'Claims aggregation by type/month/payer for executive dashboards and trend analysis';

COLLECT STATISTICS
    COLUMN (year_month, claim_type, payer_id),
    COLUMN (year_month),
    COLUMN (claim_type),
    COLUMN (payer_id),
    COLUMN (line_of_business),
    COLUMN (year_number)
ON CLAIMS_MART.MART_CLAIM_SUMMARY;


-- =============================================================================
-- MART_ENCOUNTER_SUMMARY: encounter-level aggregation
-- Used for utilization management and length-of-stay analysis
-- =============================================================================
CREATE MULTISET TABLE CLAIMS_MART.MART_ENCOUNTER_SUMMARY, FALLBACK,
    NO BEFORE JOURNAL,
    NO AFTER JOURNAL,
    CHECKSUM = DEFAULT,
    DEFAULT MERGEBLOCKRATIO
(
    year_month              INTEGER          NOT NULL,
    year_number             SMALLINT         NOT NULL,
    month_number            SMALLINT         NOT NULL,
    encounter_type          VARCHAR(20)      CHARACTER SET LATIN NOT CASESPECIFIC
                            COMPRESS ('INPATIENT', 'OUTPATIENT', 'PROFESSIONAL', 'EMERGENCY'),
    payer_id                VARCHAR(20)      CHARACTER SET LATIN NOT CASESPECIFIC,
    line_of_business        VARCHAR(20)      CHARACTER SET LATIN NOT CASESPECIFIC
                            COMPRESS ('COMMERCIAL', 'MEDICARE', 'MEDICAID', 'EXCHANGE'),
    primary_diagnosis_code  VARCHAR(10)      CHARACTER SET LATIN NOT CASESPECIFIC,
    ms_drg                  VARCHAR(5)       CHARACTER SET LATIN NOT CASESPECIFIC,
    encounter_count         INTEGER,
    member_count            INTEGER,
    total_paid_amount       DECIMAL(18,2),
    total_allowed_amount    DECIMAL(18,2),
    total_charge_amount     DECIMAL(18,2),
    avg_paid_per_encounter  DECIMAL(18,2),
    avg_length_of_stay      DECIMAL(7,2),
    median_length_of_stay   DECIMAL(7,2),
    readmission_count       INTEGER          DEFAULT 0,
    readmission_rate        DECIMAL(7,4),
    avg_claims_per_encounter DECIMAL(7,2),
    etl_batch_id            BIGINT,
    etl_loaded_ts           TIMESTAMP(6)     DEFAULT CURRENT_TIMESTAMP(6)
)
PRIMARY INDEX (year_month, encounter_type, payer_id);

COMMENT ON TABLE CLAIMS_MART.MART_ENCOUNTER_SUMMARY
    AS 'Encounter-level aggregation for utilization management and LOS analysis';

COLLECT STATISTICS
    COLUMN (year_month, encounter_type, payer_id),
    COLUMN (year_month),
    COLUMN (encounter_type),
    COLUMN (primary_diagnosis_code),
    COLUMN (ms_drg),
    COLUMN (year_number)
ON CLAIMS_MART.MART_ENCOUNTER_SUMMARY;


-- =============================================================================
-- MART_QUALITY_MEASURES: HEDIS-like quality measure results
-- Used for quality reporting, star ratings, and regulatory compliance
-- =============================================================================
CREATE MULTISET TABLE CLAIMS_MART.MART_QUALITY_MEASURES, FALLBACK,
    NO BEFORE JOURNAL,
    NO AFTER JOURNAL,
    CHECKSUM = DEFAULT,
    DEFAULT MERGEBLOCKRATIO
(
    measure_id              VARCHAR(20)      CHARACTER SET LATIN NOT CASESPECIFIC NOT NULL,
    measure_name            VARCHAR(200)     CHARACTER SET LATIN NOT CASESPECIFIC,
    measure_category        VARCHAR(50)      CHARACTER SET LATIN NOT CASESPECIFIC
                            COMPRESS ('PREVENTIVE', 'CHRONIC', 'BEHAVIORAL', 'ACCESS', 'UTILIZATION'),
    measurement_year        SMALLINT         NOT NULL,
    payer_id                VARCHAR(20)      CHARACTER SET LATIN NOT CASESPECIFIC,
    line_of_business        VARCHAR(20)      CHARACTER SET LATIN NOT CASESPECIFIC
                            COMPRESS ('COMMERCIAL', 'MEDICARE', 'MEDICAID', 'EXCHANGE'),
    eligible_population     INTEGER,
    numerator_count         INTEGER,
    denominator_count       INTEGER,
    exclusion_count         INTEGER          DEFAULT 0,
    rate                    DECIMAL(7,4),
    benchmark_25th          DECIMAL(7,4),
    benchmark_50th          DECIMAL(7,4),
    benchmark_75th          DECIMAL(7,4),
    benchmark_90th          DECIMAL(7,4),
    star_rating             SMALLINT         COMPRESS (1, 2, 3, 4, 5),
    performance_level       VARCHAR(20)      CHARACTER SET LATIN NOT CASESPECIFIC
                            COMPRESS ('BELOW_AVG', 'AVERAGE', 'ABOVE_AVG', 'EXCELLENT'),
    etl_batch_id            BIGINT,
    etl_loaded_ts           TIMESTAMP(6)     DEFAULT CURRENT_TIMESTAMP(6)
)
PRIMARY INDEX (measure_id, measurement_year, payer_id);

COMMENT ON TABLE CLAIMS_MART.MART_QUALITY_MEASURES
    AS 'HEDIS-like quality measure results for star ratings and regulatory reporting';

COLLECT STATISTICS
    COLUMN (measure_id, measurement_year, payer_id),
    COLUMN (measure_id),
    COLUMN (measurement_year),
    COLUMN (payer_id),
    COLUMN (line_of_business),
    COLUMN (measure_category),
    COLUMN (star_rating)
ON CLAIMS_MART.MART_QUALITY_MEASURES;
