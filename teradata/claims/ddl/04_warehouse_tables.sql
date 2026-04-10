/*******************************************************************************
 * Healthcare Claims Teradata Migration Demo - Warehouse Tables
 *
 * Teradata-specific features used:
 *   - PERIOD data type for SCD Type 2 temporal tracking
 *   - PRIMARY INDEX (hash distribution)
 *   - PARTITION BY RANGE_N (PPI)
 *   - COMPRESS values
 *   - COLLECT STATISTICS
 *   - IDENTITY columns (surrogate keys)
 *   - FORMAT patterns
 *
 * Tables:
 *   DIM_MEMBER          - member dimension (SCD Type 2 with PERIOD)
 *   DIM_PROVIDER        - provider dimension
 *   DIM_DATE            - calendar/date dimension
 *   FCT_MEDICAL_CLAIM   - medical claims fact table
 *   FCT_PHARMACY_CLAIM  - pharmacy claims fact table
 *   FCT_ENCOUNTER       - encounter grouping fact table
 ******************************************************************************/

DATABASE CLAIMS_DWH;

-- =============================================================================
-- DIM_MEMBER: member dimension with SCD Type 2 using PERIOD data type
-- Tracks historical changes to member demographics and enrollment
-- =============================================================================
CREATE SET TABLE CLAIMS_DWH.DIM_MEMBER, FALLBACK,
    NO BEFORE JOURNAL,
    NO AFTER JOURNAL,
    CHECKSUM = DEFAULT,
    DEFAULT MERGEBLOCKRATIO
(
    member_sk               INTEGER          GENERATED ALWAYS AS IDENTITY
                            (START WITH 1 INCREMENT BY 1),
    member_id               VARCHAR(20)      CHARACTER SET LATIN NOT CASESPECIFIC NOT NULL,
    plan_id                 VARCHAR(20)      CHARACTER SET LATIN NOT CASESPECIFIC,
    payer_id                VARCHAR(20)      CHARACTER SET LATIN NOT CASESPECIFIC,
    gender                  CHAR(1)          CHARACTER SET LATIN NOT CASESPECIFIC
                            COMPRESS ('M', 'F', 'U'),
    date_of_birth           DATE FORMAT 'YYYY-MM-DD',
    race                    VARCHAR(30)      CHARACTER SET LATIN NOT CASESPECIFIC
                            COMPRESS ('WHITE', 'BLACK', 'HISPANIC', 'ASIAN', 'OTHER', 'UNKNOWN'),
    zip_code                CHAR(5)          CHARACTER SET LATIN NOT CASESPECIFIC,
    state                   CHAR(2)          CHARACTER SET LATIN NOT CASESPECIFIC,
    relation_to_subscriber  VARCHAR(10)      CHARACTER SET LATIN NOT CASESPECIFIC
                            COMPRESS ('SELF', 'SPOUSE', 'CHILD', 'OTHER'),
    subscriber_id           VARCHAR(20)      CHARACTER SET LATIN NOT CASESPECIFIC,
    group_id                VARCHAR(20)      CHARACTER SET LATIN NOT CASESPECIFIC,
    line_of_business        VARCHAR(20)      CHARACTER SET LATIN NOT CASESPECIFIC
                            COMPRESS ('COMMERCIAL', 'MEDICARE', 'MEDICAID', 'EXCHANGE'),
    age_band                VARCHAR(10)      CHARACTER SET LATIN NOT CASESPECIFIC
                            COMPRESS ('0-17', '18-25', '26-34', '35-44', '45-54', '55-64', '65+'),
    /* SCD Type 2 fields using Teradata PERIOD data type */
    validity_period         PERIOD(DATE)     NOT NULL,
    is_current              CHAR(1)          CHARACTER SET LATIN NOT CASESPECIFIC
                            COMPRESS ('Y', 'N') DEFAULT 'Y',
    effective_from          DATE FORMAT 'YYYY-MM-DD' NOT NULL,
    effective_to            DATE FORMAT 'YYYY-MM-DD' DEFAULT DATE '9999-12-31',
    etl_batch_id            BIGINT,
    etl_loaded_ts           TIMESTAMP(6)     DEFAULT CURRENT_TIMESTAMP(6)
)
PRIMARY INDEX (member_id);

COMMENT ON TABLE CLAIMS_DWH.DIM_MEMBER
    AS 'Member dimension - SCD Type 2 with PERIOD data type for temporal tracking';

COLLECT STATISTICS
    COLUMN (member_sk),
    COLUMN (member_id),
    COLUMN (is_current),
    COLUMN (member_id, is_current),
    COLUMN (state),
    COLUMN (line_of_business),
    COLUMN (gender)
ON CLAIMS_DWH.DIM_MEMBER;


-- =============================================================================
-- DIM_PROVIDER: provider dimension (NPI-based)
-- =============================================================================
CREATE SET TABLE CLAIMS_DWH.DIM_PROVIDER, FALLBACK,
    NO BEFORE JOURNAL,
    NO AFTER JOURNAL,
    CHECKSUM = DEFAULT,
    DEFAULT MERGEBLOCKRATIO
(
    provider_sk             INTEGER          GENERATED ALWAYS AS IDENTITY
                            (START WITH 1 INCREMENT BY 1),
    npi                     VARCHAR(10)      CHARACTER SET LATIN NOT CASESPECIFIC NOT NULL,
    provider_name           VARCHAR(200)     CHARACTER SET LATIN NOT CASESPECIFIC,
    provider_type           VARCHAR(50)      CHARACTER SET LATIN NOT CASESPECIFIC
                            COMPRESS ('INDIVIDUAL', 'ORGANIZATION'),
    taxonomy_code           VARCHAR(20)      CHARACTER SET LATIN NOT CASESPECIFIC,
    specialty               VARCHAR(100)     CHARACTER SET LATIN NOT CASESPECIFIC,
    address_line_1          VARCHAR(200)     CHARACTER SET LATIN NOT CASESPECIFIC,
    city                    VARCHAR(100)     CHARACTER SET LATIN NOT CASESPECIFIC,
    state                   CHAR(2)          CHARACTER SET LATIN NOT CASESPECIFIC,
    zip_code                CHAR(5)          CHARACTER SET LATIN NOT CASESPECIFIC,
    phone_number            VARCHAR(20)      CHARACTER SET LATIN NOT CASESPECIFIC,
    par_status              CHAR(1)          CHARACTER SET LATIN NOT CASESPECIFIC
                            COMPRESS ('Y', 'N') DEFAULT 'N',
    is_current              CHAR(1)          CHARACTER SET LATIN NOT CASESPECIFIC
                            COMPRESS ('Y', 'N') DEFAULT 'Y',
    etl_batch_id            BIGINT,
    etl_loaded_ts           TIMESTAMP(6)     DEFAULT CURRENT_TIMESTAMP(6)
)
PRIMARY INDEX (npi);

COMMENT ON TABLE CLAIMS_DWH.DIM_PROVIDER
    AS 'Provider dimension keyed by NPI with specialty and par status';

COLLECT STATISTICS
    COLUMN (provider_sk),
    COLUMN (npi),
    COLUMN (is_current),
    COLUMN (specialty),
    COLUMN (state)
ON CLAIMS_DWH.DIM_PROVIDER;


-- =============================================================================
-- DIM_DATE: calendar/date dimension
-- Pre-populated for all dates needed by the data warehouse
-- =============================================================================
CREATE SET TABLE CLAIMS_DWH.DIM_DATE, FALLBACK,
    NO BEFORE JOURNAL,
    NO AFTER JOURNAL,
    CHECKSUM = DEFAULT,
    DEFAULT MERGEBLOCKRATIO
(
    date_key                INTEGER          NOT NULL,
    calendar_date           DATE FORMAT 'YYYY-MM-DD' NOT NULL,
    day_of_week             SMALLINT,
    day_name                VARCHAR(10)      CHARACTER SET LATIN NOT CASESPECIFIC,
    day_of_month            SMALLINT,
    day_of_year             SMALLINT,
    week_of_year            SMALLINT,
    month_number            SMALLINT,
    month_name              VARCHAR(10)      CHARACTER SET LATIN NOT CASESPECIFIC,
    month_start_date        DATE FORMAT 'YYYY-MM-DD',
    month_end_date          DATE FORMAT 'YYYY-MM-DD',
    quarter_number          SMALLINT         COMPRESS (1, 2, 3, 4),
    quarter_name            VARCHAR(5)       CHARACTER SET LATIN NOT CASESPECIFIC
                            COMPRESS ('Q1', 'Q2', 'Q3', 'Q4'),
    year_number             SMALLINT,
    year_month              INTEGER,
    year_quarter            VARCHAR(7)       CHARACTER SET LATIN NOT CASESPECIFIC,
    is_weekday              CHAR(1)          CHARACTER SET LATIN NOT CASESPECIFIC
                            COMPRESS ('Y', 'N'),
    is_holiday              CHAR(1)          CHARACTER SET LATIN NOT CASESPECIFIC
                            COMPRESS ('Y', 'N') DEFAULT 'N',
    fiscal_year             SMALLINT,
    fiscal_quarter          SMALLINT
)
UNIQUE PRIMARY INDEX (date_key);

COMMENT ON TABLE CLAIMS_DWH.DIM_DATE
    AS 'Calendar dimension with fiscal year alignment and holiday flags';

COLLECT STATISTICS
    COLUMN (date_key),
    COLUMN (calendar_date),
    COLUMN (year_month),
    COLUMN (year_number),
    COLUMN (month_number)
ON CLAIMS_DWH.DIM_DATE;


-- =============================================================================
-- FCT_MEDICAL_CLAIM: medical claims fact table
-- Partitioned by service date key (monthly)
-- =============================================================================
CREATE MULTISET TABLE CLAIMS_DWH.FCT_MEDICAL_CLAIM, FALLBACK,
    NO BEFORE JOURNAL,
    NO AFTER JOURNAL,
    CHECKSUM = DEFAULT,
    DEFAULT MERGEBLOCKRATIO
(
    claim_id                VARCHAR(30)      CHARACTER SET LATIN NOT CASESPECIFIC NOT NULL,
    claim_line_number       SMALLINT         NOT NULL,
    member_sk               INTEGER          NOT NULL,
    billing_provider_sk     INTEGER,
    rendering_provider_sk   INTEGER,
    facility_provider_sk    INTEGER,
    service_date_from_key   INTEGER          NOT NULL,
    service_date_to_key     INTEGER,
    admission_date_key      INTEGER,
    discharge_date_key      INTEGER,
    claim_type              VARCHAR(5)       CHARACTER SET LATIN NOT CASESPECIFIC
                            COMPRESS ('I', 'P', 'O'),
    place_of_service        VARCHAR(5)       CHARACTER SET LATIN NOT CASESPECIFIC,
    bill_type               VARCHAR(4)       CHARACTER SET LATIN NOT CASESPECIFIC,
    revenue_center_code     VARCHAR(10)      CHARACTER SET LATIN NOT CASESPECIFIC,
    ms_drg                  VARCHAR(5)       CHARACTER SET LATIN NOT CASESPECIFIC,
    apr_drg                 VARCHAR(5)       CHARACTER SET LATIN NOT CASESPECIFIC,
    hcpcs_code              VARCHAR(10)      CHARACTER SET LATIN NOT CASESPECIFIC,
    cpt_code                VARCHAR(10)      CHARACTER SET LATIN NOT CASESPECIFIC,
    icd_diagnosis_code_1    VARCHAR(10)      CHARACTER SET LATIN NOT CASESPECIFIC,
    icd_diagnosis_code_2    VARCHAR(10)      CHARACTER SET LATIN NOT CASESPECIFIC,
    icd_diagnosis_code_3    VARCHAR(10)      CHARACTER SET LATIN NOT CASESPECIFIC,
    admit_type              VARCHAR(5)       CHARACTER SET LATIN NOT CASESPECIFIC,
    discharge_disposition   VARCHAR(5)       CHARACTER SET LATIN NOT CASESPECIFIC,
    claim_status            VARCHAR(10)      CHARACTER SET LATIN NOT CASESPECIFIC
                            COMPRESS ('PAID', 'ADJUSTED', 'DENIED', 'REVERSED'),
    paid_amount             DECIMAL(18,2),
    charge_amount           DECIMAL(18,2),
    allowed_amount          DECIMAL(18,2),
    coinsurance             DECIMAL(18,2),
    copay                   DECIMAL(18,2),
    deductible              DECIMAL(18,2),
    plan_paid_amount        DECIMAL(18,2),
    member_oop              DECIMAL(18,2),
    payer_id                VARCHAR(20)      CHARACTER SET LATIN NOT CASESPECIFIC,
    encounter_id            VARCHAR(50)      CHARACTER SET LATIN NOT CASESPECIFIC,
    etl_batch_id            BIGINT,
    etl_loaded_ts           TIMESTAMP(6)     DEFAULT CURRENT_TIMESTAMP(6)
)
PRIMARY INDEX (claim_id, claim_line_number)
PARTITION BY RANGE_N(
    service_date_from_key BETWEEN 20150101 AND 20301231
    EACH 100   /* approximately monthly partitions on YYYYMMDD integer */
);

COMMENT ON TABLE CLAIMS_DWH.FCT_MEDICAL_CLAIM
    AS 'Medical claims fact table with surrogate keys to dimensions';

COLLECT STATISTICS
    COLUMN (claim_id, claim_line_number),
    COLUMN (member_sk),
    COLUMN (service_date_from_key),
    COLUMN (billing_provider_sk),
    COLUMN (claim_type),
    COLUMN (claim_status),
    COLUMN (encounter_id),
    COLUMN (PARTITION)
ON CLAIMS_DWH.FCT_MEDICAL_CLAIM;


-- =============================================================================
-- FCT_PHARMACY_CLAIM: pharmacy claims fact table
-- =============================================================================
CREATE MULTISET TABLE CLAIMS_DWH.FCT_PHARMACY_CLAIM, FALLBACK,
    NO BEFORE JOURNAL,
    NO AFTER JOURNAL,
    CHECKSUM = DEFAULT,
    DEFAULT MERGEBLOCKRATIO
(
    claim_id                VARCHAR(30)      CHARACTER SET LATIN NOT CASESPECIFIC NOT NULL,
    member_sk               INTEGER          NOT NULL,
    dispensing_provider_sk  INTEGER,
    prescribing_provider_sk INTEGER,
    dispensing_date_key     INTEGER          NOT NULL,
    ndc_code                VARCHAR(11)      CHARACTER SET LATIN NOT CASESPECIFIC NOT NULL,
    quantity                DECIMAL(10,2),
    days_supply             SMALLINT,
    refill_number           SMALLINT,
    claim_status            VARCHAR(10)      CHARACTER SET LATIN NOT CASESPECIFIC
                            COMPRESS ('PAID', 'ADJUSTED', 'DENIED', 'REVERSED'),
    paid_amount             DECIMAL(18,2),
    charge_amount           DECIMAL(18,2),
    allowed_amount          DECIMAL(18,2),
    copay                   DECIMAL(18,2),
    coinsurance             DECIMAL(18,2),
    deductible              DECIMAL(18,2),
    plan_paid               DECIMAL(18,2),
    member_oop              DECIMAL(18,2),
    payer_id                VARCHAR(20)      CHARACTER SET LATIN NOT CASESPECIFIC,
    etl_batch_id            BIGINT,
    etl_loaded_ts           TIMESTAMP(6)     DEFAULT CURRENT_TIMESTAMP(6)
)
PRIMARY INDEX (claim_id)
PARTITION BY RANGE_N(
    dispensing_date_key BETWEEN 20150101 AND 20301231
    EACH 100
);

COMMENT ON TABLE CLAIMS_DWH.FCT_PHARMACY_CLAIM
    AS 'Pharmacy claims fact table with surrogate keys to member and provider dimensions';

COLLECT STATISTICS
    COLUMN (claim_id),
    COLUMN (member_sk),
    COLUMN (dispensing_date_key),
    COLUMN (ndc_code),
    COLUMN (claim_status),
    COLUMN (PARTITION)
ON CLAIMS_DWH.FCT_PHARMACY_CLAIM;


-- =============================================================================
-- FCT_ENCOUNTER: encounter grouping fact table
-- Merges overlapping service dates into encounter episodes
-- =============================================================================
CREATE MULTISET TABLE CLAIMS_DWH.FCT_ENCOUNTER, FALLBACK,
    NO BEFORE JOURNAL,
    NO AFTER JOURNAL,
    CHECKSUM = DEFAULT,
    DEFAULT MERGEBLOCKRATIO
(
    encounter_id            VARCHAR(50)      CHARACTER SET LATIN NOT CASESPECIFIC NOT NULL,
    member_sk               INTEGER          NOT NULL,
    encounter_start_date    DATE FORMAT 'YYYY-MM-DD' NOT NULL,
    encounter_end_date      DATE FORMAT 'YYYY-MM-DD',
    encounter_period        PERIOD(DATE),
    encounter_type          VARCHAR(20)      CHARACTER SET LATIN NOT CASESPECIFIC
                            COMPRESS ('INPATIENT', 'OUTPATIENT', 'PROFESSIONAL', 'EMERGENCY'),
    primary_diagnosis_code  VARCHAR(10)      CHARACTER SET LATIN NOT CASESPECIFIC,
    primary_ms_drg          VARCHAR(5)       CHARACTER SET LATIN NOT CASESPECIFIC,
    facility_provider_sk    INTEGER,
    attending_provider_sk   INTEGER,
    claim_count             SMALLINT,
    claim_line_count        SMALLINT,
    total_paid_amount       DECIMAL(18,2),
    total_charge_amount     DECIMAL(18,2),
    total_allowed_amount    DECIMAL(18,2),
    total_member_oop        DECIMAL(18,2),
    length_of_stay          SMALLINT,
    payer_id                VARCHAR(20)      CHARACTER SET LATIN NOT CASESPECIFIC,
    etl_batch_id            BIGINT,
    etl_loaded_ts           TIMESTAMP(6)     DEFAULT CURRENT_TIMESTAMP(6)
)
PRIMARY INDEX (encounter_id);

COMMENT ON TABLE CLAIMS_DWH.FCT_ENCOUNTER
    AS 'Encounter-level fact table grouping overlapping claims into episodes of care';

COLLECT STATISTICS
    COLUMN (encounter_id),
    COLUMN (member_sk),
    COLUMN (encounter_start_date),
    COLUMN (encounter_type),
    COLUMN (primary_diagnosis_code),
    COLUMN (facility_provider_sk)
ON CLAIMS_DWH.FCT_ENCOUNTER;


-- =============================================================================
-- ETL_ERROR_LOG: error logging for stored procedure execution
-- =============================================================================
CREATE MULTISET TABLE CLAIMS_DWH.ETL_ERROR_LOG, FALLBACK,
    NO BEFORE JOURNAL,
    NO AFTER JOURNAL,
    CHECKSUM = DEFAULT,
    DEFAULT MERGEBLOCKRATIO
(
    error_id                INTEGER          GENERATED ALWAYS AS IDENTITY
                            (START WITH 1 INCREMENT BY 1),
    procedure_name          VARCHAR(100)     CHARACTER SET LATIN NOT CASESPECIFIC,
    error_code              INTEGER,
    error_state             CHAR(5)          CHARACTER SET LATIN NOT CASESPECIFIC,
    error_message           VARCHAR(500)     CHARACTER SET LATIN NOT CASESPECIFIC,
    error_ts                TIMESTAMP(6)     DEFAULT CURRENT_TIMESTAMP(6),
    batch_id                BIGINT
)
PRIMARY INDEX (error_id);

COMMENT ON TABLE CLAIMS_DWH.ETL_ERROR_LOG
    AS 'ETL error logging for stored procedure execution tracking';
