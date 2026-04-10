/*******************************************************************************
 * Healthcare Claims Data Warehouse - Warehouse Tables (Snowflake)
 *
 * Snowflake-specific features used:
 *   - CLUSTER BY for micro-partition pruning
 *   - COPY GRANTS
 *   - DATA_RETENTION_TIME_IN_DAYS (Time Travel)
 *   - NUMBER / VARCHAR / BOOLEAN / DATE / TIMESTAMP_NTZ types
 *   - COMMENT on tables and columns
 *
 * Dimensional model:
 *   DIM_MEMBER      - SCD Type 2 member dimension
 *   DIM_PROVIDER     - Provider reference dimension
 *   DIM_DATE         - Calendar date dimension
 *   FCT_MEDICAL_CLAIM  - Medical claims fact
 *   FCT_PHARMACY_CLAIM - Pharmacy claims fact
 *   FCT_ENCOUNTER      - Grouped encounter fact
 ******************************************************************************/

USE DATABASE CLAIMS_DW;
USE SCHEMA WAREHOUSE;

-- =============================================================================
-- DIM_MEMBER: SCD Type 2 member dimension
-- Tracks historical changes to member attributes over time
-- =============================================================================
CREATE OR REPLACE TABLE CLAIMS_DW.WAREHOUSE.DIM_MEMBER
(
    member_key                  INTEGER          NOT NULL AUTOINCREMENT
                                COMMENT 'Surrogate key',
    member_id                   VARCHAR(20)      NOT NULL
                                COMMENT 'Natural / business key',
    subscriber_id               VARCHAR(20),
    first_name                  VARCHAR(100),
    last_name                   VARCHAR(100),
    date_of_birth               DATE,
    gender                      VARCHAR(1),
    age_band                    VARCHAR(20)
                                COMMENT 'Derived: 0-17, 18-25, 26-35, 36-45, 46-55, 56-64, 65+',
    state_code                  VARCHAR(2),
    zip_code                    VARCHAR(10),
    plan_code                   VARCHAR(20),
    plan_name                   VARCHAR(200),
    product_type                VARCHAR(20),
    line_of_business            VARCHAR(20),
    group_number                VARCHAR(30),
    group_name                  VARCHAR(200),
    pcp_provider_id             VARCHAR(20),
    coverage_type               VARCHAR(20),
    relationship_code           VARCHAR(5),
    -- SCD Type 2 tracking columns
    effective_date              DATE             NOT NULL,
    expiration_date             DATE             NOT NULL DEFAULT '9999-12-31',
    is_current                  BOOLEAN          NOT NULL DEFAULT TRUE,
    record_hash                 VARCHAR(64)      COMMENT 'Hash of SCD-tracked columns for change detection',
    created_timestamp           TIMESTAMP_NTZ    DEFAULT CURRENT_TIMESTAMP(),
    updated_timestamp           TIMESTAMP_NTZ    DEFAULT CURRENT_TIMESTAMP()
)
DATA_RETENTION_TIME_IN_DAYS = 30
CLUSTER BY (member_id, is_current)
COPY GRANTS
COMMENT = 'SCD Type 2 member dimension - tracks historical attribute changes';


-- =============================================================================
-- DIM_PROVIDER: provider reference dimension
-- =============================================================================
CREATE OR REPLACE TABLE CLAIMS_DW.WAREHOUSE.DIM_PROVIDER
(
    provider_key                INTEGER          NOT NULL AUTOINCREMENT
                                COMMENT 'Surrogate key',
    provider_id                 VARCHAR(20)      NOT NULL
                                COMMENT 'Natural / business key',
    provider_npi                VARCHAR(10),
    provider_name               VARCHAR(200),
    provider_type               VARCHAR(50)
                                COMMENT 'Individual, Group, Facility, Ancillary',
    specialty_code              VARCHAR(20),
    specialty_description       VARCHAR(200),
    taxonomy_code               VARCHAR(20),
    network_status              VARCHAR(20)
                                COMMENT 'IN_NETWORK, OUT_OF_NETWORK, TIER1, TIER2',
    network_effective_date      DATE,
    network_termination_date    DATE,
    practice_address_line_1     VARCHAR(200),
    practice_city               VARCHAR(100),
    practice_state              VARCHAR(2),
    practice_zip                VARCHAR(10),
    phone_number                VARCHAR(20),
    accepting_patients_flag     BOOLEAN,
    par_flag                    BOOLEAN          COMMENT 'Participating provider flag',
    is_current                  BOOLEAN          NOT NULL DEFAULT TRUE,
    created_timestamp           TIMESTAMP_NTZ    DEFAULT CURRENT_TIMESTAMP(),
    updated_timestamp           TIMESTAMP_NTZ    DEFAULT CURRENT_TIMESTAMP()
)
DATA_RETENTION_TIME_IN_DAYS = 30
CLUSTER BY (provider_id)
COPY GRANTS
COMMENT = 'Provider reference dimension with network status';


-- =============================================================================
-- DIM_DATE: calendar date dimension
-- =============================================================================
CREATE OR REPLACE TABLE CLAIMS_DW.WAREHOUSE.DIM_DATE
(
    date_key                    INTEGER          NOT NULL
                                COMMENT 'YYYYMMDD integer key',
    calendar_date               DATE             NOT NULL,
    year                        INTEGER          NOT NULL,
    quarter                     INTEGER          NOT NULL,
    month                       INTEGER          NOT NULL,
    month_name                  VARCHAR(15),
    day_of_month                INTEGER,
    day_of_week                 INTEGER,
    day_name                    VARCHAR(15),
    week_of_year                INTEGER,
    is_weekend                  BOOLEAN,
    is_holiday                  BOOLEAN          DEFAULT FALSE,
    holiday_name                VARCHAR(100),
    fiscal_year                 INTEGER,
    fiscal_quarter              INTEGER,
    fiscal_month                INTEGER,
    -- Healthcare-specific date attributes
    is_business_day             BOOLEAN,
    calendar_year_month         VARCHAR(7)       COMMENT 'YYYY-MM format',
    calendar_year_quarter       VARCHAR(7)       COMMENT 'YYYY-Q# format',
    days_in_month               INTEGER,
    is_month_end                BOOLEAN,
    is_quarter_end              BOOLEAN,
    is_year_end                 BOOLEAN
)
DATA_RETENTION_TIME_IN_DAYS = 7
CLUSTER BY (calendar_date)
COPY GRANTS
COMMENT = 'Calendar date dimension with fiscal and healthcare attributes';


-- =============================================================================
-- FCT_MEDICAL_CLAIM: medical claims fact table
-- =============================================================================
CREATE OR REPLACE TABLE CLAIMS_DW.WAREHOUSE.FCT_MEDICAL_CLAIM
(
    medical_claim_key           INTEGER          NOT NULL AUTOINCREMENT,
    claim_id                    VARCHAR(30)      NOT NULL,
    claim_line_number           INTEGER          NOT NULL,
    member_key                  INTEGER          NOT NULL
                                COMMENT 'FK to DIM_MEMBER',
    member_id                   VARCHAR(20)      NOT NULL
                                COMMENT 'Degenerate dimension',
    rendering_provider_key      INTEGER
                                COMMENT 'FK to DIM_PROVIDER',
    billing_provider_key        INTEGER
                                COMMENT 'FK to DIM_PROVIDER',
    -- Date keys
    start_date_key              INTEGER          COMMENT 'FK to DIM_DATE',
    end_date_key                INTEGER,
    admission_date_key          INTEGER,
    discharge_date_key          INTEGER,
    adjudication_date_key       INTEGER,
    -- Degenerate dimensions
    status_code                 VARCHAR(20),
    claim_type                  VARCHAR(10),
    place_of_service            VARCHAR(5),
    bill_type                   VARCHAR(5),
    cpt_code                    VARCHAR(10),
    cpt_modifier_1              VARCHAR(5),
    revenue_code                VARCHAR(10),
    drg_code                    VARCHAR(10),
    -- Diagnosis codes (hybrid - Snowflake unique)
    diagnosis_codes             VARIANT,
    primary_diagnosis_code      VARCHAR(10),
    -- Measures
    billed_amount               NUMBER(18,2),
    allowed_amount              NUMBER(18,2),
    paid_amount                 NUMBER(18,2),
    net_paid_amount             NUMBER(18,2),
    copay_amount                NUMBER(18,2),
    coinsurance_amount          NUMBER(18,2),
    deductible_amount           NUMBER(18,2),
    cob_amount                  NUMBER(18,2),
    member_liability            NUMBER(18,2)
                                COMMENT 'copay + coinsurance + deductible',
    units                       NUMBER(10,2),
    -- Encounter reference
    encounter_id                VARCHAR(50),
    -- Audit
    source_system               VARCHAR(50),
    etl_load_timestamp          TIMESTAMP_NTZ    DEFAULT CURRENT_TIMESTAMP()
)
DATA_RETENTION_TIME_IN_DAYS = 30
CLUSTER BY (member_id, start_date_key)
COPY GRANTS
COMMENT = 'Medical claims fact table with hybrid diagnosis codes';


-- =============================================================================
-- FCT_PHARMACY_CLAIM: pharmacy claims fact table
-- =============================================================================
CREATE OR REPLACE TABLE CLAIMS_DW.WAREHOUSE.FCT_PHARMACY_CLAIM
(
    pharmacy_claim_key          INTEGER          NOT NULL AUTOINCREMENT,
    claim_id                    VARCHAR(30)      NOT NULL,
    member_key                  INTEGER          NOT NULL
                                COMMENT 'FK to DIM_MEMBER',
    member_id                   VARCHAR(20)      NOT NULL,
    prescribing_provider_key    INTEGER
                                COMMENT 'FK to DIM_PROVIDER',
    -- Date keys
    fill_date_key               INTEGER          NOT NULL
                                COMMENT 'FK to DIM_DATE',
    written_date_key            INTEGER,
    -- Degenerate dimensions
    status_code                 VARCHAR(20),
    ndc_code                    VARCHAR(11),
    drug_name                   VARCHAR(200),
    generic_name                VARCHAR(200),
    therapeutic_class_code      VARCHAR(20),
    therapeutic_class_desc      VARCHAR(200),
    gpi_code                    VARCHAR(20),
    pharmacy_id                 VARCHAR(20),
    pharmacy_name               VARCHAR(200),
    mail_order_flag             BOOLEAN,
    daw_code                    VARCHAR(5),
    formulary_flag              BOOLEAN,
    prior_auth_flag             BOOLEAN,
    -- Measures
    quantity_dispensed           NUMBER(10,3),
    days_supply                 INTEGER,
    refill_number               INTEGER,
    billed_amount               NUMBER(18,2),
    allowed_amount              NUMBER(18,2),
    paid_amount                 NUMBER(18,2),
    net_paid_amount             NUMBER(18,2),
    copay_amount                NUMBER(18,2),
    coinsurance_amount          NUMBER(18,2),
    deductible_amount           NUMBER(18,2),
    ingredient_cost             NUMBER(18,2),
    dispensing_fee              NUMBER(18,2),
    member_liability            NUMBER(18,2),
    -- Diagnosis
    icd_diagnosis_code          VARCHAR(10),
    -- Audit
    source_system               VARCHAR(50),
    etl_load_timestamp          TIMESTAMP_NTZ    DEFAULT CURRENT_TIMESTAMP()
)
DATA_RETENTION_TIME_IN_DAYS = 30
CLUSTER BY (member_id, fill_date_key)
COPY GRANTS
COMMENT = 'Pharmacy claims fact table';


-- =============================================================================
-- FCT_ENCOUNTER: grouped encounter fact table
-- Episodes of care derived from overlapping/contiguous claims
-- =============================================================================
CREATE OR REPLACE TABLE CLAIMS_DW.WAREHOUSE.FCT_ENCOUNTER
(
    encounter_key               INTEGER          NOT NULL AUTOINCREMENT,
    encounter_id                VARCHAR(50)      NOT NULL
                                COMMENT 'Derived encounter identifier',
    member_key                  INTEGER          NOT NULL
                                COMMENT 'FK to DIM_MEMBER',
    member_id                   VARCHAR(20)      NOT NULL,
    rendering_provider_key      INTEGER,
    facility_id                 VARCHAR(20),
    -- Encounter dates (derived from constituent claims)
    encounter_start_date        DATE             NOT NULL,
    encounter_end_date          DATE             NOT NULL,
    encounter_start_date_key    INTEGER,
    encounter_end_date_key      INTEGER,
    -- Encounter attributes
    encounter_type              VARCHAR(20)
                                COMMENT 'INPATIENT, OUTPATIENT, ED, OFFICE_VISIT, TELEHEALTH',
    drg_code                    VARCHAR(10),
    primary_diagnosis_code      VARCHAR(10),
    diagnosis_codes             VARIANT,
    place_of_service            VARCHAR(5),
    -- Measures (aggregated across all claims in the encounter)
    total_claim_lines           INTEGER,
    total_billed_amount         NUMBER(18,2),
    total_allowed_amount        NUMBER(18,2),
    total_paid_amount           NUMBER(18,2),
    total_net_paid_amount       NUMBER(18,2),
    total_member_liability      NUMBER(18,2),
    length_of_stay              INTEGER
                                COMMENT 'Days between encounter start and end',
    -- Audit
    source_system               VARCHAR(50),
    etl_load_timestamp          TIMESTAMP_NTZ    DEFAULT CURRENT_TIMESTAMP()
)
DATA_RETENTION_TIME_IN_DAYS = 30
CLUSTER BY (member_id, encounter_start_date)
COPY GRANTS
COMMENT = 'Grouped encounter fact - episodes of care from overlapping/contiguous claims';
