/*******************************************************************************
 * Healthcare Claims Data Warehouse - Raw Tables (Snowflake)
 *
 * Snowflake-specific features used:
 *   - VARIANT type for semi-structured diagnosis codes (JSON array)
 *   - CLUSTER BY for micro-partition pruning
 *   - DATA_RETENTION_TIME_IN_DAYS per table
 *   - COPY GRANTS for permission inheritance
 *   - CHANGE_TRACKING for downstream Streams
 *
 * IMPORTANT - Structural drift from other platforms:
 *   - Diagnosis codes: VARIANT JSON array (diagnosis_codes) + 10 individual
 *     columns (icd_diagnosis_code_1..10). Teradata has 25 individual columns,
 *     Databricks uses a pure ARRAY<STRING>. This hybrid approach is unique
 *     to Snowflake.
 *   - Column naming differs:
 *       Teradata service_date_from  -> Snowflake start_date
 *       Teradata claim_status       -> Snowflake status_code
 *       Extra column: net_paid_amount (not in other platforms)
 ******************************************************************************/

USE DATABASE CLAIMS_DW;
USE SCHEMA RAW;

-- =============================================================================
-- RAW_MEMBER_ELIGIBILITY: member enrollment and eligibility records
-- =============================================================================
CREATE OR REPLACE TABLE CLAIMS_DW.RAW.RAW_MEMBER_ELIGIBILITY
(
    member_id                   VARCHAR(20)      NOT NULL,
    subscriber_id               VARCHAR(20),
    person_number               VARCHAR(5),
    first_name                  VARCHAR(100),
    last_name                   VARCHAR(100),
    date_of_birth               DATE,
    gender                      VARCHAR(1),
    ssn_encrypted               VARCHAR(256),
    address_line_1              VARCHAR(200),
    address_line_2              VARCHAR(200),
    city                        VARCHAR(100),
    state_code                  VARCHAR(2),
    zip_code                    VARCHAR(10),
    phone_number                VARCHAR(20),
    email                       VARCHAR(255),
    plan_code                   VARCHAR(20),
    plan_name                   VARCHAR(200),
    product_type                VARCHAR(20)      COMMENT 'HMO, PPO, EPO, POS, HDHP',
    line_of_business            VARCHAR(20)      COMMENT 'COMMERCIAL, MEDICARE, MEDICAID',
    group_number                VARCHAR(30),
    group_name                  VARCHAR(200),
    eligibility_start_date      DATE             NOT NULL,
    eligibility_end_date        DATE,
    pcp_provider_id             VARCHAR(20),
    pcp_provider_name           VARCHAR(200),
    coverage_type               VARCHAR(20)      COMMENT 'MEDICAL, DENTAL, VISION, PHARMACY',
    relationship_code           VARCHAR(5)       COMMENT '01=Self, 02=Spouse, 03=Child',
    cobra_flag                  BOOLEAN          DEFAULT FALSE,
    source_system               VARCHAR(50),
    load_timestamp              TIMESTAMP_NTZ    DEFAULT CURRENT_TIMESTAMP(),
    record_hash                 VARCHAR(64)      COMMENT 'SHA-256 hash for CDC detection'
)
DATA_RETENTION_TIME_IN_DAYS = 14
CHANGE_TRACKING = TRUE
CLUSTER BY (member_id, eligibility_start_date)
COPY GRANTS
COMMENT = 'Raw member eligibility/enrollment records from source systems';


-- =============================================================================
-- RAW_MEDICAL_CLAIM: medical/professional/facility claims
--
-- IMPORTANT: Hybrid diagnosis code storage (unique to Snowflake):
--   1. diagnosis_codes (VARIANT) - JSON array of all diagnosis codes
--   2. icd_diagnosis_code_1..10 - Individual VARCHAR columns
--   Teradata uses 25 individual columns; Databricks uses a pure array.
--   Snowflake keeps BOTH for maximum query flexibility.
-- =============================================================================
CREATE OR REPLACE TABLE CLAIMS_DW.RAW.RAW_MEDICAL_CLAIM
(
    claim_id                    VARCHAR(30)      NOT NULL,
    claim_line_number           INTEGER          NOT NULL,
    member_id                   VARCHAR(20)      NOT NULL,
    subscriber_id               VARCHAR(20),
    -- Date columns: NOTE column naming drift
    -- Teradata: service_date_from / service_date_to
    -- Databricks: service_from_date / service_to_date
    -- Snowflake: start_date / end_date
    start_date                  DATE             NOT NULL,
    end_date                    DATE,
    admission_date              DATE,
    discharge_date              DATE,
    -- Claim status: NOTE column naming drift
    -- Teradata: claim_status    Snowflake: status_code
    status_code                 VARCHAR(20)      NOT NULL
                                COMMENT 'PAID, ADJUSTED, REVERSED, DENIED',
    claim_type                  VARCHAR(10)      COMMENT 'P=Professional, I=Institutional',
    place_of_service            VARCHAR(5),
    bill_type                   VARCHAR(5),
    -- Provider information
    rendering_provider_id       VARCHAR(20),
    rendering_provider_npi      VARCHAR(10),
    billing_provider_id         VARCHAR(20),
    billing_provider_npi        VARCHAR(10),
    facility_id                 VARCHAR(20),
    -- Procedure codes
    cpt_code                    VARCHAR(10),
    cpt_modifier_1              VARCHAR(5),
    cpt_modifier_2              VARCHAR(5),
    revenue_code                VARCHAR(10),
    drg_code                    VARCHAR(10),

    -- =========================================================================
    -- DIAGNOSIS CODES - HYBRID STORAGE (Snowflake-specific)
    -- =========================================================================
    -- VARIANT column: JSON array of all diagnosis codes
    -- Example: ["E11.9", "I10", "Z79.4", "E78.5"]
    diagnosis_codes             VARIANT          COMMENT 'JSON array of ICD-10 diagnosis codes',

    -- Individual columns (only 10, NOT 25 like Teradata - structural drift)
    icd_diagnosis_code_1        VARCHAR(10)      COMMENT 'Primary diagnosis (ICD-10)',
    icd_diagnosis_code_2        VARCHAR(10),
    icd_diagnosis_code_3        VARCHAR(10),
    icd_diagnosis_code_4        VARCHAR(10),
    icd_diagnosis_code_5        VARCHAR(10),
    icd_diagnosis_code_6        VARCHAR(10),
    icd_diagnosis_code_7        VARCHAR(10),
    icd_diagnosis_code_8        VARCHAR(10),
    icd_diagnosis_code_9        VARCHAR(10),
    icd_diagnosis_code_10       VARCHAR(10),

    -- Financial amounts
    billed_amount               NUMBER(18,2),
    allowed_amount              NUMBER(18,2),
    paid_amount                 NUMBER(18,2),
    -- net_paid_amount: EXTRA column not in Teradata or Databricks (drift)
    net_paid_amount             NUMBER(18,2)     COMMENT 'Paid minus withhold and COB - unique to Snowflake',
    copay_amount                NUMBER(18,2),
    coinsurance_amount          NUMBER(18,2),
    deductible_amount           NUMBER(18,2),
    cob_amount                  NUMBER(18,2),
    withhold_amount             NUMBER(18,2),
    units                       NUMBER(10,2),

    -- Adjustment / ADR tracking
    original_claim_id           VARCHAR(30)      COMMENT 'For adjusted/reversed claims',
    adjustment_reason_code      VARCHAR(10),
    adjudication_date           DATE,

    -- Metadata
    source_system               VARCHAR(50),
    load_timestamp              TIMESTAMP_NTZ    DEFAULT CURRENT_TIMESTAMP(),
    record_hash                 VARCHAR(64)
)
DATA_RETENTION_TIME_IN_DAYS = 30
CHANGE_TRACKING = TRUE
CLUSTER BY (member_id, start_date)
COPY GRANTS
COMMENT = 'Raw medical claims with hybrid diagnosis code storage (VARIANT + individual columns)';


-- =============================================================================
-- RAW_PHARMACY_CLAIM: pharmacy / prescription drug claims
-- =============================================================================
CREATE OR REPLACE TABLE CLAIMS_DW.RAW.RAW_PHARMACY_CLAIM
(
    claim_id                    VARCHAR(30)      NOT NULL,
    member_id                   VARCHAR(20)      NOT NULL,
    subscriber_id               VARCHAR(20),
    -- Date columns: Snowflake naming convention
    fill_date                   DATE             NOT NULL,
    written_date                DATE,
    -- Status: same naming drift as medical claims
    status_code                 VARCHAR(20)      NOT NULL
                                COMMENT 'PAID, ADJUSTED, REVERSED, DENIED',
    ndc_code                    VARCHAR(11)      NOT NULL
                                COMMENT 'National Drug Code',
    drug_name                   VARCHAR(200),
    generic_name                VARCHAR(200),
    therapeutic_class_code      VARCHAR(20),
    therapeutic_class_desc      VARCHAR(200),
    gpi_code                    VARCHAR(20)      COMMENT 'Generic Product Identifier',
    dea_schedule                VARCHAR(5),
    -- Provider and pharmacy
    prescribing_provider_id     VARCHAR(20),
    prescribing_provider_npi    VARCHAR(10),
    pharmacy_id                 VARCHAR(20),
    pharmacy_npi                VARCHAR(10),
    pharmacy_name               VARCHAR(200),
    mail_order_flag             BOOLEAN          DEFAULT FALSE,
    -- Prescription details
    quantity_dispensed           NUMBER(10,3),
    days_supply                 INTEGER,
    refill_number               INTEGER,
    daw_code                    VARCHAR(5)       COMMENT 'Dispense As Written code',
    compound_flag               BOOLEAN          DEFAULT FALSE,
    formulary_flag              BOOLEAN,
    prior_auth_flag             BOOLEAN          DEFAULT FALSE,
    -- Financial amounts
    billed_amount               NUMBER(18,2),
    allowed_amount              NUMBER(18,2),
    paid_amount                 NUMBER(18,2),
    net_paid_amount             NUMBER(18,2)     COMMENT 'Paid minus withhold - unique to Snowflake',
    copay_amount                NUMBER(18,2),
    coinsurance_amount          NUMBER(18,2),
    deductible_amount           NUMBER(18,2),
    ingredient_cost             NUMBER(18,2),
    dispensing_fee              NUMBER(18,2),
    sales_tax                   NUMBER(18,2),
    -- Adjustment tracking
    original_claim_id           VARCHAR(30),
    -- Diagnosis linked to this prescription
    icd_diagnosis_code          VARCHAR(10),
    -- Metadata
    source_system               VARCHAR(50),
    load_timestamp              TIMESTAMP_NTZ    DEFAULT CURRENT_TIMESTAMP(),
    record_hash                 VARCHAR(64)
)
DATA_RETENTION_TIME_IN_DAYS = 30
CHANGE_TRACKING = TRUE
CLUSTER BY (member_id, fill_date)
COPY GRANTS
COMMENT = 'Raw pharmacy/prescription drug claims from PBM feeds';
