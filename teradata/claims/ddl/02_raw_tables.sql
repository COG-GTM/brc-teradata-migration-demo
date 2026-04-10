/*******************************************************************************
 * Healthcare Claims Teradata Migration Demo - Raw Tables
 *
 * Teradata-specific features used:
 *   - SET / MULTISET tables
 *   - PRIMARY INDEX (hash distribution)
 *   - PARTITION BY RANGE_N (PPI)
 *   - FALLBACK / NO FALLBACK
 *   - JOURNAL options
 *   - COMPRESS values
 *   - CHARACTER SET LATIN / NOT CASESPECIFIC
 *   - FORMAT patterns
 *   - COLLECT STATISTICS
 *
 * Tables:
 *   RAW_MEMBER_ELIGIBILITY  - member enrollment and eligibility records
 *   RAW_MEDICAL_CLAIM       - medical/institutional/professional claims
 *   RAW_PHARMACY_CLAIM      - pharmacy (Rx) claims from PBM feed
 ******************************************************************************/

DATABASE CLAIMS_RAW;

-- =============================================================================
-- RAW_MEMBER_ELIGIBILITY: member enrollment and eligibility records
-- SET table enforces uniqueness at the AMP level
-- Partitioned by enrollment_start_date (yearly)
-- =============================================================================
CREATE SET TABLE CLAIMS_RAW.RAW_MEMBER_ELIGIBILITY, FALLBACK,
    NO BEFORE JOURNAL,
    NO AFTER JOURNAL,
    CHECKSUM = DEFAULT,
    DEFAULT MERGEBLOCKRATIO
(
    member_id               VARCHAR(20)      CHARACTER SET LATIN NOT CASESPECIFIC NOT NULL,
    plan_id                 VARCHAR(20)      CHARACTER SET LATIN NOT CASESPECIFIC NOT NULL,
    enrollment_start_date   DATE FORMAT 'YYYY-MM-DD' NOT NULL,
    enrollment_end_date     DATE FORMAT 'YYYY-MM-DD',
    payer_id                VARCHAR(20)      CHARACTER SET LATIN NOT CASESPECIFIC NOT NULL,
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
    load_date               DATE FORMAT 'YYYY-MM-DD' DEFAULT CURRENT_DATE,
    last_updated_ts         TIMESTAMP(6)     DEFAULT CURRENT_TIMESTAMP(6)
)
PRIMARY INDEX (member_id)
PARTITION BY RANGE_N(
    enrollment_start_date BETWEEN DATE '2010-01-01' AND DATE '2030-12-31'
    EACH INTERVAL '1' YEAR
);

COMMENT ON TABLE CLAIMS_RAW.RAW_MEMBER_ELIGIBILITY
    AS 'Member enrollment and eligibility records - SET table with yearly PPI on enrollment_start_date';

COLLECT STATISTICS
    COLUMN (member_id),
    COLUMN (plan_id),
    COLUMN (payer_id),
    COLUMN (enrollment_start_date),
    COLUMN (line_of_business),
    COLUMN (state),
    COLUMN (gender),
    COLUMN (PARTITION)
ON CLAIMS_RAW.RAW_MEMBER_ELIGIBILITY;


-- =============================================================================
-- RAW_MEDICAL_CLAIM: medical claims (institutional, professional, outpatient)
-- MULTISET table with PPI on service_date_from (monthly partitions)
-- Includes 25 individual ICD diagnosis code columns and 6 procedure code columns
-- =============================================================================
CREATE MULTISET TABLE CLAIMS_RAW.RAW_MEDICAL_CLAIM, FALLBACK,
    NO BEFORE JOURNAL,
    NO AFTER JOURNAL,
    CHECKSUM = DEFAULT,
    DEFAULT MERGEBLOCKRATIO
(
    claim_id                VARCHAR(30)      CHARACTER SET LATIN NOT CASESPECIFIC NOT NULL,
    member_id               VARCHAR(20)      CHARACTER SET LATIN NOT CASESPECIFIC NOT NULL,
    claim_line_number       SMALLINT         NOT NULL,
    claim_type              VARCHAR(5)       CHARACTER SET LATIN NOT CASESPECIFIC
                            COMPRESS ('I', 'P', 'O'),
    service_date_from       DATE FORMAT 'YYYY-MM-DD' NOT NULL,
    service_date_to         DATE FORMAT 'YYYY-MM-DD',
    admission_date          DATE FORMAT 'YYYY-MM-DD',
    discharge_date          DATE FORMAT 'YYYY-MM-DD',
    admit_type              VARCHAR(5)       CHARACTER SET LATIN NOT CASESPECIFIC
                            COMPRESS ('1', '2', '3', '4', '5', '9'),
    admit_source            VARCHAR(5)       CHARACTER SET LATIN NOT CASESPECIFIC,
    discharge_disposition   VARCHAR(5)       CHARACTER SET LATIN NOT CASESPECIFIC,
    place_of_service        VARCHAR(5)       CHARACTER SET LATIN NOT CASESPECIFIC
                            COMPRESS ('11', '21', '22', '23', '24', '31', '32', '51', '81'),
    bill_type               VARCHAR(4)       CHARACTER SET LATIN NOT CASESPECIFIC,
    revenue_center_code     VARCHAR(10)      CHARACTER SET LATIN NOT CASESPECIFIC,
    ms_drg                  VARCHAR(5)       CHARACTER SET LATIN NOT CASESPECIFIC,
    apr_drg                 VARCHAR(5)       CHARACTER SET LATIN NOT CASESPECIFIC,
    hcpcs_code              VARCHAR(10)      CHARACTER SET LATIN NOT CASESPECIFIC,
    cpt_code                VARCHAR(10)      CHARACTER SET LATIN NOT CASESPECIFIC,

    /* ICD Diagnosis Codes 1-25 (individual columns per CMS standard) */
    icd_diagnosis_code_1    VARCHAR(10)      CHARACTER SET LATIN NOT CASESPECIFIC,
    icd_diagnosis_code_2    VARCHAR(10)      CHARACTER SET LATIN NOT CASESPECIFIC,
    icd_diagnosis_code_3    VARCHAR(10)      CHARACTER SET LATIN NOT CASESPECIFIC,
    icd_diagnosis_code_4    VARCHAR(10)      CHARACTER SET LATIN NOT CASESPECIFIC,
    icd_diagnosis_code_5    VARCHAR(10)      CHARACTER SET LATIN NOT CASESPECIFIC,
    icd_diagnosis_code_6    VARCHAR(10)      CHARACTER SET LATIN NOT CASESPECIFIC,
    icd_diagnosis_code_7    VARCHAR(10)      CHARACTER SET LATIN NOT CASESPECIFIC,
    icd_diagnosis_code_8    VARCHAR(10)      CHARACTER SET LATIN NOT CASESPECIFIC,
    icd_diagnosis_code_9    VARCHAR(10)      CHARACTER SET LATIN NOT CASESPECIFIC,
    icd_diagnosis_code_10   VARCHAR(10)      CHARACTER SET LATIN NOT CASESPECIFIC,
    icd_diagnosis_code_11   VARCHAR(10)      CHARACTER SET LATIN NOT CASESPECIFIC,
    icd_diagnosis_code_12   VARCHAR(10)      CHARACTER SET LATIN NOT CASESPECIFIC,
    icd_diagnosis_code_13   VARCHAR(10)      CHARACTER SET LATIN NOT CASESPECIFIC,
    icd_diagnosis_code_14   VARCHAR(10)      CHARACTER SET LATIN NOT CASESPECIFIC,
    icd_diagnosis_code_15   VARCHAR(10)      CHARACTER SET LATIN NOT CASESPECIFIC,
    icd_diagnosis_code_16   VARCHAR(10)      CHARACTER SET LATIN NOT CASESPECIFIC,
    icd_diagnosis_code_17   VARCHAR(10)      CHARACTER SET LATIN NOT CASESPECIFIC,
    icd_diagnosis_code_18   VARCHAR(10)      CHARACTER SET LATIN NOT CASESPECIFIC,
    icd_diagnosis_code_19   VARCHAR(10)      CHARACTER SET LATIN NOT CASESPECIFIC,
    icd_diagnosis_code_20   VARCHAR(10)      CHARACTER SET LATIN NOT CASESPECIFIC,
    icd_diagnosis_code_21   VARCHAR(10)      CHARACTER SET LATIN NOT CASESPECIFIC,
    icd_diagnosis_code_22   VARCHAR(10)      CHARACTER SET LATIN NOT CASESPECIFIC,
    icd_diagnosis_code_23   VARCHAR(10)      CHARACTER SET LATIN NOT CASESPECIFIC,
    icd_diagnosis_code_24   VARCHAR(10)      CHARACTER SET LATIN NOT CASESPECIFIC,
    icd_diagnosis_code_25   VARCHAR(10)      CHARACTER SET LATIN NOT CASESPECIFIC,

    /* ICD Procedure Codes 1-6 */
    icd_procedure_code_1    VARCHAR(10)      CHARACTER SET LATIN NOT CASESPECIFIC,
    icd_procedure_code_2    VARCHAR(10)      CHARACTER SET LATIN NOT CASESPECIFIC,
    icd_procedure_code_3    VARCHAR(10)      CHARACTER SET LATIN NOT CASESPECIFIC,
    icd_procedure_code_4    VARCHAR(10)      CHARACTER SET LATIN NOT CASESPECIFIC,
    icd_procedure_code_5    VARCHAR(10)      CHARACTER SET LATIN NOT CASESPECIFIC,
    icd_procedure_code_6    VARCHAR(10)      CHARACTER SET LATIN NOT CASESPECIFIC,

    /* Provider identifiers */
    npi                     VARCHAR(10)      CHARACTER SET LATIN NOT CASESPECIFIC,
    billing_npi             VARCHAR(10)      CHARACTER SET LATIN NOT CASESPECIFIC,
    rendering_npi           VARCHAR(10)      CHARACTER SET LATIN NOT CASESPECIFIC,
    facility_npi            VARCHAR(10)      CHARACTER SET LATIN NOT CASESPECIFIC,

    /* Financial amounts */
    paid_amount             DECIMAL(18,2),
    charge_amount           DECIMAL(18,2),
    allowed_amount          DECIMAL(18,2),
    coinsurance             DECIMAL(18,2),
    copay                   DECIMAL(18,2),
    deductible              DECIMAL(18,2),

    /* Claim status and adjustment tracking */
    claim_status            VARCHAR(10)      CHARACTER SET LATIN NOT CASESPECIFIC
                            COMPRESS ('PAID', 'ADJUSTED', 'DENIED', 'REVERSED'),
    adjustment_type         VARCHAR(10)      CHARACTER SET LATIN NOT CASESPECIFIC,
    original_claim_id       VARCHAR(30)      CHARACTER SET LATIN NOT CASESPECIFIC,
    payer_id                VARCHAR(20)      CHARACTER SET LATIN NOT CASESPECIFIC,

    load_date               DATE FORMAT 'YYYY-MM-DD' DEFAULT CURRENT_DATE,
    last_updated_ts         TIMESTAMP(6)     DEFAULT CURRENT_TIMESTAMP(6)
)
PRIMARY INDEX (claim_id, claim_line_number)
PARTITION BY RANGE_N(
    service_date_from BETWEEN DATE '2015-01-01' AND DATE '2030-12-31'
    EACH INTERVAL '1' MONTH
);

COMMENT ON TABLE CLAIMS_RAW.RAW_MEDICAL_CLAIM
    AS 'Medical claims (I=Institutional, P=Professional, O=Outpatient) - MULTISET with monthly PPI on service_date_from';

COLLECT STATISTICS
    COLUMN (claim_id),
    COLUMN (claim_id, claim_line_number),
    COLUMN (member_id),
    COLUMN (service_date_from),
    COLUMN (claim_type),
    COLUMN (claim_status),
    COLUMN (payer_id),
    COLUMN (billing_npi),
    COLUMN (icd_diagnosis_code_1),
    COLUMN (ms_drg),
    COLUMN (PARTITION)
ON CLAIMS_RAW.RAW_MEDICAL_CLAIM;


-- =============================================================================
-- RAW_PHARMACY_CLAIM: pharmacy (Rx) claims from PBM feed
-- MULTISET table with PPI on dispensing_date (monthly partitions)
-- =============================================================================
CREATE MULTISET TABLE CLAIMS_RAW.RAW_PHARMACY_CLAIM, FALLBACK,
    NO BEFORE JOURNAL,
    NO AFTER JOURNAL,
    CHECKSUM = DEFAULT,
    DEFAULT MERGEBLOCKRATIO
(
    claim_id                VARCHAR(30)      CHARACTER SET LATIN NOT CASESPECIFIC NOT NULL,
    member_id               VARCHAR(20)      CHARACTER SET LATIN NOT CASESPECIFIC NOT NULL,
    dispensing_date          DATE FORMAT 'YYYY-MM-DD' NOT NULL,
    ndc_code                VARCHAR(11)      CHARACTER SET LATIN NOT CASESPECIFIC NOT NULL,
    quantity                DECIMAL(10,2),
    days_supply             SMALLINT,
    refill_number           SMALLINT         COMPRESS (0, 1, 2, 3, 4, 5),
    dispensing_npi          VARCHAR(10)      CHARACTER SET LATIN NOT CASESPECIFIC,
    prescribing_npi         VARCHAR(10)      CHARACTER SET LATIN NOT CASESPECIFIC,
    paid_amount             DECIMAL(18,2),
    charge_amount           DECIMAL(18,2),
    allowed_amount          DECIMAL(18,2),
    copay                   DECIMAL(18,2),
    coinsurance             DECIMAL(18,2),
    deductible              DECIMAL(18,2),
    plan_paid               DECIMAL(18,2),
    claim_status            VARCHAR(10)      CHARACTER SET LATIN NOT CASESPECIFIC
                            COMPRESS ('PAID', 'ADJUSTED', 'DENIED', 'REVERSED'),
    payer_id                VARCHAR(20)      CHARACTER SET LATIN NOT CASESPECIFIC,
    load_date               DATE FORMAT 'YYYY-MM-DD' DEFAULT CURRENT_DATE,
    last_updated_ts         TIMESTAMP(6)     DEFAULT CURRENT_TIMESTAMP(6)
)
PRIMARY INDEX (claim_id)
PARTITION BY RANGE_N(
    dispensing_date BETWEEN DATE '2015-01-01' AND DATE '2030-12-31'
    EACH INTERVAL '1' MONTH
);

COMMENT ON TABLE CLAIMS_RAW.RAW_PHARMACY_CLAIM
    AS 'Pharmacy claims from PBM feed - MULTISET with monthly PPI on dispensing_date';

COLLECT STATISTICS
    COLUMN (claim_id),
    COLUMN (member_id),
    COLUMN (dispensing_date),
    COLUMN (ndc_code),
    COLUMN (claim_status),
    COLUMN (payer_id),
    COLUMN (dispensing_npi),
    COLUMN (prescribing_npi),
    COLUMN (PARTITION)
ON CLAIMS_RAW.RAW_PHARMACY_CLAIM;
