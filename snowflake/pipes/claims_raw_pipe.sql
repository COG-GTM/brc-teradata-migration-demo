/*******************************************************************************
 * Healthcare Claims - Snowpipe Definitions (Auto-Ingestion)
 *
 * Snowflake-specific features:
 *   - CREATE PIPE with AUTO_INGEST = TRUE
 *   - External stages (S3 / Azure Blob)
 *   - File format definitions (CSV, Parquet)
 *   - COPY INTO with transformation
 *   - Error handling with ON_ERROR
 ******************************************************************************/

USE DATABASE CLAIMS_DW;
USE SCHEMA RAW;

-- =============================================================================
-- File Formats
-- =============================================================================

-- CSV format for delimited flat files
CREATE OR REPLACE FILE FORMAT CLAIMS_DW.RAW.FF_CLAIMS_CSV
    TYPE                    = 'CSV'
    FIELD_DELIMITER         = ','
    RECORD_DELIMITER        = '\n'
    SKIP_HEADER             = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    NULL_IF                 = ('', 'NULL', 'null', '\\N')
    TRIM_SPACE              = TRUE
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
    DATE_FORMAT             = 'YYYY-MM-DD'
    TIMESTAMP_FORMAT        = 'YYYY-MM-DD HH24:MI:SS.FF'
    COMMENT                 = 'Standard CSV format for healthcare claims flat files';

-- Parquet format for columnar files
CREATE OR REPLACE FILE FORMAT CLAIMS_DW.RAW.FF_CLAIMS_PARQUET
    TYPE                    = 'PARQUET'
    SNAPPY_COMPRESSION      = TRUE
    COMMENT                 = 'Parquet format for columnar claims data files';

-- JSON format for semi-structured feeds
CREATE OR REPLACE FILE FORMAT CLAIMS_DW.RAW.FF_CLAIMS_JSON
    TYPE                    = 'JSON'
    STRIP_OUTER_ARRAY       = TRUE
    STRIP_NULL_VALUES       = FALSE
    COMMENT                 = 'JSON format for semi-structured claims feeds';


-- =============================================================================
-- External Stages
-- =============================================================================

-- S3 stage for claims data landing zone
CREATE OR REPLACE STAGE CLAIMS_DW.RAW.STG_CLAIMS_S3
    URL = 's3://claims-data-landing/raw/'
    -- STORAGE_INTEGRATION = claims_s3_integration
    -- Uncomment above and comment below for production (use storage integration)
    -- CREDENTIALS = (AWS_KEY_ID = '...' AWS_SECRET_KEY = '...')
    FILE_FORMAT = CLAIMS_DW.RAW.FF_CLAIMS_CSV
    COMMENT     = 'S3 landing zone for raw claims data files';

-- S3 stage for Parquet files
CREATE OR REPLACE STAGE CLAIMS_DW.RAW.STG_CLAIMS_PARQUET_S3
    URL = 's3://claims-data-landing/parquet/'
    FILE_FORMAT = CLAIMS_DW.RAW.FF_CLAIMS_PARQUET
    COMMENT     = 'S3 landing zone for Parquet claims data files';


-- =============================================================================
-- Snowpipe: Medical Claims (CSV)
-- Auto-ingests medical claim files from S3 into RAW_MEDICAL_CLAIM
-- =============================================================================
CREATE OR REPLACE PIPE CLAIMS_DW.RAW.PIPE_RAW_MEDICAL_CLAIM_CSV
    AUTO_INGEST = TRUE
    COMMENT     = 'Auto-ingest medical claims from CSV files in S3'
AS
COPY INTO CLAIMS_DW.RAW.RAW_MEDICAL_CLAIM (
    claim_id, claim_line_number, member_id, subscriber_id,
    start_date, end_date, admission_date, discharge_date,
    status_code, claim_type, place_of_service, bill_type,
    rendering_provider_id, rendering_provider_npi,
    billing_provider_id, billing_provider_npi, facility_id,
    cpt_code, cpt_modifier_1, cpt_modifier_2, revenue_code, drg_code,
    diagnosis_codes,
    icd_diagnosis_code_1, icd_diagnosis_code_2, icd_diagnosis_code_3,
    icd_diagnosis_code_4, icd_diagnosis_code_5, icd_diagnosis_code_6,
    icd_diagnosis_code_7, icd_diagnosis_code_8, icd_diagnosis_code_9,
    icd_diagnosis_code_10,
    billed_amount, allowed_amount, paid_amount, net_paid_amount,
    copay_amount, coinsurance_amount, deductible_amount, cob_amount,
    withhold_amount, units,
    original_claim_id, adjustment_reason_code, adjudication_date,
    source_system, load_timestamp, record_hash
)
FROM (
    SELECT
        $1,  $2,  $3,  $4,                              -- claim_id..subscriber_id
        TO_DATE($5, 'YYYY-MM-DD'),                       -- start_date
        TO_DATE($6, 'YYYY-MM-DD'),                       -- end_date
        TO_DATE($7, 'YYYY-MM-DD'),                       -- admission_date
        TO_DATE($8, 'YYYY-MM-DD'),                       -- discharge_date
        $9,  $10, $11, $12,                              -- status_code..bill_type
        $13, $14, $15, $16, $17,                         -- provider IDs
        $18, $19, $20, $21, $22,                         -- procedure codes
        TRY_PARSE_JSON($23),                             -- diagnosis_codes (JSON)
        $24, $25, $26, $27, $28, $29, $30, $31, $32, $33, -- icd codes 1-10
        $34, $35, $36, $37,                              -- financial amounts
        $38, $39, $40, $41, $42, $43,                    -- more financial
        $44, $45,                                        -- original_claim, adj_reason
        TO_DATE($46, 'YYYY-MM-DD'),                      -- adjudication_date
        $47,                                             -- source_system
        CURRENT_TIMESTAMP(),                             -- load_timestamp
        MD5($1 || '|' || $2 || '|' || $9)               -- record_hash
    FROM @CLAIMS_DW.RAW.STG_CLAIMS_S3/medical_claims/
)
PATTERN = '.*medical_claim.*[.]csv'
ON_ERROR = 'CONTINUE';


-- =============================================================================
-- Snowpipe: Medical Claims (Parquet)
-- =============================================================================
CREATE OR REPLACE PIPE CLAIMS_DW.RAW.PIPE_RAW_MEDICAL_CLAIM_PARQUET
    AUTO_INGEST = TRUE
    COMMENT     = 'Auto-ingest medical claims from Parquet files in S3'
AS
COPY INTO CLAIMS_DW.RAW.RAW_MEDICAL_CLAIM (
    claim_id, claim_line_number, member_id, subscriber_id,
    start_date, end_date, admission_date, discharge_date,
    status_code, claim_type, place_of_service, bill_type,
    rendering_provider_id, rendering_provider_npi,
    billing_provider_id, billing_provider_npi, facility_id,
    cpt_code, cpt_modifier_1, cpt_modifier_2, revenue_code, drg_code,
    diagnosis_codes,
    icd_diagnosis_code_1, icd_diagnosis_code_2, icd_diagnosis_code_3,
    icd_diagnosis_code_4, icd_diagnosis_code_5, icd_diagnosis_code_6,
    icd_diagnosis_code_7, icd_diagnosis_code_8, icd_diagnosis_code_9,
    icd_diagnosis_code_10,
    billed_amount, allowed_amount, paid_amount, net_paid_amount,
    copay_amount, coinsurance_amount, deductible_amount, cob_amount,
    withhold_amount, units,
    original_claim_id, adjustment_reason_code, adjudication_date,
    source_system, load_timestamp, record_hash
)
FROM (
    SELECT
        $1:claim_id::VARCHAR,
        $1:claim_line_number::INTEGER,
        $1:member_id::VARCHAR,
        $1:subscriber_id::VARCHAR,
        $1:start_date::DATE,
        $1:end_date::DATE,
        $1:admission_date::DATE,
        $1:discharge_date::DATE,
        $1:status_code::VARCHAR,
        $1:claim_type::VARCHAR,
        $1:place_of_service::VARCHAR,
        $1:bill_type::VARCHAR,
        $1:rendering_provider_id::VARCHAR,
        $1:rendering_provider_npi::VARCHAR,
        $1:billing_provider_id::VARCHAR,
        $1:billing_provider_npi::VARCHAR,
        $1:facility_id::VARCHAR,
        $1:cpt_code::VARCHAR,
        $1:cpt_modifier_1::VARCHAR,
        $1:cpt_modifier_2::VARCHAR,
        $1:revenue_code::VARCHAR,
        $1:drg_code::VARCHAR,
        $1:diagnosis_codes::VARIANT,
        $1:icd_diagnosis_code_1::VARCHAR,
        $1:icd_diagnosis_code_2::VARCHAR,
        $1:icd_diagnosis_code_3::VARCHAR,
        $1:icd_diagnosis_code_4::VARCHAR,
        $1:icd_diagnosis_code_5::VARCHAR,
        $1:icd_diagnosis_code_6::VARCHAR,
        $1:icd_diagnosis_code_7::VARCHAR,
        $1:icd_diagnosis_code_8::VARCHAR,
        $1:icd_diagnosis_code_9::VARCHAR,
        $1:icd_diagnosis_code_10::VARCHAR,
        $1:billed_amount::NUMBER(18,2),
        $1:allowed_amount::NUMBER(18,2),
        $1:paid_amount::NUMBER(18,2),
        $1:net_paid_amount::NUMBER(18,2),
        $1:copay_amount::NUMBER(18,2),
        $1:coinsurance_amount::NUMBER(18,2),
        $1:deductible_amount::NUMBER(18,2),
        $1:cob_amount::NUMBER(18,2),
        $1:withhold_amount::NUMBER(18,2),
        $1:units::NUMBER(10,2),
        $1:original_claim_id::VARCHAR,
        $1:adjustment_reason_code::VARCHAR,
        $1:adjudication_date::DATE,
        $1:source_system::VARCHAR,
        CURRENT_TIMESTAMP(),
        MD5($1:claim_id::VARCHAR || '|' || $1:claim_line_number::VARCHAR || '|' || $1:status_code::VARCHAR)
    FROM @CLAIMS_DW.RAW.STG_CLAIMS_PARQUET_S3/medical_claims/
)
PATTERN = '.*medical_claim.*[.]parquet'
ON_ERROR = 'CONTINUE';


-- =============================================================================
-- Snowpipe: Pharmacy Claims (CSV)
-- =============================================================================
CREATE OR REPLACE PIPE CLAIMS_DW.RAW.PIPE_RAW_PHARMACY_CLAIM_CSV
    AUTO_INGEST = TRUE
    COMMENT     = 'Auto-ingest pharmacy claims from CSV files in S3'
AS
COPY INTO CLAIMS_DW.RAW.RAW_PHARMACY_CLAIM (
    claim_id, member_id, subscriber_id,
    fill_date, written_date, status_code,
    ndc_code, drug_name, generic_name,
    therapeutic_class_code, therapeutic_class_desc, gpi_code, dea_schedule,
    prescribing_provider_id, prescribing_provider_npi,
    pharmacy_id, pharmacy_npi, pharmacy_name, mail_order_flag,
    quantity_dispensed, days_supply, refill_number,
    daw_code, compound_flag, formulary_flag, prior_auth_flag,
    billed_amount, allowed_amount, paid_amount, net_paid_amount,
    copay_amount, coinsurance_amount, deductible_amount,
    ingredient_cost, dispensing_fee, sales_tax,
    original_claim_id, icd_diagnosis_code,
    source_system, load_timestamp, record_hash
)
FROM (
    SELECT
        $1, $2, $3,                                      -- claim_id, member_id, subscriber_id
        TO_DATE($4, 'YYYY-MM-DD'),                       -- fill_date
        TO_DATE($5, 'YYYY-MM-DD'),                       -- written_date
        $6,                                              -- status_code
        $7, $8, $9,                                      -- ndc_code, drug_name, generic_name
        $10, $11, $12, $13,                              -- therapeutic class, gpi, dea
        $14, $15, $16, $17, $18,                         -- provider/pharmacy
        $19::BOOLEAN,                                    -- mail_order_flag
        $20, $21, $22,                                   -- quantity, days_supply, refill
        $23, $24::BOOLEAN, $25::BOOLEAN, $26::BOOLEAN,   -- daw, compound, formulary, pa
        $27, $28, $29, $30,                              -- financial amounts
        $31, $32, $33, $34, $35, $36,                    -- more financial
        $37, $38,                                        -- original_claim, diagnosis
        $39,                                             -- source_system
        CURRENT_TIMESTAMP(),                             -- load_timestamp
        MD5($1 || '|' || $6)                             -- record_hash
    FROM @CLAIMS_DW.RAW.STG_CLAIMS_S3/pharmacy_claims/
)
PATTERN = '.*pharmacy_claim.*[.]csv'
ON_ERROR = 'CONTINUE';
