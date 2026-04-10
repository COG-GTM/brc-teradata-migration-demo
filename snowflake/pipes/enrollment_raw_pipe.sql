/*******************************************************************************
 * Healthcare Claims - Enrollment Data Snowpipe
 *
 * Auto-ingests member eligibility/enrollment data from external stages
 * into RAW_MEMBER_ELIGIBILITY.
 *
 * Supports both CSV and Parquet file formats.
 ******************************************************************************/

USE DATABASE CLAIMS_DW;
USE SCHEMA RAW;

-- =============================================================================
-- Snowpipe: Member Eligibility (CSV)
-- Auto-ingests enrollment files from S3
-- =============================================================================
CREATE OR REPLACE PIPE CLAIMS_DW.RAW.PIPE_RAW_MEMBER_ELIGIBILITY_CSV
    AUTO_INGEST = TRUE
    COMMENT     = 'Auto-ingest member eligibility/enrollment from CSV files in S3'
AS
COPY INTO CLAIMS_DW.RAW.RAW_MEMBER_ELIGIBILITY (
    member_id, subscriber_id, person_number,
    first_name, last_name, date_of_birth, gender,
    ssn_encrypted,
    address_line_1, address_line_2, city, state_code, zip_code,
    phone_number, email,
    plan_code, plan_name, product_type, line_of_business,
    group_number, group_name,
    eligibility_start_date, eligibility_end_date,
    pcp_provider_id, pcp_provider_name,
    coverage_type, relationship_code, cobra_flag,
    source_system, load_timestamp, record_hash
)
FROM (
    SELECT
        $1,  $2,  $3,                                    -- member_id, subscriber_id, person_number
        $4,  $5,                                         -- first_name, last_name
        TO_DATE($6, 'YYYY-MM-DD'),                       -- date_of_birth
        $7,                                              -- gender
        $8,                                              -- ssn_encrypted
        $9,  $10, $11, $12, $13,                         -- address fields
        $14, $15,                                        -- phone, email
        $16, $17, $18, $19,                              -- plan_code, plan_name, product_type, lob
        $20, $21,                                        -- group_number, group_name
        TO_DATE($22, 'YYYY-MM-DD'),                      -- eligibility_start_date
        TO_DATE($23, 'YYYY-MM-DD'),                      -- eligibility_end_date
        $24, $25,                                        -- pcp_provider_id, pcp_provider_name
        $26, $27,                                        -- coverage_type, relationship_code
        $28::BOOLEAN,                                    -- cobra_flag
        $29,                                             -- source_system
        CURRENT_TIMESTAMP(),                             -- load_timestamp
        MD5($1 || '|' || COALESCE($22, '') || '|' || COALESCE($16, ''))  -- record_hash
    FROM @CLAIMS_DW.RAW.STG_CLAIMS_S3/enrollment/
)
PATTERN = '.*eligibility.*[.]csv'
ON_ERROR = 'CONTINUE';


-- =============================================================================
-- Snowpipe: Member Eligibility (Parquet)
-- =============================================================================
CREATE OR REPLACE PIPE CLAIMS_DW.RAW.PIPE_RAW_MEMBER_ELIGIBILITY_PARQUET
    AUTO_INGEST = TRUE
    COMMENT     = 'Auto-ingest member eligibility/enrollment from Parquet files in S3'
AS
COPY INTO CLAIMS_DW.RAW.RAW_MEMBER_ELIGIBILITY (
    member_id, subscriber_id, person_number,
    first_name, last_name, date_of_birth, gender,
    ssn_encrypted,
    address_line_1, address_line_2, city, state_code, zip_code,
    phone_number, email,
    plan_code, plan_name, product_type, line_of_business,
    group_number, group_name,
    eligibility_start_date, eligibility_end_date,
    pcp_provider_id, pcp_provider_name,
    coverage_type, relationship_code, cobra_flag,
    source_system, load_timestamp, record_hash
)
FROM (
    SELECT
        $1:member_id::VARCHAR,
        $1:subscriber_id::VARCHAR,
        $1:person_number::VARCHAR,
        $1:first_name::VARCHAR,
        $1:last_name::VARCHAR,
        $1:date_of_birth::DATE,
        $1:gender::VARCHAR,
        $1:ssn_encrypted::VARCHAR,
        $1:address_line_1::VARCHAR,
        $1:address_line_2::VARCHAR,
        $1:city::VARCHAR,
        $1:state_code::VARCHAR,
        $1:zip_code::VARCHAR,
        $1:phone_number::VARCHAR,
        $1:email::VARCHAR,
        $1:plan_code::VARCHAR,
        $1:plan_name::VARCHAR,
        $1:product_type::VARCHAR,
        $1:line_of_business::VARCHAR,
        $1:group_number::VARCHAR,
        $1:group_name::VARCHAR,
        $1:eligibility_start_date::DATE,
        $1:eligibility_end_date::DATE,
        $1:pcp_provider_id::VARCHAR,
        $1:pcp_provider_name::VARCHAR,
        $1:coverage_type::VARCHAR,
        $1:relationship_code::VARCHAR,
        $1:cobra_flag::BOOLEAN,
        $1:source_system::VARCHAR,
        CURRENT_TIMESTAMP(),
        MD5(
            $1:member_id::VARCHAR || '|' ||
            COALESCE($1:eligibility_start_date::VARCHAR, '') || '|' ||
            COALESCE($1:plan_code::VARCHAR, '')
        )
    FROM @CLAIMS_DW.RAW.STG_CLAIMS_PARQUET_S3/enrollment/
)
PATTERN = '.*eligibility.*[.]parquet'
ON_ERROR = 'CONTINUE';


-- =============================================================================
-- Pipe monitoring queries
-- =============================================================================
-- Check pipe status:
-- SELECT SYSTEM$PIPE_STATUS('CLAIMS_DW.RAW.PIPE_RAW_MEMBER_ELIGIBILITY_CSV');
-- SELECT SYSTEM$PIPE_STATUS('CLAIMS_DW.RAW.PIPE_RAW_MEMBER_ELIGIBILITY_PARQUET');
--
-- Check recent copy history:
-- SELECT *
-- FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
--     TABLE_NAME => 'CLAIMS_DW.RAW.RAW_MEMBER_ELIGIBILITY',
--     START_TIME => DATEADD('hour', -24, CURRENT_TIMESTAMP())
-- ))
-- ORDER BY LAST_LOAD_TIME DESC;
