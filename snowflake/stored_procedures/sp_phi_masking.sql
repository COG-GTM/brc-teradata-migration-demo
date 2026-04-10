/*******************************************************************************
 * Healthcare Claims - PHI Masking via Snowflake Dynamic Data Masking
 *
 * IMPORTANT - Platform difference for PHI masking:
 *   Teradata:    Masking applied AFTER mart layer (post-aggregation)
 *   Databricks:  Masking applied at STAGING layer (column-level functions)
 *   Snowflake:   Dynamic Data Masking POLICIES at COLUMN level on RAW tables
 *                Masking is ALWAYS ACTIVE - enforced by Snowflake at query time
 *
 * Uses Snowflake Dynamic Data Masking (CREATE MASKING POLICY):
 *   - Policies are attached to columns, not views
 *   - Masking is role-based: privileged roles see real data
 *   - No performance penalty - masking applied at query execution
 ******************************************************************************/

USE DATABASE CLAIMS_DW;
USE SCHEMA RAW;

-- =============================================================================
-- Masking Policies
-- =============================================================================

-- SSN masking: show only last 4 digits to non-privileged roles
CREATE OR REPLACE MASKING POLICY CLAIMS_DW.RAW.MP_SSN_MASK AS
    (val VARCHAR) RETURNS VARCHAR ->
    CASE
        WHEN CURRENT_ROLE() IN ('CLAIMS_ETL_ROLE', 'SYSADMIN', 'ACCOUNTADMIN')
            THEN val
        WHEN val IS NOT NULL
            THEN 'XXX-XX-' || RIGHT(val, 4)
        ELSE NULL
    END
COMMENT = 'Masks SSN - shows last 4 digits to non-privileged roles';


-- Full name masking: show initials only to non-privileged roles
CREATE OR REPLACE MASKING POLICY CLAIMS_DW.RAW.MP_NAME_MASK AS
    (val VARCHAR) RETURNS VARCHAR ->
    CASE
        WHEN CURRENT_ROLE() IN ('CLAIMS_ETL_ROLE', 'SYSADMIN', 'ACCOUNTADMIN')
            THEN val
        WHEN val IS NOT NULL AND LENGTH(val) > 0
            THEN LEFT(val, 1) || '***'
        ELSE NULL
    END
COMMENT = 'Masks names - shows first initial only to non-privileged roles';


-- Date of birth masking: show year only to non-privileged roles
CREATE OR REPLACE MASKING POLICY CLAIMS_DW.RAW.MP_DOB_MASK AS
    (val DATE) RETURNS DATE ->
    CASE
        WHEN CURRENT_ROLE() IN ('CLAIMS_ETL_ROLE', 'SYSADMIN', 'ACCOUNTADMIN')
            THEN val
        WHEN val IS NOT NULL
            THEN DATE_FROM_PARTS(YEAR(val), 1, 1)
        ELSE NULL
    END
COMMENT = 'Masks DOB - shows January 1 of birth year to non-privileged roles';


-- Phone number masking
CREATE OR REPLACE MASKING POLICY CLAIMS_DW.RAW.MP_PHONE_MASK AS
    (val VARCHAR) RETURNS VARCHAR ->
    CASE
        WHEN CURRENT_ROLE() IN ('CLAIMS_ETL_ROLE', 'SYSADMIN', 'ACCOUNTADMIN')
            THEN val
        WHEN val IS NOT NULL AND LENGTH(val) >= 4
            THEN REPEAT('*', LENGTH(val) - 4) || RIGHT(val, 4)
        ELSE NULL
    END
COMMENT = 'Masks phone numbers - shows last 4 digits to non-privileged roles';


-- Email masking
CREATE OR REPLACE MASKING POLICY CLAIMS_DW.RAW.MP_EMAIL_MASK AS
    (val VARCHAR) RETURNS VARCHAR ->
    CASE
        WHEN CURRENT_ROLE() IN ('CLAIMS_ETL_ROLE', 'SYSADMIN', 'ACCOUNTADMIN')
            THEN val
        WHEN val IS NOT NULL AND CHARINDEX('@', val) > 0
            THEN LEFT(val, 1) || '***@' || SPLIT_PART(val, '@', 2)
        ELSE NULL
    END
COMMENT = 'Masks email - shows first char and domain to non-privileged roles';


-- Address masking: full redaction
CREATE OR REPLACE MASKING POLICY CLAIMS_DW.RAW.MP_ADDRESS_MASK AS
    (val VARCHAR) RETURNS VARCHAR ->
    CASE
        WHEN CURRENT_ROLE() IN ('CLAIMS_ETL_ROLE', 'SYSADMIN', 'ACCOUNTADMIN')
            THEN val
        ELSE '[REDACTED]'
    END
COMMENT = 'Fully redacts address for non-privileged roles';


-- Member ID partial masking: show last 4 characters
CREATE OR REPLACE MASKING POLICY CLAIMS_DW.RAW.MP_MEMBER_ID_MASK AS
    (val VARCHAR) RETURNS VARCHAR ->
    CASE
        WHEN CURRENT_ROLE() IN ('CLAIMS_ETL_ROLE', 'CLAIMS_REPORTING_ROLE', 'SYSADMIN', 'ACCOUNTADMIN')
            THEN val
        WHEN val IS NOT NULL AND LENGTH(val) > 4
            THEN REPEAT('*', LENGTH(val) - 4) || RIGHT(val, 4)
        ELSE val
    END
COMMENT = 'Partially masks member ID for highly restricted roles';


-- =============================================================================
-- Apply Masking Policies to RAW_MEMBER_ELIGIBILITY columns
-- Masking is ALWAYS ACTIVE once applied (Snowflake enforces at query time)
-- =============================================================================

-- SSN
ALTER TABLE IF EXISTS CLAIMS_DW.RAW.RAW_MEMBER_ELIGIBILITY
    MODIFY COLUMN ssn_encrypted
    SET MASKING POLICY CLAIMS_DW.RAW.MP_SSN_MASK;

-- First name
ALTER TABLE IF EXISTS CLAIMS_DW.RAW.RAW_MEMBER_ELIGIBILITY
    MODIFY COLUMN first_name
    SET MASKING POLICY CLAIMS_DW.RAW.MP_NAME_MASK;

-- Last name
ALTER TABLE IF EXISTS CLAIMS_DW.RAW.RAW_MEMBER_ELIGIBILITY
    MODIFY COLUMN last_name
    SET MASKING POLICY CLAIMS_DW.RAW.MP_NAME_MASK;

-- Date of birth
ALTER TABLE IF EXISTS CLAIMS_DW.RAW.RAW_MEMBER_ELIGIBILITY
    MODIFY COLUMN date_of_birth
    SET MASKING POLICY CLAIMS_DW.RAW.MP_DOB_MASK;

-- Phone
ALTER TABLE IF EXISTS CLAIMS_DW.RAW.RAW_MEMBER_ELIGIBILITY
    MODIFY COLUMN phone_number
    SET MASKING POLICY CLAIMS_DW.RAW.MP_PHONE_MASK;

-- Email
ALTER TABLE IF EXISTS CLAIMS_DW.RAW.RAW_MEMBER_ELIGIBILITY
    MODIFY COLUMN email
    SET MASKING POLICY CLAIMS_DW.RAW.MP_EMAIL_MASK;

-- Address line 1
ALTER TABLE IF EXISTS CLAIMS_DW.RAW.RAW_MEMBER_ELIGIBILITY
    MODIFY COLUMN address_line_1
    SET MASKING POLICY CLAIMS_DW.RAW.MP_ADDRESS_MASK;

-- Address line 2
ALTER TABLE IF EXISTS CLAIMS_DW.RAW.RAW_MEMBER_ELIGIBILITY
    MODIFY COLUMN address_line_2
    SET MASKING POLICY CLAIMS_DW.RAW.MP_ADDRESS_MASK;


-- =============================================================================
-- Verification query (run as non-privileged role to test masking)
-- =============================================================================
-- USE ROLE CLAIMS_REPORTING_ROLE;
-- SELECT
--     member_id,
--     first_name,       -- should show 'J***'
--     last_name,        -- should show 'D***'
--     date_of_birth,    -- should show '1985-01-01' instead of actual DOB
--     ssn_encrypted,    -- should show 'XXX-XX-1234'
--     phone_number,     -- should show '******1234'
--     email,            -- should show 'j***@example.com'
--     address_line_1    -- should show '[REDACTED]'
-- FROM CLAIMS_DW.RAW.RAW_MEMBER_ELIGIBILITY
-- LIMIT 5;
