/*******************************************************************************
 * Healthcare Claims Data Warehouse - Snowflake Database & Schema Definitions
 *
 * Snowflake-specific features used:
 *   - CREATE DATABASE / SCHEMA with DATA_RETENTION_TIME_IN_DAYS
 *   - Virtual Warehouses with auto-suspend and auto-resume
 *   - COMMENT ON for documentation
 *   - Role-based access control (RBAC)
 *
 * Architecture: RAW -> STAGING -> WAREHOUSE -> MART
 ******************************************************************************/

-- =============================================================================
-- Database: CLAIMS_DW
-- =============================================================================
CREATE DATABASE IF NOT EXISTS CLAIMS_DW
    DATA_RETENTION_TIME_IN_DAYS = 14
    COMMENT = 'Healthcare claims data warehouse - multi-layer architecture';

USE DATABASE CLAIMS_DW;

-- =============================================================================
-- Schema: RAW - Landing zone for ingested source data
-- =============================================================================
CREATE SCHEMA IF NOT EXISTS CLAIMS_DW.RAW
    WITH MANAGED ACCESS
    DATA_RETENTION_TIME_IN_DAYS = 7
    COMMENT = 'Raw landing zone for source system extracts (eligibility, claims, pharmacy)';

-- =============================================================================
-- Schema: STAGING - Cleansed, deduped, and conformed views
-- =============================================================================
CREATE SCHEMA IF NOT EXISTS CLAIMS_DW.STAGING
    WITH MANAGED ACCESS
    DATA_RETENTION_TIME_IN_DAYS = 7
    COMMENT = 'Staging layer with deduplication and conformance logic';

-- =============================================================================
-- Schema: WAREHOUSE - Dimensional model (star schema)
-- =============================================================================
CREATE SCHEMA IF NOT EXISTS CLAIMS_DW.WAREHOUSE
    WITH MANAGED ACCESS
    DATA_RETENTION_TIME_IN_DAYS = 30
    COMMENT = 'Core dimensional warehouse - SCD Type 2 dimensions and fact tables';

-- =============================================================================
-- Schema: MART - Reporting and analytics aggregates
-- =============================================================================
CREATE SCHEMA IF NOT EXISTS CLAIMS_DW.MART
    WITH MANAGED ACCESS
    DATA_RETENTION_TIME_IN_DAYS = 30
    COMMENT = 'Consumption-ready mart tables for reporting and analytics';

-- =============================================================================
-- Virtual Warehouses
-- =============================================================================

-- ETL warehouse: used by Tasks, Pipes, and stored procedures
CREATE WAREHOUSE IF NOT EXISTS CLAIMS_ETL_WH
    WAREHOUSE_SIZE       = 'MEDIUM'
    AUTO_SUSPEND         = 120          -- seconds
    AUTO_RESUME          = TRUE
    MIN_CLUSTER_COUNT    = 1
    MAX_CLUSTER_COUNT    = 3
    SCALING_POLICY       = 'STANDARD'
    INITIALLY_SUSPENDED  = TRUE
    COMMENT              = 'ETL processing warehouse for claims pipeline (Tasks, Pipes, SPs)';

-- Reporting warehouse: used by BI tools and ad-hoc queries
CREATE WAREHOUSE IF NOT EXISTS CLAIMS_REPORTING_WH
    WAREHOUSE_SIZE       = 'SMALL'
    AUTO_SUSPEND         = 300          -- 5 minutes
    AUTO_RESUME          = TRUE
    MIN_CLUSTER_COUNT    = 1
    MAX_CLUSTER_COUNT    = 2
    SCALING_POLICY       = 'ECONOMY'
    INITIALLY_SUSPENDED  = TRUE
    COMMENT              = 'Reporting warehouse for BI tools and analyst queries';

-- =============================================================================
-- Roles and Grants
-- =============================================================================

-- ETL service role
CREATE ROLE IF NOT EXISTS CLAIMS_ETL_ROLE
    COMMENT = 'Service role for claims ETL pipeline';

GRANT USAGE ON DATABASE CLAIMS_DW TO ROLE CLAIMS_ETL_ROLE;
GRANT USAGE ON ALL SCHEMAS IN DATABASE CLAIMS_DW TO ROLE CLAIMS_ETL_ROLE;
GRANT ALL PRIVILEGES ON SCHEMA CLAIMS_DW.RAW TO ROLE CLAIMS_ETL_ROLE;
GRANT ALL PRIVILEGES ON SCHEMA CLAIMS_DW.STAGING TO ROLE CLAIMS_ETL_ROLE;
GRANT ALL PRIVILEGES ON SCHEMA CLAIMS_DW.WAREHOUSE TO ROLE CLAIMS_ETL_ROLE;
GRANT ALL PRIVILEGES ON SCHEMA CLAIMS_DW.MART TO ROLE CLAIMS_ETL_ROLE;
GRANT USAGE ON WAREHOUSE CLAIMS_ETL_WH TO ROLE CLAIMS_ETL_ROLE;

-- Reporting / analyst role
CREATE ROLE IF NOT EXISTS CLAIMS_REPORTING_ROLE
    COMMENT = 'Read-only role for analysts and BI tools';

GRANT USAGE ON DATABASE CLAIMS_DW TO ROLE CLAIMS_REPORTING_ROLE;
GRANT USAGE ON SCHEMA CLAIMS_DW.WAREHOUSE TO ROLE CLAIMS_REPORTING_ROLE;
GRANT USAGE ON SCHEMA CLAIMS_DW.MART TO ROLE CLAIMS_REPORTING_ROLE;
GRANT SELECT ON ALL TABLES IN SCHEMA CLAIMS_DW.WAREHOUSE TO ROLE CLAIMS_REPORTING_ROLE;
GRANT SELECT ON ALL TABLES IN SCHEMA CLAIMS_DW.MART TO ROLE CLAIMS_REPORTING_ROLE;
GRANT SELECT ON FUTURE TABLES IN SCHEMA CLAIMS_DW.WAREHOUSE TO ROLE CLAIMS_REPORTING_ROLE;
GRANT SELECT ON FUTURE TABLES IN SCHEMA CLAIMS_DW.MART TO ROLE CLAIMS_REPORTING_ROLE;
GRANT USAGE ON WAREHOUSE CLAIMS_REPORTING_WH TO ROLE CLAIMS_REPORTING_ROLE;
