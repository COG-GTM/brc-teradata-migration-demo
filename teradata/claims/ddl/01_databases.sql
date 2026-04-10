/*******************************************************************************
 * Healthcare Claims Teradata Migration Demo - Database Definitions
 *
 * Creates the four-tier database architecture for a healthcare claims
 * data warehouse on Teradata:
 *   CLAIMS_RAW  -> CLAIMS_STG  -> CLAIMS_DWH  -> CLAIMS_MART
 *
 * Each database is allocated permanent space, spool space, and inherits
 * default access rights from DBC.
 ******************************************************************************/

-- =============================================================================
-- CLAIMS_RAW: landing zone for ingested source system extracts
-- Receives 837/835 claim files, eligibility feeds, pharmacy PBM feeds
-- =============================================================================
CREATE DATABASE CLAIMS_RAW FROM DBC
    AS PERM = 10e9,
       SPOOL = 5e9,
       TEMPORARY = 2e9
    NO FALLBACK
    NO BEFORE JOURNAL
    NO AFTER JOURNAL
    DEFAULT JOURNAL TABLE = CLAIMS_RAW.JOURNAL_TBL;

COMMENT ON CLAIMS_RAW AS 'Raw landing zone for healthcare claims source extracts (837/835, eligibility, pharmacy)';

-- =============================================================================
-- CLAIMS_STG: cleansed and conformed staging area
-- Deduplication, ADR processing, code standardization
-- =============================================================================
CREATE DATABASE CLAIMS_STG FROM DBC
    AS PERM = 10e9,
       SPOOL = 5e9,
       TEMPORARY = 2e9
    NO FALLBACK
    NO BEFORE JOURNAL
    NO AFTER JOURNAL;

COMMENT ON CLAIMS_STG AS 'Staging area with cleansed claims, ADR dedup, and code standardization';

-- =============================================================================
-- CLAIMS_DWH: dimensional data warehouse (star schema)
-- Conformed dimensions, fact tables, encounter groupings
-- =============================================================================
CREATE DATABASE CLAIMS_DWH FROM DBC
    AS PERM = 20e9,
       SPOOL = 10e9,
       TEMPORARY = 5e9
    NO FALLBACK
    NO BEFORE JOURNAL
    NO AFTER JOURNAL;

COMMENT ON CLAIMS_DWH AS 'Core dimensional claims data warehouse (star schema with SCD Type 2)';

-- =============================================================================
-- CLAIMS_MART: reporting and analytics marts
-- Member months, claim summaries, quality measures (HEDIS)
-- =============================================================================
CREATE DATABASE CLAIMS_MART FROM DBC
    AS PERM = 20e9,
       SPOOL = 10e9,
       TEMPORARY = 5e9
    NO FALLBACK
    NO BEFORE JOURNAL
    NO AFTER JOURNAL;

COMMENT ON CLAIMS_MART AS 'Reporting marts for claims analytics, member months, and quality measures';

-- =============================================================================
-- Grant access to ETL service account
-- =============================================================================
GRANT ALL ON CLAIMS_RAW  TO claims_etl WITH GRANT OPTION;
GRANT ALL ON CLAIMS_STG  TO claims_etl WITH GRANT OPTION;
GRANT ALL ON CLAIMS_DWH  TO claims_etl WITH GRANT OPTION;
GRANT ALL ON CLAIMS_MART TO claims_etl WITH GRANT OPTION;

-- Grant read access to reporting / analytics accounts
GRANT SELECT ON CLAIMS_MART TO claims_report;
GRANT SELECT ON CLAIMS_DWH  TO claims_report;

-- Grant read access to actuarial / data science accounts
GRANT SELECT ON CLAIMS_MART TO claims_actuarial;
GRANT SELECT ON CLAIMS_DWH  TO claims_actuarial;
