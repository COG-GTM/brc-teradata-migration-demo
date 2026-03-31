/*******************************************************************************
 * Barclays Teradata Migration Demo - Database Definitions
 *
 * Creates the four-tier database architecture typical of a Teradata EDW:
 *   RAW  -> STG  -> DWH  -> MART
 *
 * Each database is allocated permanent space, spool space, and inherits
 * default access rights from DBC.
 ******************************************************************************/

-- =============================================================================
-- RAW layer: landing zone for ingested source data
-- =============================================================================
CREATE DATABASE BARCLAYS_RAW FROM DBC
    AS PERM = 5e9,
       SPOOL = 2e9,
       TEMPORARY = 1e9
    NO FALLBACK
    NO BEFORE JOURNAL
    NO AFTER JOURNAL
    DEFAULT JOURNAL TABLE = BARCLAYS_RAW.JOURNAL_TBL;

COMMENT ON BARCLAYS_RAW AS 'Raw landing zone for source system extracts';

-- =============================================================================
-- STG layer: cleansed and conformed staging area
-- =============================================================================
CREATE DATABASE BARCLAYS_STG FROM DBC
    AS PERM = 5e9,
       SPOOL = 2e9,
       TEMPORARY = 1e9
    NO FALLBACK
    NO BEFORE JOURNAL
    NO AFTER JOURNAL;

COMMENT ON BARCLAYS_STG AS 'Staging area with cleansed and conformed data';

-- =============================================================================
-- DWH layer: dimensional data warehouse (star schema)
-- =============================================================================
CREATE DATABASE BARCLAYS_DWH FROM DBC
    AS PERM = 10e9,
       SPOOL = 5e9,
       TEMPORARY = 2e9
    NO FALLBACK
    NO BEFORE JOURNAL
    NO AFTER JOURNAL;

COMMENT ON BARCLAYS_DWH AS 'Core dimensional data warehouse (star schema)';

-- =============================================================================
-- MART layer: reporting and analytics marts
-- =============================================================================
CREATE DATABASE BARCLAYS_MART FROM DBC
    AS PERM = 10e9,
       SPOOL = 5e9,
       TEMPORARY = 2e9
    NO FALLBACK
    NO BEFORE JOURNAL
    NO AFTER JOURNAL;

COMMENT ON BARCLAYS_MART AS 'Reporting and analytics mart layer';

-- =============================================================================
-- Grant access to ETL service account
-- =============================================================================
GRANT ALL ON BARCLAYS_RAW  TO barclays_etl WITH GRANT OPTION;
GRANT ALL ON BARCLAYS_STG  TO barclays_etl WITH GRANT OPTION;
GRANT ALL ON BARCLAYS_DWH  TO barclays_etl WITH GRANT OPTION;
GRANT ALL ON BARCLAYS_MART TO barclays_etl WITH GRANT OPTION;

-- Grant read access to reporting accounts
GRANT SELECT ON BARCLAYS_MART TO barclays_report;
GRANT SELECT ON BARCLAYS_DWH  TO barclays_report;
