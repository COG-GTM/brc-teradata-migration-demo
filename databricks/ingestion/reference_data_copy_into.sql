-- =============================================================================
-- reference_data_copy_into.sql
--
-- Replaces: the source-system extract drops consumed by
--           teradata/scheduled_jobs/daily_etl_sequence.txt job
--           BRCL_DAILY_001_PREFLIGHT ("upstream source extracts must complete
--           by 05:30 GMT") and read by teradata/bteq/daily_batch_load.bteq via
--           BARCLAYS_RAW.CUSTOMER / ACCOUNT / COUNTERPARTY.
--
-- Mapping rationale:
--   The legacy warehouse had no dedicated utility script for these three
--   dimensions — they arrived as delimited files loaded by the same
--   FastLoad/TPT tooling pattern before the BTEQ batch ran. One COPY INTO per
--   file keeps that contract while making the load idempotent and
--   header-aware. Full-snapshot files are replayed with 'force' = 'true' after
--   truncation, matching the legacy "empty target then FastLoad" behaviour.
-- =============================================================================

-- Parameters: catalog, schema_raw, landing_path, delimiter

CREATE TABLE IF NOT EXISTS ${catalog}.${schema_raw}.customer (
    customer_id      STRING,
    first_name       STRING,
    last_name        STRING,
    date_of_birth    DATE,
    nationality      STRING,
    kyc_status       STRING,
    risk_rating      STRING,
    onboarding_date  DATE,
    segment          STRING,
    _source_file     STRING,
    _ingested_at     TIMESTAMP
) USING DELTA CLUSTER BY (customer_id)
COMMENT 'Bronze customer master. Replaces Teradata BARCLAYS_RAW.CUSTOMER.';

CREATE TABLE IF NOT EXISTS ${catalog}.${schema_raw}.account (
    account_id       STRING,
    customer_id      STRING,
    account_type     STRING,
    currency         STRING,
    branch_code      STRING,
    status           STRING,
    open_date        DATE,
    close_date       DATE,
    _source_file     STRING,
    _ingested_at     TIMESTAMP
) USING DELTA CLUSTER BY (account_id, customer_id)
COMMENT 'Bronze account master. Replaces Teradata BARCLAYS_RAW.ACCOUNT (PPI RANGE_N on open_date).';

CREATE TABLE IF NOT EXISTS ${catalog}.${schema_raw}.counterparty (
    counterparty_id       STRING,
    counterparty_name     STRING,
    counterparty_type     STRING,
    country_code          STRING,
    lei                   STRING,
    is_sanctions_listed   BOOLEAN,
    is_pep                BOOLEAN,
    _source_file          STRING,
    _ingested_at          TIMESTAMP
) USING DELTA CLUSTER BY (counterparty_id)
COMMENT 'Bronze counterparty reference. Replaces Teradata BARCLAYS_RAW.COUNTERPARTY.';

-- -----------------------------------------------------------------------------
TRUNCATE TABLE ${catalog}.${schema_raw}.customer;

COPY INTO ${catalog}.${schema_raw}.customer
FROM (
    SELECT
        CAST(customer_id AS STRING)                 AS customer_id,
        CAST(first_name AS STRING)                  AS first_name,
        CAST(last_name AS STRING)                   AS last_name,
        TO_DATE(date_of_birth, 'yyyy-MM-dd')        AS date_of_birth,
        CAST(nationality AS STRING)                 AS nationality,
        CAST(kyc_status AS STRING)                  AS kyc_status,
        CAST(risk_rating AS STRING)                 AS risk_rating,
        TO_DATE(onboarding_date, 'yyyy-MM-dd')      AS onboarding_date,
        CAST(segment AS STRING)                     AS segment,
        _metadata.file_path                         AS _source_file,
        CURRENT_TIMESTAMP()                         AS _ingested_at
    FROM '${landing_path}'
)
FILEFORMAT = CSV
PATTERN = 'customers*.csv'
FORMAT_OPTIONS ('header' = 'true', 'sep' = '${delimiter}', 'inferSchema' = 'false', 'mode' = 'PERMISSIVE')
COPY_OPTIONS ('force' = 'true');

-- -----------------------------------------------------------------------------
TRUNCATE TABLE ${catalog}.${schema_raw}.account;

COPY INTO ${catalog}.${schema_raw}.account
FROM (
    SELECT
        CAST(account_id AS STRING)                  AS account_id,
        CAST(customer_id AS STRING)                 AS customer_id,
        CAST(account_type AS STRING)                AS account_type,
        CAST(currency AS STRING)                    AS currency,
        CAST(branch_code AS STRING)                 AS branch_code,
        CAST(status AS STRING)                      AS status,
        TO_DATE(open_date, 'yyyy-MM-dd')            AS open_date,
        TO_DATE(NULLIF(close_date, ''), 'yyyy-MM-dd') AS close_date,
        _metadata.file_path                         AS _source_file,
        CURRENT_TIMESTAMP()                         AS _ingested_at
    FROM '${landing_path}'
)
FILEFORMAT = CSV
PATTERN = 'accounts*.csv'
FORMAT_OPTIONS ('header' = 'true', 'sep' = '${delimiter}', 'inferSchema' = 'false', 'mode' = 'PERMISSIVE')
COPY_OPTIONS ('force' = 'true');

-- -----------------------------------------------------------------------------
TRUNCATE TABLE ${catalog}.${schema_raw}.counterparty;

COPY INTO ${catalog}.${schema_raw}.counterparty
FROM (
    SELECT
        CAST(counterparty_id AS STRING)             AS counterparty_id,
        CAST(counterparty_name AS STRING)           AS counterparty_name,
        CAST(counterparty_type AS STRING)           AS counterparty_type,
        CAST(country_code AS STRING)                AS country_code,
        CAST(lei AS STRING)                         AS lei,
        CAST(is_sanctions_listed AS BOOLEAN)        AS is_sanctions_listed,
        CAST(is_pep AS BOOLEAN)                     AS is_pep,
        _metadata.file_path                         AS _source_file,
        CURRENT_TIMESTAMP()                         AS _ingested_at
    FROM '${landing_path}'
)
FILEFORMAT = CSV
PATTERN = 'counterparties*.csv'
FORMAT_OPTIONS ('header' = 'true', 'sep' = '${delimiter}', 'inferSchema' = 'false', 'mode' = 'PERMISSIVE')
COPY_OPTIONS ('force' = 'true');
