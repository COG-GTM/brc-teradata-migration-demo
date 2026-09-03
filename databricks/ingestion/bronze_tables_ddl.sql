-- =============================================================================
-- bronze_tables_ddl.sql
--
-- Replaces (target-table portion of):
--   teradata/fastload/market_data_load.fl   -> BARCLAYS_RAW.MARKET_DATA
--   teradata/tpt/transaction_load.tpt       -> BARCLAYS_RAW.TRANSACTION
--   teradata/multiload/account_balance_upsert.ml -> BARCLAYS_DWH.FCT_DAILY_BALANCE
--
-- Mapping rationale:
--   Teradata load utilities require the target table plus a pair of error
--   tables (ET/UV) and a log table to exist before the job starts. On
--   Databricks the equivalent state is held by Delta itself: `_rescued_data`
--   (Auto Loader) and COPY INTO's idempotent file ledger replace the error
--   tables, and the Delta transaction log replaces the MultiLoad log table.
--   The Teradata primary index / PPI choices become Delta liquid clustering
--   keys; no distribution key is needed because storage is decoupled.
--
--   Run once per environment (or via the `bootstrap` task of the daily job).
--   Catalog/schema names are parameterised for Unity Catalog.
-- =============================================================================

-- Widget-style parameters when run from a Databricks SQL task:
--   catalog = barclays        schema_raw = raw        schema_dwh = dwh
--   landing_volume = /Volumes/barclays/raw/landing

CREATE CATALOG IF NOT EXISTS ${catalog};

CREATE SCHEMA IF NOT EXISTS ${catalog}.${schema_raw}
  COMMENT 'Bronze landing zone. Replaces Teradata database BARCLAYS_RAW.';

CREATE SCHEMA IF NOT EXISTS ${catalog}.${schema_dwh}
  COMMENT 'Warehouse layer. Replaces Teradata database BARCLAYS_DWH.';

-- Unity Catalog volume replacing the ${INPUT_DIR} NFS mount that FastLoad,
-- MultiLoad and TPT read their delimited .dat files from.
CREATE VOLUME IF NOT EXISTS ${catalog}.${schema_raw}.landing
  COMMENT 'Landing volume for source-system extracts (was ${INPUT_DIR}).';

-- -----------------------------------------------------------------------------
-- MARKET_DATA — FastLoad target (teradata/fastload/market_data_load.fl)
-- Teradata PI: instrument_id, valuation_date -> liquid clustering on the same
-- columns; DECIMAL(18,8) price precision preserved.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ${catalog}.${schema_raw}.market_data (
    instrument_id     STRING       COMMENT 'Instrument identifier (VARCHAR(20) in Teradata)',
    valuation_date    DATE         COMMENT 'Valuation date, parsed from yyyy-MM-dd text',
    instrument_type   STRING,
    instrument_name   STRING,
    currency          STRING       COMMENT 'ISO 4217 currency code',
    mid_price         DECIMAL(18,8),
    bid_price         DECIMAL(18,8),
    ask_price         DECIMAL(18,8),
    source_system     STRING,
    _source_file      STRING       COMMENT 'Auto Loader input_file_name(); replaces FastLoad ET table lineage',
    _ingested_at      TIMESTAMP,
    _rescued_data     STRING       COMMENT 'Malformed/unexpected fields; replaces MKTDATA_FL_ET / _UV'
)
USING DELTA
CLUSTER BY (valuation_date, instrument_id)
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
)
COMMENT 'Bronze market data. Replaces Teradata BARCLAYS_RAW.MARKET_DATA loaded by market_data_load.fl.';

-- -----------------------------------------------------------------------------
-- TRANSACTION — TPT LOAD target (teradata/tpt/transaction_load.tpt)
-- Teradata PI: transaction_id, PPI: RANGE_N on transaction_date -> clustering
-- on transaction_date first so date-ranged reads prune files.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ${catalog}.${schema_raw}.transaction (
    transaction_id    STRING,
    account_id        STRING,
    transaction_date  DATE,
    transaction_time  STRING       COMMENT 'TIME(6) in Teradata; kept as text in bronze, cast in dbt staging',
    amount            DECIMAL(18,2),
    currency          STRING,
    transaction_type  STRING,
    counterparty_id   STRING,
    description       STRING,
    channel           STRING,
    reference_number  STRING,
    _source_file      STRING,
    _ingested_at      TIMESTAMP,
    _rescued_data     STRING       COMMENT 'Replaces TXN_TPT_ET / TXN_TPT_UV error tables'
)
USING DELTA
CLUSTER BY (transaction_date, account_id)
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true'
)
COMMENT 'Bronze transactions. Replaces Teradata BARCLAYS_RAW.TRANSACTION loaded by transaction_load.tpt.';

-- -----------------------------------------------------------------------------
-- FCT_DAILY_BALANCE — MultiLoad upsert target
-- (teradata/multiload/account_balance_upsert.ml)
-- The MultiLoad "DO INSERT FOR MISSING UPDATE ROWS" matching key
-- (account_sk, date_key) becomes the MERGE ON key and the clustering key.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ${catalog}.${schema_dwh}.fct_daily_balance (
    account_sk        INT,
    date_key          INT          COMMENT 'yyyyMMdd integer date key, as in Teradata',
    opening_balance   DECIMAL(18,2),
    closing_balance   DECIMAL(18,2),
    total_debits      DECIMAL(18,2),
    total_credits     DECIMAL(18,2),
    transaction_count INT,
    currency          STRING,
    etl_loaded_ts     TIMESTAMP
)
USING DELTA
CLUSTER BY (date_key, account_sk)
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true'
)
COMMENT 'Daily balance fact. Replaces Teradata BARCLAYS_DWH.FCT_DAILY_BALANCE upserted by account_balance_upsert.ml.';

-- Staging table that the balance MERGE reads from (equivalent to the MultiLoad
-- .IMPORT INFILE work table). Truncated and reloaded each run.
CREATE TABLE IF NOT EXISTS ${catalog}.${schema_dwh}.stg_daily_balance_load (
    account_sk        INT,
    date_key          INT,
    opening_balance   DECIMAL(18,2),
    closing_balance   DECIMAL(18,2),
    total_debits      DECIMAL(18,2),
    total_credits     DECIMAL(18,2),
    transaction_count INT,
    currency          STRING,
    _source_file      STRING,
    _ingested_at      TIMESTAMP
)
USING DELTA
COMMENT 'Landing table for daily_balances_*.dat. Replaces the MultiLoad import work tables.';
