-- =============================================================================
-- Snowpipe Configuration: Raw Data Ingestion
-- Replaces:
--   - BTEQ .IMPORT commands from flat files
--   - Databricks Auto Loader (cloudFiles) streaming ingestion
--   - Manual COPY INTO from external stages
--
-- Each Snowpipe monitors an S3/Azure/GCS stage for new files and
-- automatically loads them into the raw landing tables.
-- =============================================================================

-- Stage for raw data files
CREATE OR REPLACE STAGE healthcare_consolidated.stages.raw_data_stage
    URL = 's3://healthcare-raw-data/'  -- Replace with actual bucket
    FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"')
    COMMENT = 'Raw data landing stage. Replaces BTEQ flat file import paths.';


-- Snowpipe: Customer data
CREATE OR REPLACE PIPE healthcare_consolidated.pipes.customer_pipe
    AUTO_INGEST = TRUE
    COMMENT = 'Auto-ingest customer data. Replaces BTEQ .IMPORT and Databricks Auto Loader.'
AS
    COPY INTO healthcare_consolidated.raw.customer
    FROM @healthcare_consolidated.stages.raw_data_stage/customers/
    FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"')
    ON_ERROR = 'CONTINUE';


-- Snowpipe: Account data
CREATE OR REPLACE PIPE healthcare_consolidated.pipes.account_pipe
    AUTO_INGEST = TRUE
    COMMENT = 'Auto-ingest account data.'
AS
    COPY INTO healthcare_consolidated.raw.account
    FROM @healthcare_consolidated.stages.raw_data_stage/accounts/
    FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"')
    ON_ERROR = 'CONTINUE';


-- Snowpipe: Transaction data (highest volume)
CREATE OR REPLACE PIPE healthcare_consolidated.pipes.transaction_pipe
    AUTO_INGEST = TRUE
    COMMENT = 'Auto-ingest transaction data. High-volume pipe replacing BTEQ bulk load.'
AS
    COPY INTO healthcare_consolidated.raw.transaction
    FROM @healthcare_consolidated.stages.raw_data_stage/transactions/
    FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"')
    ON_ERROR = 'CONTINUE';


-- Snowpipe: Market data
CREATE OR REPLACE PIPE healthcare_consolidated.pipes.market_data_pipe
    AUTO_INGEST = TRUE
    COMMENT = 'Auto-ingest market data.'
AS
    COPY INTO healthcare_consolidated.raw.market_data
    FROM @healthcare_consolidated.stages.raw_data_stage/market_data/
    FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"')
    ON_ERROR = 'CONTINUE';


-- Snowpipe: Counterparty data
CREATE OR REPLACE PIPE healthcare_consolidated.pipes.counterparty_pipe
    AUTO_INGEST = TRUE
    COMMENT = 'Auto-ingest counterparty data.'
AS
    COPY INTO healthcare_consolidated.raw.counterparty
    FROM @healthcare_consolidated.stages.raw_data_stage/counterparties/
    FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"')
    ON_ERROR = 'CONTINUE';


-- COPY INTO fallback (for initial bulk load or catch-up)
-- This replaces the BTEQ .IMPORT for historical data loading
/*
COPY INTO healthcare_consolidated.raw.customer
FROM @healthcare_consolidated.stages.raw_data_stage/customers/
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"')
PATTERN = '.*\.csv'
ON_ERROR = 'CONTINUE'
FORCE = TRUE;
*/
