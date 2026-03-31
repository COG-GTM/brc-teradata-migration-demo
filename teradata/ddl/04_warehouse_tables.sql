/*******************************************************************************
 * Barclays Teradata Migration Demo - Warehouse (DWH) Tables
 *
 * Dimensional model tables in BARCLAYS_DWH:
 *   - dim_customer (SCD Type 2)
 *   - dim_account
 *   - dim_date
 *   - fct_transaction
 *   - fct_daily_balance
 *
 * Teradata-specific features:
 *   - MERGE INTO patterns
 *   - PERIOD data types
 *   - Temporal table semantics
 *   - COMPRESS on flag columns
 ******************************************************************************/

DATABASE BARCLAYS_DWH;

-- =============================================================================
-- DIM_CUSTOMER: SCD Type 2 customer dimension
-- =============================================================================
CREATE MULTISET TABLE BARCLAYS_DWH.DIM_CUSTOMER, FALLBACK,
    NO BEFORE JOURNAL,
    NO AFTER JOURNAL,
    CHECKSUM = DEFAULT,
    DEFAULT MERGEBLOCKRATIO
(
    customer_sk         INTEGER          NOT NULL GENERATED ALWAYS AS IDENTITY
                        (START WITH 1 INCREMENT BY 1),
    customer_id         INTEGER          NOT NULL,
    first_name          VARCHAR(100)     CHARACTER SET LATIN NOT CASESPECIFIC,
    last_name           VARCHAR(100)     CHARACTER SET LATIN NOT CASESPECIFIC,
    date_of_birth       DATE FORMAT 'YYYY-MM-DD',
    nationality         CHAR(2)          CHARACTER SET LATIN NOT CASESPECIFIC,
    kyc_status          VARCHAR(20)      CHARACTER SET LATIN NOT CASESPECIFIC,
    risk_rating         CHAR(1)          CHARACTER SET LATIN NOT CASESPECIFIC,
    segment             VARCHAR(20)      CHARACTER SET LATIN NOT CASESPECIFIC,
    postcode            VARCHAR(10)      CHARACTER SET LATIN NOT CASESPECIFIC,
    country             CHAR(2)          CHARACTER SET LATIN NOT CASESPECIFIC,
    validity_period     PERIOD(DATE)     NOT NULL,
    is_current          CHAR(1)          CHARACTER SET LATIN NOT CASESPECIFIC
                        COMPRESS ('Y', 'N')  DEFAULT 'Y',
    effective_from      DATE FORMAT 'YYYY-MM-DD' NOT NULL,
    effective_to        DATE FORMAT 'YYYY-MM-DD' DEFAULT DATE '9999-12-31',
    etl_batch_id        BIGINT,
    etl_loaded_ts       TIMESTAMP(6)     DEFAULT CURRENT_TIMESTAMP(6)
)
PRIMARY INDEX (customer_id);

COMMENT ON TABLE BARCLAYS_DWH.DIM_CUSTOMER
    AS 'SCD Type 2 customer dimension with PERIOD data type for temporal queries';

COLLECT STATISTICS
    COLUMN (customer_sk),
    COLUMN (customer_id),
    COLUMN (is_current),
    COLUMN (validity_period)
ON BARCLAYS_DWH.DIM_CUSTOMER;


-- =============================================================================
-- DIM_ACCOUNT: account dimension
-- =============================================================================
CREATE MULTISET TABLE BARCLAYS_DWH.DIM_ACCOUNT, FALLBACK,
    NO BEFORE JOURNAL,
    NO AFTER JOURNAL,
    CHECKSUM = DEFAULT,
    DEFAULT MERGEBLOCKRATIO
(
    account_sk          INTEGER          NOT NULL GENERATED ALWAYS AS IDENTITY
                        (START WITH 1 INCREMENT BY 1),
    account_id          INTEGER          NOT NULL,
    customer_id         INTEGER          NOT NULL,
    account_type        VARCHAR(30)      CHARACTER SET LATIN NOT CASESPECIFIC,
    currency            CHAR(3)          CHARACTER SET LATIN NOT CASESPECIFIC,
    branch_code         CHAR(6)          CHARACTER SET LATIN NOT CASESPECIFIC,
    sort_code           CHAR(6)          CHARACTER SET LATIN NOT CASESPECIFIC,
    status              VARCHAR(10)      CHARACTER SET LATIN NOT CASESPECIFIC,
    open_date           DATE FORMAT 'YYYY-MM-DD',
    close_date          DATE FORMAT 'YYYY-MM-DD',
    credit_limit        DECIMAL(15,2),
    overdraft_limit     DECIMAL(15,2),
    is_current          CHAR(1)          CHARACTER SET LATIN NOT CASESPECIFIC
                        COMPRESS ('Y', 'N')  DEFAULT 'Y',
    etl_batch_id        BIGINT,
    etl_loaded_ts       TIMESTAMP(6)     DEFAULT CURRENT_TIMESTAMP(6)
)
PRIMARY INDEX (account_id);

COMMENT ON TABLE BARCLAYS_DWH.DIM_ACCOUNT
    AS 'Account dimension - current state with status tracking';

COLLECT STATISTICS
    COLUMN (account_sk),
    COLUMN (account_id),
    COLUMN (customer_id),
    COLUMN (account_type),
    COLUMN (status)
ON BARCLAYS_DWH.DIM_ACCOUNT;


-- =============================================================================
-- DIM_DATE: calendar dimension
-- =============================================================================
CREATE SET TABLE BARCLAYS_DWH.DIM_DATE, FALLBACK,
    NO BEFORE JOURNAL,
    NO AFTER JOURNAL,
    CHECKSUM = DEFAULT,
    DEFAULT MERGEBLOCKRATIO
(
    date_key            INTEGER          NOT NULL,
    calendar_date       DATE FORMAT 'YYYY-MM-DD'  NOT NULL,
    day_of_week         SMALLINT,
    day_name            VARCHAR(10)      CHARACTER SET LATIN NOT CASESPECIFIC,
    day_of_month        SMALLINT,
    day_of_year         SMALLINT,
    week_of_year        SMALLINT,
    month_number        SMALLINT,
    month_name          VARCHAR(10)      CHARACTER SET LATIN NOT CASESPECIFIC,
    quarter_number      SMALLINT,
    year_number         INTEGER,
    fiscal_year         INTEGER,
    fiscal_quarter      SMALLINT,
    is_weekend          CHAR(1)          CHARACTER SET LATIN NOT CASESPECIFIC
                        COMPRESS ('Y', 'N'),
    is_bank_holiday     CHAR(1)          CHARACTER SET LATIN NOT CASESPECIFIC
                        COMPRESS ('Y', 'N')  DEFAULT 'N',
    is_business_day     CHAR(1)          CHARACTER SET LATIN NOT CASESPECIFIC
                        COMPRESS ('Y', 'N')
)
PRIMARY INDEX (date_key)
UNIQUE INDEX (calendar_date);

COMMENT ON TABLE BARCLAYS_DWH.DIM_DATE
    AS 'Calendar dimension with UK bank holiday flags';


-- =============================================================================
-- FCT_TRANSACTION: transaction fact table
-- =============================================================================
CREATE MULTISET TABLE BARCLAYS_DWH.FCT_TRANSACTION, FALLBACK,
    NO BEFORE JOURNAL,
    NO AFTER JOURNAL,
    CHECKSUM = DEFAULT,
    DEFAULT MERGEBLOCKRATIO
(
    transaction_sk      BIGINT           NOT NULL GENERATED ALWAYS AS IDENTITY
                        (START WITH 1 INCREMENT BY 1),
    transaction_id      BIGINT           NOT NULL,
    account_sk          INTEGER,
    customer_sk         INTEGER,
    date_key            INTEGER          NOT NULL,
    counterparty_id     INTEGER,
    transaction_type    VARCHAR(20)      CHARACTER SET LATIN NOT CASESPECIFIC,
    channel             VARCHAR(20)      CHARACTER SET LATIN NOT CASESPECIFIC,
    amount              DECIMAL(18,2)    NOT NULL,
    signed_amount       DECIMAL(18,2)    NOT NULL,
    currency            CHAR(3)          CHARACTER SET LATIN NOT CASESPECIFIC,
    value_band          VARCHAR(20)      CHARACTER SET LATIN NOT CASESPECIFIC,
    balance_after       DECIMAL(18,2),
    description         VARCHAR(500)     CHARACTER SET LATIN NOT CASESPECIFIC,
    reference_number    VARCHAR(50)      CHARACTER SET LATIN NOT CASESPECIFIC,
    etl_batch_id        BIGINT,
    etl_loaded_ts       TIMESTAMP(6)     DEFAULT CURRENT_TIMESTAMP(6)
)
PRIMARY INDEX (transaction_id)
PARTITION BY RANGE_N(
    date_key BETWEEN 20200101 AND 20301231
    EACH 100  /* monthly partitions based on YYYYMMDD key */
);

COMMENT ON TABLE BARCLAYS_DWH.FCT_TRANSACTION
    AS 'Transaction fact table with surrogate keys and PPI on date_key';

COLLECT STATISTICS
    COLUMN (transaction_id),
    COLUMN (account_sk),
    COLUMN (customer_sk),
    COLUMN (date_key),
    COLUMN (transaction_type),
    COLUMN (PARTITION)
ON BARCLAYS_DWH.FCT_TRANSACTION;


-- =============================================================================
-- FCT_DAILY_BALANCE: daily account balance snapshots
-- =============================================================================
CREATE MULTISET TABLE BARCLAYS_DWH.FCT_DAILY_BALANCE, FALLBACK,
    NO BEFORE JOURNAL,
    NO AFTER JOURNAL,
    CHECKSUM = DEFAULT,
    DEFAULT MERGEBLOCKRATIO
(
    balance_sk          BIGINT           NOT NULL GENERATED ALWAYS AS IDENTITY
                        (START WITH 1 INCREMENT BY 1),
    account_sk          INTEGER          NOT NULL,
    date_key            INTEGER          NOT NULL,
    opening_balance     DECIMAL(18,2),
    closing_balance     DECIMAL(18,2),
    total_debits        DECIMAL(18,2),
    total_credits       DECIMAL(18,2),
    transaction_count   INTEGER,
    currency            CHAR(3)          CHARACTER SET LATIN NOT CASESPECIFIC,
    etl_batch_id        BIGINT,
    etl_loaded_ts       TIMESTAMP(6)     DEFAULT CURRENT_TIMESTAMP(6)
)
PRIMARY INDEX (account_sk, date_key)
PARTITION BY RANGE_N(
    date_key BETWEEN 20200101 AND 20301231
    EACH 100
);

COMMENT ON TABLE BARCLAYS_DWH.FCT_DAILY_BALANCE
    AS 'Daily account balance snapshots for trend analysis';

COLLECT STATISTICS
    COLUMN (account_sk, date_key),
    COLUMN (date_key),
    COLUMN (PARTITION)
ON BARCLAYS_DWH.FCT_DAILY_BALANCE;


-- =============================================================================
-- MERGE pattern for DIM_ACCOUNT upsert (used by stored procedure)
-- =============================================================================
/*
MERGE INTO BARCLAYS_DWH.DIM_ACCOUNT tgt
USING BARCLAYS_STG.V_ACCOUNT_CURRENT src
ON tgt.account_id = src.account_id AND tgt.is_current = 'Y'
WHEN MATCHED THEN UPDATE SET
    status          = src.status,
    close_date      = src.close_date,
    credit_limit    = src.credit_limit,
    overdraft_limit = src.overdraft_limit,
    etl_loaded_ts   = CURRENT_TIMESTAMP(6)
WHEN NOT MATCHED THEN INSERT (
    account_id, customer_id, account_type, currency,
    branch_code, sort_code, status, open_date, close_date,
    credit_limit, overdraft_limit, is_current, etl_loaded_ts
) VALUES (
    src.account_id, src.customer_id, src.account_type, src.currency,
    src.branch_code, src.sort_code, src.status, src.open_date, src.close_date,
    src.credit_limit, src.overdraft_limit, 'Y', CURRENT_TIMESTAMP(6)
);
*/
