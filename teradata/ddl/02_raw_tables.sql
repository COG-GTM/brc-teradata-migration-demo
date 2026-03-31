/*******************************************************************************
 * Barclays Teradata Migration Demo - Raw Tables
 *
 * Teradata-specific features used:
 *   - SET / MULTISET tables
 *   - PRIMARY INDEX (hash distribution)
 *   - PARTITION BY RANGE_N (PPI)
 *   - FALLBACK / NO FALLBACK
 *   - JOURNAL options
 *   - COMPRESS values
 *   - CHARACTER SET LATIN / NOT CASESPECIFIC
 *   - FORMAT patterns
 *   - COLLECT STATISTICS
 ******************************************************************************/

DATABASE BARCLAYS_RAW;

-- =============================================================================
-- CUSTOMER: master customer record
-- SET table enforces uniqueness at the AMP level
-- =============================================================================
CREATE SET TABLE BARCLAYS_RAW.CUSTOMER, FALLBACK,
    NO BEFORE JOURNAL,
    NO AFTER JOURNAL,
    CHECKSUM = DEFAULT,
    DEFAULT MERGEBLOCKRATIO
(
    customer_id         INTEGER          NOT NULL,
    first_name          VARCHAR(100)     CHARACTER SET LATIN NOT CASESPECIFIC,
    last_name           VARCHAR(100)     CHARACTER SET LATIN NOT CASESPECIFIC,
    date_of_birth       DATE FORMAT 'YYYY-MM-DD',
    nationality         CHAR(2)          CHARACTER SET LATIN NOT CASESPECIFIC
                        COMPRESS ('GB', 'US', 'DE', 'FR', 'IE'),
    kyc_status          VARCHAR(20)      CHARACTER SET LATIN NOT CASESPECIFIC
                        COMPRESS ('VERIFIED', 'PENDING', 'EXPIRED'),
    risk_rating         CHAR(1)          CHARACTER SET LATIN NOT CASESPECIFIC
                        COMPRESS ('L', 'M', 'H'),
    onboarding_date     DATE FORMAT 'YYYY-MM-DD',
    segment             VARCHAR(20)      CHARACTER SET LATIN NOT CASESPECIFIC
                        COMPRESS ('RETAIL', 'WEALTH', 'CORPORATE'),
    email               VARCHAR(255)     CHARACTER SET LATIN NOT CASESPECIFIC,
    phone_number        VARCHAR(20)      CHARACTER SET LATIN NOT CASESPECIFIC,
    address_line_1      VARCHAR(200)     CHARACTER SET LATIN NOT CASESPECIFIC,
    address_line_2      VARCHAR(200)     CHARACTER SET LATIN NOT CASESPECIFIC,
    city                VARCHAR(100)     CHARACTER SET LATIN NOT CASESPECIFIC,
    postcode            VARCHAR(10)      CHARACTER SET LATIN NOT CASESPECIFIC,
    country             CHAR(2)          CHARACTER SET LATIN NOT CASESPECIFIC
                        COMPRESS ('GB'),
    last_updated_ts     TIMESTAMP(6)     DEFAULT CURRENT_TIMESTAMP(6)
)
PRIMARY INDEX (customer_id);

COMMENT ON TABLE BARCLAYS_RAW.CUSTOMER
    AS 'Master customer record - SET table ensures no duplicate rows per AMP';

COLLECT STATISTICS
    COLUMN (customer_id),
    COLUMN (nationality),
    COLUMN (kyc_status),
    COLUMN (segment),
    COLUMN (risk_rating)
ON BARCLAYS_RAW.CUSTOMER;


-- =============================================================================
-- ACCOUNT: customer accounts
-- MULTISET table with PPI on open_date (yearly partitions)
-- =============================================================================
CREATE MULTISET TABLE BARCLAYS_RAW.ACCOUNT, FALLBACK,
    NO BEFORE JOURNAL,
    NO AFTER JOURNAL,
    CHECKSUM = DEFAULT,
    DEFAULT MERGEBLOCKRATIO
(
    account_id          INTEGER          NOT NULL,
    customer_id         INTEGER          NOT NULL,
    account_type        VARCHAR(30)      CHARACTER SET LATIN NOT CASESPECIFIC
                        COMPRESS ('CURRENT', 'SAVINGS', 'ISA', 'MORTGAGE', 'LOAN', 'CREDIT_CARD'),
    currency            CHAR(3)          CHARACTER SET LATIN NOT CASESPECIFIC
                        COMPRESS ('GBP', 'EUR', 'USD'),
    branch_code         CHAR(6)          CHARACTER SET LATIN NOT CASESPECIFIC,
    sort_code           CHAR(6)          CHARACTER SET LATIN NOT CASESPECIFIC,
    status              VARCHAR(10)      CHARACTER SET LATIN NOT CASESPECIFIC
                        COMPRESS ('ACTIVE', 'DORMANT', 'CLOSED'),
    open_date           DATE FORMAT 'YYYY-MM-DD'  NOT NULL,
    close_date          DATE FORMAT 'YYYY-MM-DD',
    credit_limit        DECIMAL(15,2),
    overdraft_limit     DECIMAL(15,2),
    last_updated_ts     TIMESTAMP(6)     DEFAULT CURRENT_TIMESTAMP(6)
)
PRIMARY INDEX (account_id)
PARTITION BY RANGE_N(
    open_date BETWEEN DATE '2000-01-01' AND DATE '2030-12-31'
    EACH INTERVAL '1' YEAR
);

COMMENT ON TABLE BARCLAYS_RAW.ACCOUNT
    AS 'Customer accounts - MULTISET with yearly PPI on open_date';

COLLECT STATISTICS
    COLUMN (account_id),
    COLUMN (customer_id),
    COLUMN (account_type),
    COLUMN (status),
    COLUMN (open_date),
    COLUMN (PARTITION)
ON BARCLAYS_RAW.ACCOUNT;


-- =============================================================================
-- TRANSACTION: financial transactions
-- MULTISET table with PPI on transaction_date (monthly partitions)
-- =============================================================================
CREATE MULTISET TABLE BARCLAYS_RAW.TRANSACTION, FALLBACK,
    NO BEFORE JOURNAL,
    NO AFTER JOURNAL,
    CHECKSUM = DEFAULT,
    DEFAULT MERGEBLOCKRATIO
(
    transaction_id      BIGINT           NOT NULL,
    account_id          INTEGER          NOT NULL,
    transaction_date    DATE FORMAT 'YYYY-MM-DD'  NOT NULL,
    transaction_time    TIME(6),
    amount              DECIMAL(18,2)    NOT NULL,
    currency            CHAR(3)          CHARACTER SET LATIN NOT CASESPECIFIC
                        COMPRESS ('GBP', 'EUR', 'USD'),
    transaction_type    VARCHAR(20)      CHARACTER SET LATIN NOT CASESPECIFIC
                        COMPRESS ('DEBIT', 'CREDIT', 'TRANSFER', 'FEE', 'INTEREST', 'REVERSAL'),
    counterparty_id     INTEGER,
    description         VARCHAR(500)     CHARACTER SET LATIN NOT CASESPECIFIC,
    channel             VARCHAR(20)      CHARACTER SET LATIN NOT CASESPECIFIC
                        COMPRESS ('BRANCH', 'ONLINE', 'MOBILE', 'ATM', 'TELEPHONE', 'BATCH'),
    reference_number    VARCHAR(50)      CHARACTER SET LATIN NOT CASESPECIFIC,
    balance_after       DECIMAL(18,2),
    last_updated_ts     TIMESTAMP(6)     DEFAULT CURRENT_TIMESTAMP(6)
)
PRIMARY INDEX (transaction_id)
PARTITION BY RANGE_N(
    transaction_date BETWEEN DATE '2020-01-01' AND DATE '2030-12-31'
    EACH INTERVAL '1' MONTH
);

COMMENT ON TABLE BARCLAYS_RAW.TRANSACTION
    AS 'Financial transactions - MULTISET with monthly PPI on transaction_date';

COLLECT STATISTICS
    COLUMN (transaction_id),
    COLUMN (account_id),
    COLUMN (transaction_date),
    COLUMN (transaction_type),
    COLUMN (channel),
    COLUMN (PARTITION)
ON BARCLAYS_RAW.TRANSACTION;


-- =============================================================================
-- MARKET_DATA: daily market rates and prices
-- =============================================================================
CREATE MULTISET TABLE BARCLAYS_RAW.MARKET_DATA, FALLBACK,
    NO BEFORE JOURNAL,
    NO AFTER JOURNAL,
    CHECKSUM = DEFAULT,
    DEFAULT MERGEBLOCKRATIO
(
    instrument_id       VARCHAR(20)      CHARACTER SET LATIN NOT CASESPECIFIC NOT NULL,
    valuation_date      DATE FORMAT 'YYYY-MM-DD'  NOT NULL,
    instrument_type     VARCHAR(30)      CHARACTER SET LATIN NOT CASESPECIFIC
                        COMPRESS ('FX_RATE', 'INTEREST_RATE', 'EQUITY_PRICE', 'BOND_YIELD'),
    instrument_name     VARCHAR(100)     CHARACTER SET LATIN NOT CASESPECIFIC,
    currency            CHAR(3)          CHARACTER SET LATIN NOT CASESPECIFIC,
    mid_price           DECIMAL(18,8),
    bid_price           DECIMAL(18,8),
    ask_price           DECIMAL(18,8),
    source_system       VARCHAR(50)      CHARACTER SET LATIN NOT CASESPECIFIC,
    last_updated_ts     TIMESTAMP(6)     DEFAULT CURRENT_TIMESTAMP(6)
)
PRIMARY INDEX (instrument_id, valuation_date);

COMMENT ON TABLE BARCLAYS_RAW.MARKET_DATA
    AS 'Daily market rates (SONIA, LIBOR, FX) and instrument prices';

COLLECT STATISTICS
    COLUMN (instrument_id, valuation_date),
    COLUMN (instrument_type),
    COLUMN (valuation_date)
ON BARCLAYS_RAW.MARKET_DATA;


-- =============================================================================
-- COUNTERPARTY: counterparty reference data
-- =============================================================================
CREATE SET TABLE BARCLAYS_RAW.COUNTERPARTY, FALLBACK,
    NO BEFORE JOURNAL,
    NO AFTER JOURNAL,
    CHECKSUM = DEFAULT,
    DEFAULT MERGEBLOCKRATIO
(
    counterparty_id     INTEGER          NOT NULL,
    counterparty_name   VARCHAR(200)     CHARACTER SET LATIN NOT CASESPECIFIC,
    counterparty_type   VARCHAR(30)      CHARACTER SET LATIN NOT CASESPECIFIC
                        COMPRESS ('BANK', 'CORPORATE', 'GOVERNMENT', 'INDIVIDUAL', 'MERCHANT'),
    country_code        CHAR(2)          CHARACTER SET LATIN NOT CASESPECIFIC,
    lei_code            CHAR(20)         CHARACTER SET LATIN NOT CASESPECIFIC,
    swift_bic           VARCHAR(11)      CHARACTER SET LATIN NOT CASESPECIFIC,
    risk_rating         CHAR(1)          CHARACTER SET LATIN NOT CASESPECIFIC
                        COMPRESS ('L', 'M', 'H'),
    sanctions_flag      CHAR(1)          CHARACTER SET LATIN NOT CASESPECIFIC
                        COMPRESS ('Y', 'N'),
    pep_flag            CHAR(1)          CHARACTER SET LATIN NOT CASESPECIFIC
                        COMPRESS ('Y', 'N'),
    last_updated_ts     TIMESTAMP(6)     DEFAULT CURRENT_TIMESTAMP(6)
)
PRIMARY INDEX (counterparty_id);

COMMENT ON TABLE BARCLAYS_RAW.COUNTERPARTY
    AS 'Counterparty reference data including sanctions and PEP flags';

COLLECT STATISTICS
    COLUMN (counterparty_id),
    COLUMN (counterparty_type),
    COLUMN (country_code),
    COLUMN (sanctions_flag)
ON BARCLAYS_RAW.COUNTERPARTY;
