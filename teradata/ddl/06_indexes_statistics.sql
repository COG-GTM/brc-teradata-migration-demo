/*******************************************************************************
 * Barclays Teradata Migration Demo - Indexes & Statistics
 *
 * Teradata-specific features:
 *   - Secondary Indexes (NUSI / USI)
 *   - Join Indexes
 *   - COLLECT STATISTICS on columns and indexes
 *   - Hash indexes
 *   - Aggregate join indexes
 ******************************************************************************/

DATABASE BARCLAYS_DWH;

-- =============================================================================
-- Secondary Indexes (Non-Unique / Unique)
-- =============================================================================

-- NUSI on customer segment for filtered queries
CREATE INDEX idx_customer_segment (segment) ON BARCLAYS_RAW.CUSTOMER;

-- NUSI on account status for operational queries
CREATE INDEX idx_account_status (status) ON BARCLAYS_RAW.ACCOUNT;

-- NUSI on transaction date for range scans
CREATE INDEX idx_txn_date (transaction_date) ON BARCLAYS_RAW.TRANSACTION;

-- NUSI on transaction account for join performance
CREATE INDEX idx_txn_account (account_id) ON BARCLAYS_RAW.TRANSACTION;

-- USI on counterparty LEI (globally unique)
CREATE UNIQUE INDEX idx_counterparty_lei (lei_code) ON BARCLAYS_RAW.COUNTERPARTY;

-- NUSI on counterparty SWIFT/BIC
CREATE INDEX idx_counterparty_swift (swift_bic) ON BARCLAYS_RAW.COUNTERPARTY;


-- =============================================================================
-- Join Indexes (materialised join views for performance)
-- =============================================================================

-- Aggregate join index: daily transaction summary by account and date
CREATE JOIN INDEX BARCLAYS_DWH.JI_DAILY_TXN_SUMMARY AS
SELECT
    t.account_id,
    t.transaction_date,
    t.transaction_type,
    t.currency,
    COUNT(*) AS txn_count,
    SUM(t.amount) AS total_amount,
    MIN(t.amount) AS min_amount,
    MAX(t.amount) AS max_amount
FROM BARCLAYS_RAW.TRANSACTION t
GROUP BY t.account_id, t.transaction_date, t.transaction_type, t.currency
PRIMARY INDEX (account_id, transaction_date);


-- Single-table join index: customer lookup by postcode
CREATE JOIN INDEX BARCLAYS_DWH.JI_CUSTOMER_POSTCODE AS
SELECT
    customer_id,
    first_name,
    last_name,
    postcode,
    segment,
    kyc_status
FROM BARCLAYS_RAW.CUSTOMER
PRIMARY INDEX (postcode);


-- =============================================================================
-- Comprehensive Statistics Collection
-- =============================================================================

-- RAW layer statistics
COLLECT STATISTICS USING SAMPLE 25.00 PERCENT
    COLUMN (customer_id),
    COLUMN (segment, kyc_status),
    COLUMN (nationality, segment),
    COLUMN (date_of_birth)
ON BARCLAYS_RAW.CUSTOMER;

COLLECT STATISTICS USING SAMPLE 25.00 PERCENT
    COLUMN (account_id),
    COLUMN (customer_id),
    COLUMN (account_type, status),
    COLUMN (branch_code),
    COLUMN (open_date)
ON BARCLAYS_RAW.ACCOUNT;

COLLECT STATISTICS USING SAMPLE 10.00 PERCENT
    COLUMN (transaction_id),
    COLUMN (account_id),
    COLUMN (transaction_date),
    COLUMN (counterparty_id),
    COLUMN (account_id, transaction_date),
    COLUMN (transaction_type, channel)
ON BARCLAYS_RAW.TRANSACTION;

COLLECT STATISTICS USING SAMPLE 25.00 PERCENT
    COLUMN (instrument_id, valuation_date),
    COLUMN (instrument_type),
    COLUMN (currency)
ON BARCLAYS_RAW.MARKET_DATA;

-- DWH layer statistics
COLLECT STATISTICS USING SAMPLE 25.00 PERCENT
    COLUMN (customer_sk),
    COLUMN (customer_id),
    COLUMN (customer_id, is_current),
    COLUMN (segment, is_current)
ON BARCLAYS_DWH.DIM_CUSTOMER;

COLLECT STATISTICS USING SAMPLE 25.00 PERCENT
    COLUMN (account_sk),
    COLUMN (account_id),
    COLUMN (customer_id),
    COLUMN (account_type, is_current)
ON BARCLAYS_DWH.DIM_ACCOUNT;

COLLECT STATISTICS USING SAMPLE 10.00 PERCENT
    COLUMN (transaction_sk),
    COLUMN (transaction_id),
    COLUMN (account_sk, date_key),
    COLUMN (customer_sk),
    COLUMN (date_key)
ON BARCLAYS_DWH.FCT_TRANSACTION;

COLLECT STATISTICS USING SAMPLE 10.00 PERCENT
    COLUMN (account_sk, date_key),
    COLUMN (date_key)
ON BARCLAYS_DWH.FCT_DAILY_BALANCE;

-- MART layer statistics
COLLECT STATISTICS USING SAMPLE 25.00 PERCENT
    COLUMN (customer_id, assessment_date),
    COLUMN (risk_rating),
    COLUMN (asset_class)
ON BARCLAYS_MART.MART_CREDIT_RISK;

COLLECT STATISTICS USING SAMPLE 25.00 PERCENT
    COLUMN (reporting_date, asset_class),
    COLUMN (rollup_level)
ON BARCLAYS_MART.MART_REGULATORY_CAPITAL;

COLLECT STATISTICS USING SAMPLE 25.00 PERCENT
    COLUMN (alert_id),
    COLUMN (customer_id),
    COLUMN (alert_date),
    COLUMN (alert_type, severity)
ON BARCLAYS_MART.MART_AML_ALERTS;

COLLECT STATISTICS USING SAMPLE 25.00 PERCENT
    COLUMN (reporting_month, business_line),
    COLUMN (rollup_level)
ON BARCLAYS_MART.MART_MONTHLY_PNL;
