/*******************************************************************************
 * Barclays Teradata Migration Demo - Staging Views
 *
 * Teradata-specific features used:
 *   - REPLACE VIEW
 *   - QUALIFY ROW_NUMBER()
 *   - ZEROIFNULL / NULLIFZERO
 *   - CASESPECIFIC / NOT CASESPECIFIC
 *   - COALESCE with Teradata date arithmetic
 *   - LOCK ROW FOR ACCESS (dirty reads for performance)
 ******************************************************************************/

DATABASE BARCLAYS_STG;

-- =============================================================================
-- V_CUSTOMER_LATEST: deduplicated customer view (latest record per customer)
-- =============================================================================
REPLACE VIEW BARCLAYS_STG.V_CUSTOMER_LATEST AS
LOCK ROW FOR ACCESS
SELECT
    customer_id,
    first_name,
    last_name,
    date_of_birth,
    nationality,
    kyc_status,
    risk_rating,
    segment,
    email,
    phone_number,
    postcode,
    country,
    onboarding_date,
    last_updated_ts
FROM BARCLAYS_RAW.CUSTOMER
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY customer_id
    ORDER BY last_updated_ts DESC
) = 1;

COMMENT ON BARCLAYS_STG.V_CUSTOMER_LATEST
    AS 'Latest customer record - deduped using QUALIFY ROW_NUMBER()';


-- =============================================================================
-- V_ACCOUNT_CURRENT: active accounts with enriched status
-- =============================================================================
REPLACE VIEW BARCLAYS_STG.V_ACCOUNT_CURRENT AS
LOCK ROW FOR ACCESS
SELECT
    a.account_id,
    a.customer_id,
    a.account_type,
    a.currency,
    a.branch_code,
    a.sort_code,
    a.status,
    a.open_date,
    a.close_date,
    ZEROIFNULL(a.credit_limit)   AS credit_limit,
    ZEROIFNULL(a.overdraft_limit) AS overdraft_limit,
    CASE
        WHEN a.close_date IS NOT NULL THEN 'CLOSED'
        WHEN a.status = 'DORMANT' THEN 'DORMANT'
        WHEN (CURRENT_DATE - a.open_date) < 90 THEN 'NEW'
        ELSE 'ACTIVE'
    END AS derived_status,
    (CURRENT_DATE - a.open_date) AS days_since_opening,
    a.last_updated_ts
FROM BARCLAYS_RAW.ACCOUNT a
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY a.account_id
    ORDER BY a.last_updated_ts DESC
) = 1;

COMMENT ON BARCLAYS_STG.V_ACCOUNT_CURRENT
    AS 'Current account state with derived status and tenure';


-- =============================================================================
-- V_TRANSACTION_ENRICHED: transactions with type classification
-- =============================================================================
REPLACE VIEW BARCLAYS_STG.V_TRANSACTION_ENRICHED AS
LOCK ROW FOR ACCESS
SELECT
    t.transaction_id,
    t.account_id,
    t.transaction_date,
    t.transaction_time,
    t.amount,
    t.currency,
    t.transaction_type,
    t.counterparty_id,
    t.description (NOT CASESPECIFIC),
    t.channel,
    t.reference_number,
    ZEROIFNULL(t.balance_after) AS balance_after,
    CASE
        WHEN t.amount > 10000.00 THEN 'HIGH_VALUE'
        WHEN t.amount > 1000.00  THEN 'MEDIUM_VALUE'
        ELSE 'STANDARD'
    END AS value_band,
    CASE
        WHEN t.transaction_type IN ('DEBIT', 'FEE') THEN t.amount * -1
        ELSE t.amount
    END AS signed_amount,
    t.last_updated_ts
FROM BARCLAYS_RAW.TRANSACTION t;

COMMENT ON BARCLAYS_STG.V_TRANSACTION_ENRICHED
    AS 'Enriched transaction view with value band and signed amount';


-- =============================================================================
-- V_MARKET_DATA_LATEST: latest market data per instrument
-- =============================================================================
REPLACE VIEW BARCLAYS_STG.V_MARKET_DATA_LATEST AS
LOCK ROW FOR ACCESS
SELECT
    instrument_id,
    valuation_date,
    instrument_type,
    instrument_name,
    currency,
    ZEROIFNULL(mid_price) AS mid_price,
    ZEROIFNULL(bid_price) AS bid_price,
    ZEROIFNULL(ask_price) AS ask_price,
    NULLIFZERO(ask_price - bid_price) AS bid_ask_spread,
    source_system,
    last_updated_ts
FROM BARCLAYS_RAW.MARKET_DATA
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY instrument_id, valuation_date
    ORDER BY last_updated_ts DESC
) = 1;

COMMENT ON BARCLAYS_STG.V_MARKET_DATA_LATEST
    AS 'Deduplicated market data with latest values per instrument per day';


-- =============================================================================
-- V_COUNTERPARTY_SCREENED: counterparties with screening flags
-- =============================================================================
REPLACE VIEW BARCLAYS_STG.V_COUNTERPARTY_SCREENED AS
LOCK ROW FOR ACCESS
SELECT
    counterparty_id,
    counterparty_name (NOT CASESPECIFIC),
    counterparty_type,
    country_code,
    lei_code,
    swift_bic,
    risk_rating,
    sanctions_flag,
    pep_flag,
    CASE
        WHEN sanctions_flag = 'Y' OR pep_flag = 'Y' THEN 'HIGH'
        WHEN risk_rating = 'H' THEN 'ELEVATED'
        ELSE 'STANDARD'
    END AS screening_category,
    last_updated_ts
FROM BARCLAYS_RAW.COUNTERPARTY
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY counterparty_id
    ORDER BY last_updated_ts DESC
) = 1;

COMMENT ON BARCLAYS_STG.V_COUNTERPARTY_SCREENED
    AS 'Counterparty view with screening categorisation';
