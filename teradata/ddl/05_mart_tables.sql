/*******************************************************************************
 * Barclays Teradata Migration Demo - Mart Tables
 *
 * Reporting mart tables in BARCLAYS_MART:
 *   - mart_credit_risk          (Basel III RWA)
 *   - mart_regulatory_capital   (capital adequacy)
 *   - mart_aml_alerts           (AML screening results)
 *   - mart_monthly_pnl          (P&L rollup)
 *
 * Teradata-specific features:
 *   - OLAP window functions (CSUM, MAVG, MDIFF)
 *   - GROUP BY ROLLUP
 *   - Aggregate UDFs
 ******************************************************************************/

DATABASE BARCLAYS_MART;

-- =============================================================================
-- MART_CREDIT_RISK: Basel III credit risk metrics per customer
-- =============================================================================
CREATE MULTISET TABLE BARCLAYS_MART.MART_CREDIT_RISK, FALLBACK,
    NO BEFORE JOURNAL,
    NO AFTER JOURNAL,
    CHECKSUM = DEFAULT,
    DEFAULT MERGEBLOCKRATIO
(
    risk_assessment_id  BIGINT           NOT NULL GENERATED ALWAYS AS IDENTITY
                        (START WITH 1 INCREMENT BY 1),
    customer_id         INTEGER          NOT NULL,
    assessment_date     DATE FORMAT 'YYYY-MM-DD'  NOT NULL,
    risk_rating         CHAR(1)          CHARACTER SET LATIN NOT CASESPECIFIC,
    probability_default DECIMAL(10,6),
    loss_given_default  DECIMAL(10,6),
    exposure_at_default DECIMAL(18,2),
    risk_weighted_asset DECIMAL(18,2),
    expected_loss       DECIMAL(18,2),
    unexpected_loss     DECIMAL(18,2),
    risk_weight_pct     DECIMAL(10,4),
    asset_class         VARCHAR(50)      CHARACTER SET LATIN NOT CASESPECIFIC
                        COMPRESS ('RETAIL_MORTGAGE', 'RETAIL_REVOLVING', 'RETAIL_OTHER',
                                  'CORPORATE', 'SOVEREIGN', 'INTERBANK'),
    model_version       VARCHAR(20)      CHARACTER SET LATIN NOT CASESPECIFIC,
    etl_batch_id        BIGINT,
    etl_loaded_ts       TIMESTAMP(6)     DEFAULT CURRENT_TIMESTAMP(6)
)
PRIMARY INDEX (customer_id, assessment_date);

COMMENT ON TABLE BARCLAYS_MART.MART_CREDIT_RISK
    AS 'Basel III credit risk metrics - PD, LGD, EAD, RWA per customer';


-- =============================================================================
-- MART_REGULATORY_CAPITAL: capital adequacy reporting
-- =============================================================================
CREATE MULTISET TABLE BARCLAYS_MART.MART_REGULATORY_CAPITAL, FALLBACK,
    NO BEFORE JOURNAL,
    NO AFTER JOURNAL,
    CHECKSUM = DEFAULT,
    DEFAULT MERGEBLOCKRATIO
(
    report_id           BIGINT           NOT NULL GENERATED ALWAYS AS IDENTITY
                        (START WITH 1 INCREMENT BY 1),
    reporting_date      DATE FORMAT 'YYYY-MM-DD'  NOT NULL,
    asset_class         VARCHAR(50)      CHARACTER SET LATIN NOT CASESPECIFIC,
    total_exposure      DECIMAL(18,2),
    total_rwa           DECIMAL(18,2),
    capital_required    DECIMAL(18,2),
    capital_ratio       DECIMAL(10,6),
    tier1_capital       DECIMAL(18,2),
    tier2_capital       DECIMAL(18,2),
    total_capital       DECIMAL(18,2),
    leverage_ratio      DECIMAL(10,6),
    countercyclical_buf DECIMAL(10,6),
    systemic_buf        DECIMAL(10,6),
    rollup_level        VARCHAR(20)      CHARACTER SET LATIN NOT CASESPECIFIC
                        COMPRESS ('DETAIL', 'ASSET_CLASS', 'TOTAL'),
    etl_batch_id        BIGINT,
    etl_loaded_ts       TIMESTAMP(6)     DEFAULT CURRENT_TIMESTAMP(6)
)
PRIMARY INDEX (reporting_date, asset_class);

COMMENT ON TABLE BARCLAYS_MART.MART_REGULATORY_CAPITAL
    AS 'Regulatory capital adequacy report with GROUP BY ROLLUP levels';


-- =============================================================================
-- MART_AML_ALERTS: anti-money-laundering screening results
-- =============================================================================
CREATE MULTISET TABLE BARCLAYS_MART.MART_AML_ALERTS, FALLBACK,
    NO BEFORE JOURNAL,
    NO AFTER JOURNAL,
    CHECKSUM = DEFAULT,
    DEFAULT MERGEBLOCKRATIO
(
    alert_id            BIGINT           NOT NULL GENERATED ALWAYS AS IDENTITY
                        (START WITH 1 INCREMENT BY 1),
    customer_id         INTEGER          NOT NULL,
    account_id          INTEGER,
    alert_date          DATE FORMAT 'YYYY-MM-DD'  NOT NULL,
    alert_type          VARCHAR(50)      CHARACTER SET LATIN NOT CASESPECIFIC
                        COMPRESS ('SANCTIONS_HIT', 'PEP_MATCH', 'UNUSUAL_ACTIVITY',
                                  'STRUCTURING', 'VELOCITY_BREACH', 'NAME_SCREENING'),
    severity            VARCHAR(10)      CHARACTER SET LATIN NOT CASESPECIFIC
                        COMPRESS ('LOW', 'MEDIUM', 'HIGH', 'CRITICAL'),
    match_score         DECIMAL(5,2),
    matched_entity      VARCHAR(200)     CHARACTER SET LATIN NOT CASESPECIFIC,
    alert_status        VARCHAR(20)      CHARACTER SET LATIN NOT CASESPECIFIC
                        COMPRESS ('OPEN', 'INVESTIGATING', 'ESCALATED', 'CLOSED_TRUE',
                                  'CLOSED_FALSE'),
    investigation_notes VARCHAR(4000)    CHARACTER SET LATIN NOT CASESPECIFIC,
    assigned_analyst    VARCHAR(100)     CHARACTER SET LATIN NOT CASESPECIFIC,
    etl_batch_id        BIGINT,
    etl_loaded_ts       TIMESTAMP(6)     DEFAULT CURRENT_TIMESTAMP(6)
)
PRIMARY INDEX (alert_id)
PARTITION BY RANGE_N(
    alert_date BETWEEN DATE '2020-01-01' AND DATE '2030-12-31'
    EACH INTERVAL '1' MONTH
);

COMMENT ON TABLE BARCLAYS_MART.MART_AML_ALERTS
    AS 'AML screening alerts with match scores and investigation tracking';


-- =============================================================================
-- MART_MONTHLY_PNL: monthly profit and loss rollup
-- =============================================================================
CREATE MULTISET TABLE BARCLAYS_MART.MART_MONTHLY_PNL, FALLBACK,
    NO BEFORE JOURNAL,
    NO AFTER JOURNAL,
    CHECKSUM = DEFAULT,
    DEFAULT MERGEBLOCKRATIO
(
    pnl_id              BIGINT           NOT NULL GENERATED ALWAYS AS IDENTITY
                        (START WITH 1 INCREMENT BY 1),
    reporting_month     DATE FORMAT 'YYYY-MM-DD'  NOT NULL,
    business_line       VARCHAR(50)      CHARACTER SET LATIN NOT CASESPECIFIC
                        COMPRESS ('RETAIL_BANKING', 'WEALTH_MANAGEMENT', 'CORPORATE_BANKING',
                                  'CARDS', 'MORTGAGES'),
    product_type        VARCHAR(50)      CHARACTER SET LATIN NOT CASESPECIFIC,
    gross_revenue       DECIMAL(18,2),
    net_interest_income DECIMAL(18,2),
    fee_income          DECIMAL(18,2),
    trading_income      DECIMAL(18,2),
    operating_expenses  DECIMAL(18,2),
    provision_charges   DECIMAL(18,2),
    net_profit          DECIMAL(18,2),
    cost_income_ratio   DECIMAL(10,4),
    return_on_equity    DECIMAL(10,4),
    /* Running cumulative sum using Teradata OLAP */
    ytd_net_profit      DECIMAL(18,2),
    /* 3-month moving average */
    ma3_net_profit      DECIMAL(18,2),
    rollup_level        VARCHAR(20)      CHARACTER SET LATIN NOT CASESPECIFIC
                        COMPRESS ('DETAIL', 'BUSINESS_LINE', 'TOTAL'),
    etl_batch_id        BIGINT,
    etl_loaded_ts       TIMESTAMP(6)     DEFAULT CURRENT_TIMESTAMP(6)
)
PRIMARY INDEX (reporting_month, business_line);

COMMENT ON TABLE BARCLAYS_MART.MART_MONTHLY_PNL
    AS 'Monthly P&L with CSUM (YTD) and MAVG (3-month MA) OLAP calculations';

/*
-- Example query using Teradata OLAP functions to populate ytd and moving avg:
--
-- SELECT
--     reporting_month,
--     business_line,
--     product_type,
--     net_profit,
--     CSUM(net_profit, reporting_month) AS ytd_net_profit,
--     MAVG(net_profit, 3, reporting_month)  AS ma3_net_profit,
--     MDIFF(net_profit, 1, reporting_month) AS mom_change
-- FROM pnl_detail
-- GROUP BY ROLLUP (business_line, product_type);
*/
