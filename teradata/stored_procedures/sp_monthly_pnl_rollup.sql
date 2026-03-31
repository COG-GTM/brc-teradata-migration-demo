/*******************************************************************************
 * sp_monthly_pnl_rollup
 *
 * Monthly P&L rollup procedure:
 *   - Aggregates daily transaction data into monthly P&L
 *   - Uses CSUM for year-to-date calculations
 *   - Uses MAVG for 3-month moving averages
 *   - GROUP BY ROLLUP for business line / product summaries
 *   - EXTRACT function for date decomposition
 *
 ******************************************************************************/

REPLACE PROCEDURE BARCLAYS_DWH.sp_monthly_pnl_rollup (
    IN p_reporting_month DATE  -- first day of month, e.g. 2025-01-01
)
BEGIN
    DECLARE v_batch_id      BIGINT;
    DECLARE v_row_count     INTEGER;
    DECLARE v_sqlstate       CHAR(5);
    DECLARE v_month_start   DATE;
    DECLARE v_month_end     DATE;
    DECLARE v_year_start    DATE;

    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        SET v_sqlstate = SQLSTATE;
        INSERT INTO BARCLAYS_DWH.ETL_ERROR_LOG (
            procedure_name, error_code, error_state, error_ts
        ) VALUES (
            'sp_monthly_pnl_rollup', SQLCODE, v_sqlstate, CURRENT_TIMESTAMP
        );
    END;

    SET v_batch_id = CAST(
        CAST(p_reporting_month AS FORMAT 'YYYYMMDD') || '005' AS BIGINT
    );

    SET v_month_start = p_reporting_month;
    SET v_month_end   = ADD_MONTHS(p_reporting_month, 1) - 1;
    SET v_year_start  = p_reporting_month - EXTRACT(DAY FROM p_reporting_month) + 1
                        - (EXTRACT(MONTH FROM p_reporting_month) - 1) * 30;
    -- Simplified: use TRUNC to Jan 1
    SET v_year_start  = CAST(EXTRACT(YEAR FROM p_reporting_month) || '-01-01' AS DATE);

    ---------------------------------------------------------------------------
    -- STEP 1: Calculate monthly revenue and cost components
    ---------------------------------------------------------------------------
    CREATE VOLATILE TABLE vt_monthly_detail AS (
        SELECT
            -- Derive business line from account type
            CASE da.account_type
                WHEN 'CURRENT'     THEN 'RETAIL_BANKING'
                WHEN 'SAVINGS'     THEN 'RETAIL_BANKING'
                WHEN 'ISA'         THEN 'WEALTH_MANAGEMENT'
                WHEN 'MORTGAGE'    THEN 'MORTGAGES'
                WHEN 'LOAN'        THEN 'RETAIL_BANKING'
                WHEN 'CREDIT_CARD' THEN 'CARDS'
                ELSE 'RETAIL_BANKING'
            END AS business_line,
            da.account_type AS product_type,

            -- Revenue components
            SUM(CASE
                WHEN ft.transaction_type = 'INTEREST' AND ft.signed_amount > 0
                THEN ft.signed_amount ELSE 0
            END) AS net_interest_income,

            SUM(CASE
                WHEN ft.transaction_type = 'FEE'
                THEN ABS(ft.signed_amount) ELSE 0
            END) AS fee_income,

            0.00 AS trading_income,  -- placeholder for demo

            -- Gross revenue
            SUM(CASE
                WHEN ft.signed_amount > 0 AND ft.transaction_type IN ('INTEREST', 'FEE')
                THEN ft.signed_amount ELSE 0
            END) AS gross_revenue,

            -- Operating expenses (simplified: 60% of gross revenue)
            SUM(CASE
                WHEN ft.signed_amount > 0 AND ft.transaction_type IN ('INTEREST', 'FEE')
                THEN ft.signed_amount ELSE 0
            END) * 0.60 AS operating_expenses,

            -- Provision charges from risk scoring
            ZEROIFNULL(risk_prov.total_el) AS provision_charges

        FROM BARCLAYS_DWH.FCT_TRANSACTION ft
        INNER JOIN BARCLAYS_DWH.DIM_ACCOUNT da
            ON ft.account_sk = da.account_sk
        INNER JOIN BARCLAYS_DWH.DIM_DATE dd
            ON ft.date_key = dd.date_key
        LEFT JOIN (
            SELECT
                cr.assessment_date,
                CASE
                    WHEN da2.account_type IN ('CURRENT', 'SAVINGS', 'LOAN') THEN 'RETAIL_BANKING'
                    WHEN da2.account_type = 'ISA' THEN 'WEALTH_MANAGEMENT'
                    WHEN da2.account_type = 'MORTGAGE' THEN 'MORTGAGES'
                    WHEN da2.account_type = 'CREDIT_CARD' THEN 'CARDS'
                    ELSE 'RETAIL_BANKING'
                END AS risk_business_line,
                SUM(cr.expected_loss) AS total_el
            FROM BARCLAYS_MART.MART_CREDIT_RISK cr
            INNER JOIN BARCLAYS_DWH.DIM_ACCOUNT da2
                ON cr.customer_id IN (
                    SELECT customer_id FROM BARCLAYS_DWH.DIM_CUSTOMER
                    WHERE customer_sk = da2.account_sk  -- simplified join
                )
            GROUP BY cr.assessment_date, 2
        ) risk_prov
            ON risk_prov.assessment_date = v_month_end
           AND risk_prov.risk_business_line = CASE da.account_type
                WHEN 'CURRENT'     THEN 'RETAIL_BANKING'
                WHEN 'SAVINGS'     THEN 'RETAIL_BANKING'
                WHEN 'ISA'         THEN 'WEALTH_MANAGEMENT'
                WHEN 'MORTGAGE'    THEN 'MORTGAGES'
                WHEN 'CREDIT_CARD' THEN 'CARDS'
                ELSE 'RETAIL_BANKING'
            END

        WHERE dd.calendar_date BETWEEN v_month_start AND v_month_end
        GROUP BY 1, 2, risk_prov.total_el
    ) WITH DATA
    ON COMMIT PRESERVE ROWS;

    ---------------------------------------------------------------------------
    -- STEP 2: Insert with GROUP BY ROLLUP and OLAP functions
    ---------------------------------------------------------------------------
    DELETE FROM BARCLAYS_MART.MART_MONTHLY_PNL
    WHERE reporting_month = p_reporting_month;

    INSERT INTO BARCLAYS_MART.MART_MONTHLY_PNL (
        reporting_month, business_line, product_type,
        gross_revenue, net_interest_income, fee_income, trading_income,
        operating_expenses, provision_charges, net_profit,
        cost_income_ratio, return_on_equity,
        ytd_net_profit, ma3_net_profit,
        rollup_level, etl_batch_id, etl_loaded_ts
    )
    SELECT
        p_reporting_month,
        COALESCE(md.business_line, 'ALL_LINES')    AS business_line,
        COALESCE(md.product_type, 'ALL_PRODUCTS')  AS product_type,
        SUM(md.gross_revenue),
        SUM(md.net_interest_income),
        SUM(md.fee_income),
        SUM(md.trading_income),
        SUM(md.operating_expenses),
        SUM(md.provision_charges),
        SUM(md.gross_revenue) - SUM(md.operating_expenses) - SUM(md.provision_charges)
            AS net_profit,
        -- Cost-to-income ratio
        CASE
            WHEN NULLIFZERO(SUM(md.gross_revenue)) IS NOT NULL
            THEN SUM(md.operating_expenses) / SUM(md.gross_revenue)
            ELSE 0
        END AS cost_income_ratio,
        -- Return on equity (simplified: net profit / assumed equity)
        CASE
            WHEN 35000000000.00 > 0
            THEN (SUM(md.gross_revenue) - SUM(md.operating_expenses) - SUM(md.provision_charges))
                 / 35000000000.00  -- GBP 35bn equity base
            ELSE 0
        END AS return_on_equity,
        -- YTD and MA3 will be calculated in a subsequent update using OLAP
        0 AS ytd_net_profit,
        0 AS ma3_net_profit,
        CASE
            WHEN GROUPING(md.business_line) = 1 THEN 'TOTAL'
            WHEN GROUPING(md.product_type) = 1  THEN 'BUSINESS_LINE'
            ELSE 'DETAIL'
        END AS rollup_level,
        v_batch_id,
        CURRENT_TIMESTAMP(6)
    FROM vt_monthly_detail md
    GROUP BY ROLLUP (md.business_line, md.product_type);

    ---------------------------------------------------------------------------
    -- STEP 3: Update YTD and moving averages using Teradata OLAP
    ---------------------------------------------------------------------------
    UPDATE BARCLAYS_MART.MART_MONTHLY_PNL
    SET ytd_net_profit = derived.ytd_val,
        ma3_net_profit = derived.ma3_val
    FROM (
        SELECT
            pnl_id,
            CSUM(net_profit, reporting_month) AS ytd_val,
            MAVG(net_profit, 3, reporting_month) AS ma3_val
        FROM BARCLAYS_MART.MART_MONTHLY_PNL
        WHERE EXTRACT(YEAR FROM reporting_month) = EXTRACT(YEAR FROM p_reporting_month)
          AND rollup_level = 'TOTAL'
    ) derived
    WHERE BARCLAYS_MART.MART_MONTHLY_PNL.pnl_id = derived.pnl_id;

    SET v_row_count = ACTIVITY_COUNT;

    -- Cleanup
    DROP TABLE vt_monthly_detail;

    COLLECT STATISTICS ON BARCLAYS_MART.MART_MONTHLY_PNL
        COLUMN (reporting_month, business_line);

END;
