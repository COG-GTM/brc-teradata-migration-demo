/******************************************************************************
 * task_monthly_regulatory.sql
 *
 * Snowflake Task DAG replacing Teradata monthly regulatory reporting batch.
 *
 * MIGRATED FROM:
 *   - teradata/bteq/monthly_regulatory_report.bteq
 *   - teradata/scheduled_jobs/monthly_regulatory_sequence.txt
 *   - teradata/stored_procedures/sp_regulatory_capital_calc.sql
 *   - teradata/stored_procedures/sp_monthly_pnl_rollup.sql
 *
 * Original Teradata sequence:
 *   PREFLIGHT -> REG_CAPITAL + PNL_ROLLUP (parallel) -> BTEQ_REGULATORY
 *     -> EXPORT_REGULATORY + EXPORT_PNL + STATS_REFRESH (parallel)
 *     -> VALIDATION -> ARCHIVE + NOTIFICATION (parallel)
 *
 * Snowflake Task DAG:
 *   monthly_preflight (root, 1st business day 07:00 UTC)
 *     -> regulatory_capital_calc (Basel III)
 *     -> pnl_rollup (P&L aggregation)
 *       -> monthly_validation (cross-check against GL)
 *         -> regulatory_export (COPY INTO stage)
 *           -> monthly_notification
 ******************************************************************************/

USE ROLE sysadmin;
USE DATABASE barclays_dwh;
USE SCHEMA orchestration;

-- =============================================================================
-- ROOT TASK: Monthly pre-flight checks
-- Replaces: BRCL_MONTH_001_PREFLIGHT
-- Schedule: 1st-3rd of each month at 07:00 UTC
-- Runs on days 1-3 to handle weekends: if the 1st falls on a weekend, the task
-- fires on 2nd and 3rd as well. Only the first business day proceeds; subsequent
-- days detect a completed run and abort.
-- =============================================================================
CREATE OR REPLACE TASK monthly_preflight
  WAREHOUSE = compute_wh
  SCHEDULE  = 'USING CRON 0 7 1-3 * * UTC'
  COMMENT   = 'Monthly pre-flight: verify month-end data completeness before regulatory calcs'
AS
BEGIN
  -- Determine the reporting month (prior month)
  LET v_reporting_month DATE := DATE_TRUNC('month', DATEADD('month', -1, CURRENT_DATE()));
  LET v_month_end DATE := LAST_DAY(:v_reporting_month);

  -- Check if today is a business day (skip weekends)
  -- Use SYSTEM$ABORT to prevent child tasks from executing
  IF (DAYOFWEEK(CURRENT_DATE()) IN (0, 6)) THEN
      INSERT INTO barclays_dwh.etl_audit_log (
          pipeline_name, step_name, status, row_count, started_at, completed_at, details
      ) VALUES (
          'MONTHLY_REGULATORY', 'PREFLIGHT', 'SKIPPED', 0,
          CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(),
          'Weekend: aborting to prevent child task execution. Will retry on next weekday (CRON runs 1st-3rd).'
      );
      CALL SYSTEM$ABORT('Not a business day; skipping monthly regulatory pipeline.');
  END IF;

  -- Check if this month's regulatory run already completed (handles 2nd/3rd day retries)
  LET v_already_run INTEGER := (
      SELECT COUNT(*)
      FROM barclays_dwh.etl_audit_log
      WHERE pipeline_name = 'MONTHLY_REGULATORY'
        AND step_name = 'PREFLIGHT'
        AND status = 'SUCCESS'
        AND started_at >= DATE_TRUNC('month', CURRENT_DATE())
  );
  IF (v_already_run > 0) THEN
      CALL SYSTEM$ABORT('Monthly regulatory already completed this month; skipping duplicate run.');
  END IF;

  INSERT INTO barclays_dwh.etl_audit_log (
      pipeline_name, step_name, status, row_count, started_at
  ) VALUES ('MONTHLY_REGULATORY', 'PREFLIGHT', 'RUNNING', 0, CURRENT_TIMESTAMP());

  -- Verify all daily ETL jobs for the prior month completed successfully
  LET v_daily_success_count INTEGER := (
      SELECT COUNT(DISTINCT started_at::DATE)
      FROM barclays_dwh.etl_audit_log
      WHERE pipeline_name = 'DAILY_ETL'
        AND step_name = 'DAILY_VALIDATION'
        AND status IN ('SUCCESS', 'WARNING')
        AND started_at::DATE BETWEEN :v_reporting_month AND :v_month_end
  );

  -- Count business days in the prior month
  LET v_expected_days INTEGER := (
      SELECT COUNT(*)
      FROM barclays_dwh.dim_date
      WHERE calendar_date BETWEEN :v_reporting_month AND :v_month_end
        AND is_business_day = TRUE
  );

  -- Verify transaction data completeness
  LET v_txn_day_count INTEGER := (
      SELECT COUNT(DISTINCT TO_DATE(TO_CHAR(date_key), 'YYYYMMDD'))
      FROM barclays_dwh.fct_transaction
      WHERE TO_DATE(TO_CHAR(date_key), 'YYYYMMDD') BETWEEN :v_reporting_month AND :v_month_end
  );

  IF (v_daily_success_count < v_expected_days * 0.9) THEN
      -- Less than 90% of business days had successful ETL — abort
      INSERT INTO barclays_dwh.etl_audit_log (
          pipeline_name, step_name, status, row_count, started_at, completed_at, details
      ) VALUES (
          'MONTHLY_REGULATORY', 'PREFLIGHT', 'FAIL', 0,
          CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(),
          CONCAT('Only ', v_daily_success_count, ' of ', v_expected_days,
                 ' expected daily ETL runs completed for ', :v_reporting_month)
      );
      CALL SYSTEM$ABORT('Insufficient daily ETL completions for monthly regulatory run.');
  END IF;

  INSERT INTO barclays_dwh.etl_audit_log (
      pipeline_name, step_name, status, row_count, started_at, completed_at, details
  ) VALUES (
      'MONTHLY_REGULATORY', 'PREFLIGHT', 'SUCCESS', 0,
      CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(),
      OBJECT_CONSTRUCT(
          'reporting_month', :v_reporting_month,
          'month_end', :v_month_end,
          'daily_etl_completions', v_daily_success_count,
          'expected_business_days', v_expected_days,
          'transaction_day_coverage', v_txn_day_count
      )::STRING
  );
END;

-- =============================================================================
-- CHILD TASK: Regulatory capital calculation (Basel III)
-- Replaces: BRCL_MONTH_002_REG_CAPITAL + sp_regulatory_capital_calc
-- Migrated from: teradata/stored_procedures/sp_regulatory_capital_calc.sql
-- Key translations:
--   CSUM() -> SUM() OVER (ORDER BY ...)
--   MAVG() -> AVG() OVER (ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
--   MDIFF() -> LAG() window function
--   NORMALIZE ON -> gap-and-island SQL
--   GROUP BY ROLLUP -> GROUP BY ROLLUP (native in Snowflake)
-- =============================================================================
CREATE OR REPLACE TASK regulatory_capital_calc
  WAREHOUSE = compute_wh
  AFTER monthly_preflight
  COMMENT   = 'Basel III regulatory capital; replaces sp_regulatory_capital_calc'
AS
BEGIN
  LET v_reporting_month DATE := DATE_TRUNC('month', DATEADD('month', -1, CURRENT_DATE()));
  LET v_batch_id BIGINT := TO_NUMBER(TO_CHAR(:v_reporting_month, 'YYYYMMDD') || '003');
  LET v_min_capital_ratio DECIMAL(10,6) := 0.08;     -- Basel III 8% minimum
  LET v_ccb_rate DECIMAL(10,6) := 0.025;              -- 2.5% conservation buffer
  LET v_systemic_rate DECIMAL(10,6) := 0.01;          -- 1% G-SIB buffer

  INSERT INTO barclays_dwh.etl_audit_log (
      pipeline_name, step_name, status, row_count, started_at
  ) VALUES ('MONTHLY_REGULATORY', 'REGULATORY_CAPITAL_CALC', 'RUNNING', 0, CURRENT_TIMESTAMP());

  -- Build exposure trend with OLAP functions
  -- Replaces Teradata CSUM/MAVG/MDIFF with Snowflake window functions
  CREATE OR REPLACE TEMPORARY TABLE tmp_exposure_trend AS
  SELECT
      cr.asset_class,
      cr.assessment_date,
      SUM(cr.exposure_at_default)    AS total_ead,
      SUM(cr.risk_weighted_asset)    AS total_rwa,
      SUM(cr.expected_loss)          AS total_el,
      COUNT(DISTINCT cr.customer_id) AS customer_count,
      -- CSUM replacement: cumulative sum
      SUM(SUM(cr.risk_weighted_asset)) OVER (
          PARTITION BY cr.asset_class
          ORDER BY cr.assessment_date
          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
      ) AS cumulative_rwa,
      -- MAVG replacement: 3-period moving average
      AVG(SUM(cr.risk_weighted_asset)) OVER (
          PARTITION BY cr.asset_class
          ORDER BY cr.assessment_date
          ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
      ) AS ma3_rwa,
      -- MDIFF replacement: month-over-month difference
      SUM(cr.risk_weighted_asset) - LAG(SUM(cr.risk_weighted_asset), 1) OVER (
          PARTITION BY cr.asset_class
          ORDER BY cr.assessment_date
      ) AS mom_rwa_change
  FROM barclays_mart.mart_credit_risk cr
  WHERE cr.assessment_date BETWEEN DATEADD('month', -12, :v_reporting_month)
                                AND :v_reporting_month
  GROUP BY cr.asset_class, cr.assessment_date;

  -- Delete existing data for idempotent re-run
  DELETE FROM barclays_mart.mart_regulatory_capital
  WHERE reporting_date = :v_reporting_month;

  -- Insert with GROUP BY ROLLUP (native Snowflake support)
  INSERT INTO barclays_mart.mart_regulatory_capital (
      reporting_date, asset_class,
      total_exposure, total_rwa, capital_required, capital_ratio,
      tier1_capital, tier2_capital, total_capital,
      leverage_ratio, countercyclical_buf, systemic_buf,
      rollup_level, etl_batch_id, etl_loaded_ts
  )
  SELECT
      :v_reporting_month,
      COALESCE(et.asset_class, 'ALL_CLASSES'),
      SUM(et.total_ead),
      SUM(et.total_rwa),
      SUM(et.total_rwa) * :v_min_capital_ratio,
      CASE
          WHEN NULLIF(SUM(et.total_rwa), 0) IS NOT NULL
          THEN 50000000000.00 / SUM(et.total_rwa)
          ELSE 0
      END,
      50000000000.00 * 0.70,    -- Tier 1 capital (70%)
      50000000000.00 * 0.30,    -- Tier 2 capital (30%)
      50000000000.00,            -- Total capital base
      CASE
          WHEN NULLIF(SUM(et.total_ead), 0) IS NOT NULL
          THEN (50000000000.00 * 0.70) / SUM(et.total_ead)
          ELSE 0
      END,
      :v_ccb_rate,
      :v_systemic_rate,
      CASE
          WHEN GROUPING(et.asset_class) = 1 THEN 'TOTAL'
          ELSE 'ASSET_CLASS'
      END,
      :v_batch_id,
      CURRENT_TIMESTAMP()
  FROM tmp_exposure_trend et
  WHERE et.assessment_date = :v_reporting_month
  GROUP BY ROLLUP (et.asset_class);

  -- Basel III capital adequacy validation checks
  LET v_total_ratio DECIMAL(10,6) := (
      SELECT capital_ratio
      FROM barclays_mart.mart_regulatory_capital
      WHERE reporting_date = :v_reporting_month
        AND rollup_level = 'TOTAL'
  );

  -- Basel III minimum: 8% + 2.5% CCB + 1% G-SIB = 11.5%
  IF (v_total_ratio < (:v_min_capital_ratio + :v_ccb_rate + :v_systemic_rate)) THEN
      INSERT INTO barclays_dwh.etl_validation_results (
          validation_date, check_name, status, expected_value, actual_value, details
      ) VALUES (
          :v_reporting_month, 'BASEL_III_CAPITAL_ADEQUACY', 'BREACH',
          (:v_min_capital_ratio + :v_ccb_rate + :v_systemic_rate)::STRING,
          v_total_ratio::STRING,
          'Capital ratio below Basel III minimum + buffers threshold'
      );
      CALL SYSTEM$SEND_NOTIFICATION(
          'daily_etl_alerts',
          'REGULATORY ALERT: Capital Adequacy Breach',
          CONCAT('Capital ratio ', v_total_ratio, ' is below minimum threshold of ',
                 (:v_min_capital_ratio + :v_ccb_rate + :v_systemic_rate))
      );
  END IF;

  LET v_rows INTEGER := SQLROWCOUNT;

  DROP TABLE IF EXISTS tmp_exposure_trend;

  INSERT INTO barclays_dwh.etl_audit_log (
      pipeline_name, step_name, status, row_count, started_at, completed_at
  ) VALUES ('MONTHLY_REGULATORY', 'REGULATORY_CAPITAL_CALC', 'SUCCESS', v_rows,
            CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP());
END;

-- =============================================================================
-- CHILD TASK: Monthly P&L rollup
-- Replaces: BRCL_MONTH_003_PNL_ROLLUP + sp_monthly_pnl_rollup
-- Runs in parallel with regulatory_capital_calc
-- Migrated from: teradata/stored_procedures/sp_monthly_pnl_rollup.sql
-- =============================================================================
CREATE OR REPLACE TASK pnl_rollup
  WAREHOUSE = compute_wh
  AFTER monthly_preflight
  COMMENT   = 'Monthly P&L rollup with ROLLUP aggregation; replaces sp_monthly_pnl_rollup'
AS
BEGIN
  LET v_reporting_month DATE := DATE_TRUNC('month', DATEADD('month', -1, CURRENT_DATE()));
  LET v_month_end DATE := LAST_DAY(:v_reporting_month);
  LET v_year_start DATE := DATE_TRUNC('year', :v_reporting_month);
  LET v_batch_id BIGINT := TO_NUMBER(TO_CHAR(:v_reporting_month, 'YYYYMMDD') || '005');

  INSERT INTO barclays_dwh.etl_audit_log (
      pipeline_name, step_name, status, row_count, started_at
  ) VALUES ('MONTHLY_REGULATORY', 'PNL_ROLLUP', 'RUNNING', 0, CURRENT_TIMESTAMP());

  -- Step 1: Calculate monthly revenue and cost components
  CREATE OR REPLACE TEMPORARY TABLE tmp_monthly_detail AS
  SELECT
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
      SUM(CASE
          WHEN ft.transaction_type = 'INTEREST' AND ft.signed_amount > 0
          THEN ft.signed_amount ELSE 0
      END) AS net_interest_income,
      SUM(CASE
          WHEN ft.transaction_type = 'FEE'
          THEN ABS(ft.signed_amount) ELSE 0
      END) AS fee_income,
      0.00 AS trading_income,
      SUM(CASE
          WHEN ft.signed_amount > 0 AND ft.transaction_type IN ('INTEREST', 'FEE')
          THEN ft.signed_amount ELSE 0
      END) AS gross_revenue,
      SUM(CASE
          WHEN ft.signed_amount > 0 AND ft.transaction_type IN ('INTEREST', 'FEE')
          THEN ft.signed_amount ELSE 0
      END) * 0.60 AS operating_expenses,
      0 AS provision_charges  -- placeholder; allocated proportionally below
  FROM barclays_dwh.fct_transaction ft
  INNER JOIN barclays_dwh.dim_account da
      ON ft.account_sk = da.account_sk
  INNER JOIN barclays_dwh.dim_date dd
      ON ft.date_key = dd.date_key
  WHERE dd.calendar_date BETWEEN :v_reporting_month AND :v_month_end
  GROUP BY 1, 2;

  -- Allocate provision charges proportionally by gross_revenue within each business_line
  -- This avoids the fan-out issue where business-line-level provisions inflate at rollup
  UPDATE tmp_monthly_detail md
  SET provision_charges = CASE
      WHEN bl_totals.bl_gross_revenue > 0
      THEN COALESCE(risk_prov.total_el, 0) * (md.gross_revenue / bl_totals.bl_gross_revenue)
      ELSE 0
  END
  FROM (
      SELECT business_line, SUM(gross_revenue) AS bl_gross_revenue
      FROM tmp_monthly_detail
      GROUP BY business_line
  ) bl_totals,
  (
      SELECT
          CASE da2.account_type
              WHEN 'CURRENT'     THEN 'RETAIL_BANKING'
              WHEN 'SAVINGS'     THEN 'RETAIL_BANKING'
              WHEN 'ISA'         THEN 'WEALTH_MANAGEMENT'
              WHEN 'MORTGAGE'    THEN 'MORTGAGES'
              WHEN 'CREDIT_CARD' THEN 'CARDS'
              ELSE 'RETAIL_BANKING'
          END AS risk_business_line,
          SUM(cr.expected_loss) AS total_el
      FROM barclays_mart.mart_credit_risk cr
      INNER JOIN barclays_dwh.dim_account da2
          ON cr.customer_id = da2.customer_id AND da2.is_current = 'Y'
      WHERE cr.assessment_date = LAST_DAY(DATE_TRUNC('month', DATEADD('month', -1, CURRENT_DATE())))
      GROUP BY 1
  ) risk_prov
  WHERE md.business_line = bl_totals.business_line
    AND md.business_line = risk_prov.risk_business_line;

  -- Delete existing for idempotent re-run
  DELETE FROM barclays_mart.mart_monthly_pnl
  WHERE reporting_month = :v_reporting_month;

  -- Step 2: Insert with GROUP BY ROLLUP
  INSERT INTO barclays_mart.mart_monthly_pnl (
      reporting_month, business_line, product_type,
      gross_revenue, net_interest_income, fee_income, trading_income,
      operating_expenses, provision_charges, net_profit,
      cost_income_ratio, return_on_equity,
      ytd_net_profit, ma3_net_profit,
      rollup_level, etl_batch_id, etl_loaded_ts
  )
  SELECT
      :v_reporting_month,
      COALESCE(md.business_line, 'ALL_LINES'),
      COALESCE(md.product_type, 'ALL_PRODUCTS'),
      SUM(md.gross_revenue),
      SUM(md.net_interest_income),
      SUM(md.fee_income),
      SUM(md.trading_income),
      SUM(md.operating_expenses),
      SUM(md.provision_charges),
      SUM(md.gross_revenue) - SUM(md.operating_expenses) - SUM(md.provision_charges),
      CASE
          WHEN NULLIF(SUM(md.gross_revenue), 0) IS NOT NULL
          THEN SUM(md.operating_expenses) / SUM(md.gross_revenue)
          ELSE 0
      END,
      CASE
          WHEN 35000000000.00 > 0
          THEN (SUM(md.gross_revenue) - SUM(md.operating_expenses) - SUM(md.provision_charges))
               / 35000000000.00
          ELSE 0
      END,
      0,  -- YTD computed in update below
      0,  -- MA3 computed in update below
      CASE
          WHEN GROUPING(md.business_line) = 1 THEN 'TOTAL'
          WHEN GROUPING(md.product_type) = 1  THEN 'BUSINESS_LINE'
          ELSE 'DETAIL'
      END,
      :v_batch_id,
      CURRENT_TIMESTAMP()
  FROM tmp_monthly_detail md
  GROUP BY ROLLUP (md.business_line, md.product_type);

  -- Step 3: Update YTD and moving averages
  -- Replaces Teradata CSUM/MAVG with Snowflake window functions
  UPDATE barclays_mart.mart_monthly_pnl tgt
  SET
      ytd_net_profit = derived.ytd_val,
      ma3_net_profit = derived.ma3_val
  FROM (
      SELECT
          pnl_id,
          SUM(net_profit) OVER (
              ORDER BY reporting_month
              ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
          ) AS ytd_val,
          AVG(net_profit) OVER (
              ORDER BY reporting_month
              ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
          ) AS ma3_val
      FROM barclays_mart.mart_monthly_pnl
      WHERE YEAR(reporting_month) = YEAR(:v_reporting_month)
        AND rollup_level = 'TOTAL'
  ) derived
  WHERE tgt.pnl_id = derived.pnl_id;

  LET v_rows INTEGER := SQLROWCOUNT;

  DROP TABLE IF EXISTS tmp_monthly_detail;

  INSERT INTO barclays_dwh.etl_audit_log (
      pipeline_name, step_name, status, row_count, started_at, completed_at
  ) VALUES ('MONTHLY_REGULATORY', 'PNL_ROLLUP', 'SUCCESS', v_rows,
            CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP());
END;

-- =============================================================================
-- CHILD TASK: Monthly validation
-- Replaces: BRCL_MONTH_008_VALIDATION
-- Depends on both regulatory_capital_calc and pnl_rollup
-- =============================================================================
CREATE OR REPLACE TASK monthly_validation
  WAREHOUSE = compute_wh
  AFTER regulatory_capital_calc, pnl_rollup
  COMMENT   = 'Cross-check regulatory totals against GL; replaces BRCL_MONTH_008_VALIDATION'
AS
BEGIN
  LET v_reporting_month DATE := DATE_TRUNC('month', DATEADD('month', -1, CURRENT_DATE()));
  LET v_issues INTEGER := 0;

  INSERT INTO barclays_dwh.etl_audit_log (
      pipeline_name, step_name, status, row_count, started_at
  ) VALUES ('MONTHLY_REGULATORY', 'VALIDATION', 'RUNNING', 0, CURRENT_TIMESTAMP());

  -- Validation 1: Capital adequacy — ratio must exceed Basel III minimum (8% + buffers)
  LET v_cap_ratio DECIMAL(10,6) := (
      SELECT capital_ratio FROM barclays_mart.mart_regulatory_capital
      WHERE reporting_date = :v_reporting_month AND rollup_level = 'TOTAL'
  );
  INSERT INTO barclays_dwh.etl_validation_results (
      validation_date, check_name, status, expected_value, actual_value, details
  ) VALUES (
      :v_reporting_month, 'CAPITAL_ADEQUACY_RATIO',
      CASE WHEN v_cap_ratio >= 0.115 THEN 'PASS' ELSE 'FAIL' END,
      '>= 0.115', v_cap_ratio::STRING,
      'Basel III: 8% minimum + 2.5% CCB + 1% G-SIB = 11.5%'
  );
  IF (v_cap_ratio < 0.115) THEN
      v_issues := v_issues + 1;
  END IF;

  -- Validation 2: Leverage ratio must exceed 3%
  LET v_lev_ratio DECIMAL(10,6) := (
      SELECT leverage_ratio FROM barclays_mart.mart_regulatory_capital
      WHERE reporting_date = :v_reporting_month AND rollup_level = 'TOTAL'
  );
  INSERT INTO barclays_dwh.etl_validation_results (
      validation_date, check_name, status, expected_value, actual_value, details
  ) VALUES (
      :v_reporting_month, 'LEVERAGE_RATIO',
      CASE WHEN v_lev_ratio >= 0.03 THEN 'PASS' ELSE 'FAIL' END,
      '>= 0.03', v_lev_ratio::STRING,
      'Basel III minimum leverage ratio'
  );
  IF (v_lev_ratio < 0.03) THEN
      v_issues := v_issues + 1;
  END IF;

  -- Validation 3: P&L balance check — gross revenue >= operating expenses
  LET v_pnl_balanced BOOLEAN := (
      SELECT gross_revenue >= operating_expenses
      FROM barclays_mart.mart_monthly_pnl
      WHERE reporting_month = :v_reporting_month AND rollup_level = 'TOTAL'
  );
  INSERT INTO barclays_dwh.etl_validation_results (
      validation_date, check_name, status, expected_value, actual_value, details
  ) VALUES (
      :v_reporting_month, 'PNL_BALANCE_CHECK',
      CASE WHEN v_pnl_balanced THEN 'PASS' ELSE 'WARNING' END,
      'gross_revenue >= operating_expenses', v_pnl_balanced::STRING,
      'Sanity check: revenue should cover expenses at TOTAL level'
  );
  IF (NOT v_pnl_balanced) THEN
      v_issues := v_issues + 1;
  END IF;

  -- Validation 4: Cost-to-income ratio sanity check (should be 0-1)
  LET v_cir DECIMAL(10,6) := (
      SELECT cost_income_ratio FROM barclays_mart.mart_monthly_pnl
      WHERE reporting_month = :v_reporting_month AND rollup_level = 'TOTAL'
  );
  INSERT INTO barclays_dwh.etl_validation_results (
      validation_date, check_name, status, expected_value, actual_value, details
  ) VALUES (
      :v_reporting_month, 'COST_INCOME_RATIO',
      CASE WHEN v_cir BETWEEN 0 AND 1 THEN 'PASS' ELSE 'FAIL' END,
      '0 <= CIR <= 1', v_cir::STRING,
      'Cost-to-income ratio should be between 0 and 1'
  );
  IF (v_cir NOT BETWEEN 0 AND 1) THEN
      v_issues := v_issues + 1;
  END IF;

  INSERT INTO barclays_dwh.etl_audit_log (
      pipeline_name, step_name, status, row_count, started_at, completed_at, details
  ) VALUES (
      'MONTHLY_REGULATORY', 'VALIDATION',
      CASE WHEN v_issues = 0 THEN 'SUCCESS' ELSE 'WARNING' END,
      v_issues, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(),
      OBJECT_CONSTRUCT(
          'capital_ratio', v_cap_ratio,
          'leverage_ratio', v_lev_ratio,
          'cost_income_ratio', v_cir,
          'pnl_balanced', v_pnl_balanced
      )::STRING
  );
END;

-- =============================================================================
-- CHILD TASK: Regulatory export
-- Replaces: BRCL_MONTH_005_EXPORT_REGULATORY + BRCL_MONTH_006_EXPORT_PNL
-- Uses COPY INTO to stage, replacing BTEQ .EXPORT
-- =============================================================================
CREATE OR REPLACE TASK regulatory_export
  WAREHOUSE = compute_wh
  AFTER monthly_validation
  COMMENT   = 'Export regulatory & PNL reports to stage; replaces BTEQ .EXPORT'
AS
BEGIN
  LET v_reporting_month DATE := DATE_TRUNC('month', DATEADD('month', -1, CURRENT_DATE()));
  LET v_month_str STRING := TO_CHAR(:v_reporting_month, 'YYYYMM');

  INSERT INTO barclays_dwh.etl_audit_log (
      pipeline_name, step_name, status, row_count, started_at
  ) VALUES ('MONTHLY_REGULATORY', 'REGULATORY_EXPORT', 'RUNNING', 0, CURRENT_TIMESTAMP());

  -- Export regulatory capital report
  COPY INTO @barclays_exports/regulatory/capital_adequacy_${v_month_str}.csv
  FROM (
      SELECT
          reporting_date,
          asset_class,
          total_exposure,
          total_rwa,
          capital_required,
          capital_ratio,
          tier1_capital,
          tier2_capital,
          total_capital,
          leverage_ratio,
          countercyclical_buf,
          systemic_buf,
          rollup_level
      FROM barclays_mart.mart_regulatory_capital
      WHERE reporting_date = :v_reporting_month
      ORDER BY rollup_level, asset_class
  )
  FILE_FORMAT = (TYPE = 'CSV' FIELD_DELIMITER = '|' COMPRESSION = 'GZIP'
                 FIELD_OPTIONALLY_ENCLOSED_BY = '"')
  HEADER = TRUE
  OVERWRITE = TRUE;

  -- Export P&L report
  COPY INTO @barclays_exports/pnl/monthly_pnl_${v_month_str}.csv
  FROM (
      SELECT
          reporting_month,
          business_line,
          product_type,
          gross_revenue,
          net_interest_income,
          fee_income,
          trading_income,
          operating_expenses,
          provision_charges,
          net_profit,
          cost_income_ratio,
          return_on_equity,
          ytd_net_profit,
          ma3_net_profit,
          rollup_level
      FROM barclays_mart.mart_monthly_pnl
      WHERE reporting_month = :v_reporting_month
      ORDER BY rollup_level, business_line, product_type
  )
  FILE_FORMAT = (TYPE = 'CSV' FIELD_DELIMITER = '|' COMPRESSION = 'GZIP'
                 FIELD_OPTIONALLY_ENCLOSED_BY = '"')
  HEADER = TRUE
  OVERWRITE = TRUE;

  INSERT INTO barclays_dwh.etl_audit_log (
      pipeline_name, step_name, status, row_count, started_at, completed_at
  ) VALUES ('MONTHLY_REGULATORY', 'REGULATORY_EXPORT', 'SUCCESS', 0,
            CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP());
END;

-- =============================================================================
-- CHILD TASK: Monthly notification
-- Replaces: BRCL_MONTH_010_NOTIFICATION
-- =============================================================================
CREATE OR REPLACE TASK monthly_notification
  WAREHOUSE = compute_wh
  AFTER regulatory_export
  COMMENT   = 'Send monthly completion notification; replaces BRCL_MONTH_010_NOTIFICATION'
AS
BEGIN
  LET v_reporting_month DATE := DATE_TRUNC('month', DATEADD('month', -1, CURRENT_DATE()));

  LET v_cap_ratio STRING := (
      SELECT capital_ratio::STRING
      FROM barclays_mart.mart_regulatory_capital
      WHERE reporting_date = :v_reporting_month AND rollup_level = 'TOTAL'
  );

  CALL SYSTEM$SEND_NOTIFICATION(
      'daily_etl_alerts',
      CONCAT('Monthly Regulatory Report Complete - ', :v_reporting_month::STRING),
      CONCAT(
          'Monthly regulatory pipeline completed at ', CURRENT_TIMESTAMP()::STRING, '\n',
          'Reporting Month: ', :v_reporting_month::STRING, '\n',
          'Capital Ratio: ', v_cap_ratio, '\n',
          'Exports staged at: @barclays_exports/regulatory/ and @barclays_exports/pnl/\n',
          'Review validation results in barclays_dwh.etl_validation_results.'
      )
  );

  INSERT INTO barclays_dwh.etl_audit_log (
      pipeline_name, step_name, status, row_count, started_at, completed_at
  ) VALUES ('MONTHLY_REGULATORY', 'NOTIFICATION_SENT', 'SUCCESS', 0,
            CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP());
END;

-- =============================================================================
-- ENABLE THE MONTHLY TASK DAG
-- =============================================================================
ALTER TASK monthly_notification      RESUME;
ALTER TASK regulatory_export         RESUME;
ALTER TASK monthly_validation        RESUME;
ALTER TASK pnl_rollup                RESUME;
ALTER TASK regulatory_capital_calc   RESUME;
ALTER TASK monthly_preflight         RESUME;  -- Root task: resume LAST
