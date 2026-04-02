/******************************************************************************
 * task_daily_etl_pipeline.sql
 *
 * Snowflake Task DAG replacing Teradata daily batch orchestration.
 *
 * MIGRATED FROM:
 *   - teradata/bteq/daily_batch_load.bteq
 *   - teradata/scheduled_jobs/daily_etl_sequence.txt
 *
 * Original Teradata sequence:
 *   PREFLIGHT -> FASTLOAD_MKTDATA + TPT_TRANSACTIONS (parallel)
 *     -> MLOAD_BALANCES -> BTEQ_MAIN (sp_daily_transaction_load,
 *        sp_customer_risk_scoring, sp_aml_screening)
 *     -> EXPORT_CUSTOMERS + STATS_COLLECTION (parallel)
 *     -> VALIDATION -> NOTIFICATION
 *
 * Snowflake Task DAG:
 *   raw_data_validation (root, 06:00 UTC daily)
 *     -> market_data_load (parallel with transaction_ingest via Snowpipe)
 *     -> daily_transaction_load (SCD2 + fact load)
 *       -> customer_risk_scoring (Basel III PD/LGD/EAD)
 *       -> aml_screening (sanctions, structuring, velocity)
 *         -> daily_validation (post-load checks)
 *           -> daily_notification (alert on success/failure)
 *
 * NOTE: Snowpipe handles raw data ingestion (see snowpipe/ directory),
 *       replacing FastLoad/TPT/MultiLoad. Streams detect new data arrival.
 ******************************************************************************/

USE ROLE sysadmin;
USE DATABASE barclays_dwh;
USE SCHEMA orchestration;

-- =============================================================================
-- NOTIFICATION INTEGRATION (for error alerting)
-- =============================================================================
CREATE OR REPLACE NOTIFICATION INTEGRATION daily_etl_alerts
  TYPE = QUEUE
  ENABLED = TRUE
  NOTIFICATION_PROVIDER = AWS_SNS
  DIRECTION = OUTBOUND
  AWS_SNS_TOPIC_ARN = 'arn:aws:sns:eu-west-2:123456789012:barclays-etl-alerts'
  AWS_SNS_ROLE_ARN  = 'arn:aws:iam::123456789012:role/snowflake-sns-role';

-- =============================================================================
-- ALERT: Monitor for task failures
-- =============================================================================
CREATE OR REPLACE ALERT daily_etl_failure_alert
  WAREHOUSE = compute_wh
  SCHEDULE  = '5 MINUTE'
  IF (EXISTS (
      SELECT *
      FROM TABLE(information_schema.task_history(
          scheduled_time_range_start => DATEADD('hour', -1, CURRENT_TIMESTAMP()),
          task_name                  => 'RAW_DATA_VALIDATION'
      ))
      WHERE state = 'FAILED'
        AND scheduled_time > DATEADD('minute', -10, CURRENT_TIMESTAMP())
  ))
  THEN
      CALL SYSTEM$SEND_NOTIFICATION(
          'daily_etl_alerts',
          'Daily ETL Pipeline FAILED',
          CONCAT('Daily ETL pipeline failed at ', CURRENT_TIMESTAMP()::STRING,
                 '. Check TASK_HISTORY for details.')
      );

-- =============================================================================
-- ROOT TASK: Pre-flight data validation
-- Replaces: BRCL_DAILY_001_PREFLIGHT
-- Original: Pre-flight checks in daily_batch_load.bteq (lines 18-29)
-- Schedule: Daily at 06:00 UTC (was 06:00 GMT in UC4/AutoSys)
-- =============================================================================
CREATE OR REPLACE TASK raw_data_validation
  WAREHOUSE = compute_wh
  SCHEDULE  = 'USING CRON 0 6 * * * UTC'
  COMMENT   = 'Pre-flight: verify source data exists for today before ETL begins'
  -- Only run if new data has arrived via Snowpipe
  WHEN SYSTEM$STREAM_HAS_DATA('barclays_raw.raw_transaction_stream')
    OR SYSTEM$STREAM_HAS_DATA('barclays_raw.raw_market_data_stream')
AS
BEGIN
  -- Log ETL start
  INSERT INTO barclays_dwh.etl_audit_log (
      pipeline_name, step_name, status, row_count, started_at
  )
  VALUES ('DAILY_ETL', 'RAW_DATA_VALIDATION', 'RUNNING', 0, CURRENT_TIMESTAMP());

  -- Pre-flight: verify raw transaction data exists for today
  LET v_raw_txn_count INTEGER := (
      SELECT COUNT(*)
      FROM barclays_raw.transaction
      WHERE transaction_date = CURRENT_DATE()
  );

  -- Pre-flight: verify market data exists for today
  LET v_mkt_count INTEGER := (
      SELECT COUNT(*)
      FROM barclays_raw.market_data
      WHERE valuation_date = CURRENT_DATE()
  );

  -- Log validation results
  INSERT INTO barclays_dwh.etl_audit_log (
      pipeline_name, step_name, status, row_count, started_at, completed_at, details
  )
  VALUES (
      'DAILY_ETL', 'RAW_DATA_VALIDATION',
      CASE WHEN v_raw_txn_count > 0 THEN 'SUCCESS' ELSE 'WARNING_NO_DATA' END,
      v_raw_txn_count,
      CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(),
      OBJECT_CONSTRUCT(
          'raw_txn_count', v_raw_txn_count,
          'market_data_count', v_mkt_count,
          'validation_date', CURRENT_DATE()
      )::STRING
  );

  -- Fail the task if no data at all (prevents downstream tasks from running)
  IF (v_raw_txn_count = 0 AND v_mkt_count = 0) THEN
      CALL SYSTEM$SEND_NOTIFICATION(
          'daily_etl_alerts',
          'Daily ETL: No Source Data',
          CONCAT('No raw transactions or market data found for ', CURRENT_DATE()::STRING)
      );
      -- Raise an error to prevent child tasks from executing
      CALL SYSTEM$ABORT('No source data available for today. ETL skipped.');
  END IF;
END;

-- =============================================================================
-- CHILD TASK: Market data load
-- Replaces: BRCL_DAILY_002_FASTLOAD_MKTDATA (market_data_load.fl)
-- Processes incremental market data from stream
-- =============================================================================
CREATE OR REPLACE TASK market_data_load
  WAREHOUSE = compute_wh
  AFTER raw_data_validation
  COMMENT   = 'Process market data from stream; replaces FastLoad market_data_load.fl'
AS
BEGIN
  INSERT INTO barclays_dwh.etl_audit_log (
      pipeline_name, step_name, status, row_count, started_at
  )
  VALUES ('DAILY_ETL', 'MARKET_DATA_LOAD', 'RUNNING', 0, CURRENT_TIMESTAMP());

  -- Merge new/changed market data from stream into warehouse
  MERGE INTO barclays_dwh.dim_market_data tgt
  USING (
      SELECT
          instrument_id,
          valuation_date,
          instrument_type,
          instrument_name,
          currency,
          mid_price,
          bid_price,
          ask_price,
          source_system,
          CURRENT_TIMESTAMP() AS etl_loaded_ts
      FROM barclays_raw.raw_market_data_stream
      WHERE METADATA$ACTION = 'INSERT'
  ) src
  ON tgt.instrument_id = src.instrument_id
     AND tgt.valuation_date = src.valuation_date
  WHEN MATCHED THEN UPDATE SET
      tgt.mid_price      = src.mid_price,
      tgt.bid_price      = src.bid_price,
      tgt.ask_price      = src.ask_price,
      tgt.source_system  = src.source_system,
      tgt.etl_loaded_ts  = src.etl_loaded_ts
  WHEN NOT MATCHED THEN INSERT (
      instrument_id, valuation_date, instrument_type, instrument_name,
      currency, mid_price, bid_price, ask_price, source_system, etl_loaded_ts
  ) VALUES (
      src.instrument_id, src.valuation_date, src.instrument_type, src.instrument_name,
      src.currency, src.mid_price, src.bid_price, src.ask_price, src.source_system, src.etl_loaded_ts
  );

  LET v_rows_merged INTEGER := SQLROWCOUNT;

  INSERT INTO barclays_dwh.etl_audit_log (
      pipeline_name, step_name, status, row_count, started_at, completed_at
  )
  VALUES ('DAILY_ETL', 'MARKET_DATA_LOAD', 'SUCCESS', v_rows_merged,
          CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP());
END;

-- =============================================================================
-- CHILD TASK: Daily transaction load
-- Replaces: BRCL_DAILY_005_BTEQ_MAIN -> sp_daily_transaction_load
-- Original: SCD Type 2 customer dimension + fact load + balance snapshots
-- Migrated from: teradata/stored_procedures/sp_daily_transaction_load.sql
-- =============================================================================
CREATE OR REPLACE TASK daily_transaction_load
  WAREHOUSE = compute_wh
  AFTER raw_data_validation
  COMMENT   = 'SCD2 customer dim + transaction facts + balance snapshots; replaces sp_daily_transaction_load'
AS
BEGIN
  INSERT INTO barclays_dwh.etl_audit_log (
      pipeline_name, step_name, status, row_count, started_at
  )
  VALUES ('DAILY_ETL', 'DAILY_TRANSACTION_LOAD', 'RUNNING', 0, CURRENT_TIMESTAMP());

  -- Execute via dbt: dbt run --select tag:daily_transactions
  -- This runs the following dbt models in dependency order:
  --   stg_customers -> int_customer_scd2 -> dim_customer
  --   stg_accounts  -> dim_account (MERGE upsert)
  --   stg_transactions -> int_transaction_enriched -> fct_transaction
  --   fct_daily_balance (MERGE upsert with running balances)
  EXECUTE IMMEDIATE $$
    CALL barclays_dwh.procedures.run_dbt_selector('tag:daily_transactions');
  $$;

  -- Alternatively, if not using dbt, the migrated SQL logic runs directly:
  -- Step 1: SCD Type 2 for customer dimension
  -- (Replaces cursor-based SCD2 from sp_daily_transaction_load lines 55-96)
  MERGE INTO barclays_dwh.dim_customer tgt
  USING barclays_stg.v_customer_latest src
  ON tgt.customer_id = src.customer_id AND tgt.is_current = 'Y'
  WHEN MATCHED AND (
      src.risk_rating <> tgt.risk_rating
      OR src.kyc_status <> tgt.kyc_status
      OR src.segment <> tgt.segment
  ) THEN UPDATE SET
      is_current    = 'N',
      effective_to  = DATEADD('day', -1, CURRENT_DATE()),
      etl_loaded_ts = CURRENT_TIMESTAMP()
  ;

  -- Insert new versions for changed customers
  INSERT INTO barclays_dwh.dim_customer (
      customer_id, first_name, last_name, date_of_birth,
      nationality, kyc_status, risk_rating, segment,
      postcode, country, is_current, effective_from, effective_to,
      etl_batch_id, etl_loaded_ts
  )
  SELECT
      src.customer_id, src.first_name, src.last_name, src.date_of_birth,
      src.nationality, src.kyc_status, src.risk_rating, src.segment,
      src.postcode, src.country, 'Y', CURRENT_DATE(), '9999-12-31'::DATE,
      TO_NUMBER(TO_CHAR(CURRENT_DATE(), 'YYYYMMDD') || '001'),
      CURRENT_TIMESTAMP()
  FROM barclays_stg.v_customer_latest src
  LEFT JOIN barclays_dwh.dim_customer tgt
      ON src.customer_id = tgt.customer_id AND tgt.is_current = 'Y'
  WHERE tgt.customer_id IS NULL
     OR (tgt.is_current = 'N' AND tgt.effective_to = DATEADD('day', -1, CURRENT_DATE()));

  -- Step 2: Upsert accounts (replaces MERGE in sp_daily_transaction_load lines 123-143)
  MERGE INTO barclays_dwh.dim_account tgt
  USING barclays_stg.v_account_current src
  ON tgt.account_id = src.account_id AND tgt.is_current = 'Y'
  WHEN MATCHED THEN UPDATE SET
      status          = src.status,
      close_date      = src.close_date,
      credit_limit    = COALESCE(src.credit_limit, 0),
      overdraft_limit = COALESCE(src.overdraft_limit, 0),
      etl_loaded_ts   = CURRENT_TIMESTAMP()
  WHEN NOT MATCHED THEN INSERT (
      account_id, customer_id, account_type, currency,
      branch_code, sort_code, status, open_date, close_date,
      credit_limit, overdraft_limit, is_current, etl_loaded_ts
  ) VALUES (
      src.account_id, src.customer_id, src.account_type, src.currency,
      src.branch_code, src.sort_code, src.status, src.open_date, src.close_date,
      COALESCE(src.credit_limit, 0), COALESCE(src.overdraft_limit, 0), 'Y',
      CURRENT_TIMESTAMP()
  );

  -- Step 3: Load transactions into fact table
  INSERT INTO barclays_dwh.fct_transaction (
      transaction_id, account_sk, customer_sk, date_key,
      counterparty_id, transaction_type, channel,
      amount, signed_amount, currency, value_band,
      balance_after, description, reference_number,
      etl_batch_id, etl_loaded_ts
  )
  SELECT
      t.transaction_id,
      da.account_sk,
      dc.customer_sk,
      TO_NUMBER(TO_CHAR(t.transaction_date, 'YYYYMMDD')) AS date_key,
      t.counterparty_id,
      t.transaction_type,
      t.channel,
      t.amount,
      t.signed_amount,
      t.currency,
      t.value_band,
      t.balance_after,
      t.description,
      t.reference_number,
      TO_NUMBER(TO_CHAR(CURRENT_DATE(), 'YYYYMMDD') || '001'),
      CURRENT_TIMESTAMP()
  FROM barclays_stg.v_transaction_enriched t
  INNER JOIN barclays_dwh.dim_account da
      ON t.account_id = da.account_id AND da.is_current = 'Y'
  INNER JOIN barclays_dwh.dim_customer dc
      ON da.customer_id = dc.customer_id AND dc.is_current = 'Y'
  WHERE t.transaction_date = CURRENT_DATE()
    AND NOT EXISTS (
        SELECT 1 FROM barclays_dwh.fct_transaction f
        WHERE f.transaction_id = t.transaction_id
    );

  -- Step 4: Update daily balance snapshots
  MERGE INTO barclays_dwh.fct_daily_balance tgt
  USING (
      SELECT
          da.account_sk,
          TO_NUMBER(TO_CHAR(CURRENT_DATE(), 'YYYYMMDD')) AS date_key,
          COALESCE(prev.closing_balance, 0) AS opening_balance,
          COALESCE(prev.closing_balance, 0) + COALESCE(txn.net_amount, 0) AS closing_balance,
          COALESCE(txn.total_debits, 0)  AS total_debits,
          COALESCE(txn.total_credits, 0) AS total_credits,
          COALESCE(txn.txn_count, 0)     AS transaction_count,
          da.currency
      FROM barclays_dwh.dim_account da
      LEFT JOIN barclays_dwh.fct_daily_balance prev
          ON da.account_sk = prev.account_sk
         AND prev.date_key = TO_NUMBER(TO_CHAR(DATEADD('day', -1, CURRENT_DATE()), 'YYYYMMDD'))
      LEFT JOIN (
          SELECT
              account_sk,
              SUM(CASE WHEN signed_amount < 0 THEN ABS(signed_amount) ELSE 0 END) AS total_debits,
              SUM(CASE WHEN signed_amount > 0 THEN signed_amount ELSE 0 END)      AS total_credits,
              SUM(signed_amount) AS net_amount,
              COUNT(*)           AS txn_count
          FROM barclays_dwh.fct_transaction
          WHERE date_key = TO_NUMBER(TO_CHAR(CURRENT_DATE(), 'YYYYMMDD'))
          GROUP BY account_sk
      ) txn ON da.account_sk = txn.account_sk
      WHERE da.is_current = 'Y'
  ) src
  ON tgt.account_sk = src.account_sk AND tgt.date_key = src.date_key
  WHEN MATCHED THEN UPDATE SET
      opening_balance   = src.opening_balance,
      closing_balance   = src.closing_balance,
      total_debits      = src.total_debits,
      total_credits     = src.total_credits,
      transaction_count = src.transaction_count,
      etl_loaded_ts     = CURRENT_TIMESTAMP()
  WHEN NOT MATCHED THEN INSERT (
      account_sk, date_key, opening_balance, closing_balance,
      total_debits, total_credits, transaction_count, currency, etl_loaded_ts
  ) VALUES (
      src.account_sk, src.date_key, src.opening_balance, src.closing_balance,
      src.total_debits, src.total_credits, src.transaction_count, src.currency,
      CURRENT_TIMESTAMP()
  );

  LET v_txn_count INTEGER := (
      SELECT COUNT(*) FROM barclays_dwh.fct_transaction
      WHERE date_key = TO_NUMBER(TO_CHAR(CURRENT_DATE(), 'YYYYMMDD'))
  );

  INSERT INTO barclays_dwh.etl_audit_log (
      pipeline_name, step_name, status, row_count, started_at, completed_at
  )
  VALUES ('DAILY_ETL', 'DAILY_TRANSACTION_LOAD', 'SUCCESS', v_txn_count,
          CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP());
END;

-- =============================================================================
-- CHILD TASK: Customer risk scoring (Basel III)
-- Replaces: sp_customer_risk_scoring
-- Depends on: daily_transaction_load (needs updated customer dim + balances)
-- Migrated from: teradata/stored_procedures/sp_customer_risk_scoring.sql
-- =============================================================================
CREATE OR REPLACE TASK customer_risk_scoring
  WAREHOUSE = compute_wh
  AFTER daily_transaction_load
  COMMENT   = 'Basel III PD/LGD/EAD risk scoring; replaces sp_customer_risk_scoring'
AS
BEGIN
  INSERT INTO barclays_dwh.etl_audit_log (
      pipeline_name, step_name, status, row_count, started_at
  )
  VALUES ('DAILY_ETL', 'CUSTOMER_RISK_SCORING', 'RUNNING', 0, CURRENT_TIMESTAMP());

  -- Execute via dbt: dbt run --select tag:risk_scoring
  -- Or run the migrated SQL directly:

  -- Delete existing scores for today (idempotent re-run)
  DELETE FROM barclays_mart.mart_credit_risk
  WHERE assessment_date = CURRENT_DATE();

  -- Calculate risk scores
  -- Replaces volatile table vt_risk_factors + INSERT into MART_CREDIT_RISK
  INSERT INTO barclays_mart.mart_credit_risk (
      customer_id, assessment_date, risk_rating,
      probability_default, loss_given_default, exposure_at_default,
      risk_weighted_asset, expected_loss, unexpected_loss,
      risk_weight_pct, asset_class, model_version,
      etl_batch_id, etl_loaded_ts
  )
  WITH risk_factors AS (
      SELECT
          c.customer_id,
          c.risk_rating                                       AS current_risk_rating,
          c.kyc_status,
          c.segment,
          DATEDIFF('year', c.date_of_birth, CURRENT_DATE())   AS customer_age,
          DATEDIFF('day', c.effective_from, CURRENT_DATE())    AS tenure_days,
          COALESCE(acct.total_accounts, 0)                     AS total_accounts,
          COALESCE(acct.active_accounts, 0)                    AS active_accounts,
          COALESCE(acct.total_credit_limit, 0)                 AS total_credit_limit,
          COALESCE(acct.total_overdraft, 0)                    AS total_overdraft,
          COALESCE(txn.txn_count_90d, 0)                       AS txn_count_90d,
          COALESCE(txn.total_debit_90d, 0)                     AS total_debit_90d,
          COALESCE(txn.total_credit_90d, 0)                    AS total_credit_90d,
          COALESCE(txn.avg_txn_amount_90d, 0)                  AS avg_txn_amount_90d,
          COALESCE(txn.max_txn_amount_90d, 0)                  AS max_txn_amount_90d,
          NULLIF(COALESCE(txn.high_value_txn_count, 0), 0)     AS high_value_txn_count,
          COALESCE(bal.closing_balance, 0)                      AS latest_balance,
          MOD(HASH(c.customer_id), 10)                         AS model_segment
      FROM barclays_stg.v_customer_latest c
      LEFT JOIN (
          SELECT
              customer_id,
              COUNT(*)                                                        AS total_accounts,
              SUM(CASE WHEN status = 'ACTIVE' THEN 1 ELSE 0 END)            AS active_accounts,
              SUM(COALESCE(credit_limit, 0))                                  AS total_credit_limit,
              SUM(COALESCE(overdraft_limit, 0))                               AS total_overdraft
          FROM barclays_stg.v_account_current
          GROUP BY customer_id
      ) acct ON c.customer_id = acct.customer_id
      LEFT JOIN (
          SELECT
              a.customer_id,
              COUNT(*)                                                         AS txn_count_90d,
              SUM(CASE WHEN t.signed_amount < 0 THEN ABS(t.signed_amount) ELSE 0 END) AS total_debit_90d,
              SUM(CASE WHEN t.signed_amount > 0 THEN t.signed_amount ELSE 0 END)      AS total_credit_90d,
              AVG(t.amount)                                                    AS avg_txn_amount_90d,
              MAX(t.amount)                                                    AS max_txn_amount_90d,
              SUM(CASE WHEN t.value_band = 'HIGH_VALUE' THEN 1 ELSE 0 END)    AS high_value_txn_count
          FROM barclays_stg.v_transaction_enriched t
          INNER JOIN barclays_raw.account a ON t.account_id = a.account_id
          WHERE t.transaction_date BETWEEN DATEADD('day', -90, CURRENT_DATE()) AND CURRENT_DATE()
          GROUP BY a.customer_id
      ) txn ON c.customer_id = txn.customer_id
      LEFT JOIN (
          SELECT account_sk, closing_balance
          FROM barclays_dwh.fct_daily_balance
          QUALIFY ROW_NUMBER() OVER (PARTITION BY account_sk ORDER BY date_key DESC) = 1
      ) bal ON bal.account_sk IN (
          SELECT account_sk FROM barclays_dwh.dim_account
          WHERE customer_id = c.customer_id AND is_current = 'Y'
      )
  )
  SELECT
      rf.customer_id,
      CURRENT_DATE(),
      -- Risk rating
      CASE
          WHEN rf.kyc_status = 'EXPIRED'                                     THEN 'H'
          WHEN rf.high_value_txn_count IS NOT NULL AND rf.tenure_days < 365  THEN 'H'
          WHEN rf.total_debit_90d > rf.total_credit_limit * 0.8              THEN 'H'
          WHEN rf.txn_count_90d < 3 AND rf.tenure_days > 730                 THEN 'M'
          WHEN rf.latest_balance < 0                                          THEN 'M'
          ELSE 'L'
      END,
      -- PD
      CASE
          WHEN rf.kyc_status = 'EXPIRED'    THEN 0.05
          WHEN rf.tenure_days < 365         THEN 0.03
          WHEN rf.latest_balance < 0        THEN 0.02
          ELSE 0.005
      END,
      -- LGD
      CASE rf.segment
          WHEN 'RETAIL'    THEN 0.45
          WHEN 'WEALTH'    THEN 0.30
          WHEN 'CORPORATE' THEN 0.40
          ELSE 0.45
      END,
      -- EAD
      rf.total_credit_limit + rf.total_overdraft,
      -- RWA
      (rf.total_credit_limit + rf.total_overdraft)
          * CASE rf.segment
              WHEN 'RETAIL'    THEN 0.75
              WHEN 'WEALTH'    THEN 0.50
              WHEN 'CORPORATE' THEN 1.00
              ELSE 0.75
          END,
      -- Expected Loss = PD * LGD * EAD
      CASE
          WHEN rf.kyc_status = 'EXPIRED' THEN 0.05
          WHEN rf.tenure_days < 365      THEN 0.03
          ELSE 0.005
      END
      * CASE rf.segment WHEN 'RETAIL' THEN 0.45 WHEN 'WEALTH' THEN 0.30 ELSE 0.40 END
      * (rf.total_credit_limit + rf.total_overdraft),
      0,  -- unexpected_loss placeholder
      -- Risk weight pct
      CASE rf.segment
          WHEN 'RETAIL'    THEN 75.00
          WHEN 'WEALTH'    THEN 50.00
          WHEN 'CORPORATE' THEN 100.00
          ELSE 75.00
      END,
      -- Asset class
      CASE rf.segment
          WHEN 'RETAIL'    THEN 'RETAIL_OTHER'
          WHEN 'WEALTH'    THEN 'RETAIL_REVOLVING'
          WHEN 'CORPORATE' THEN 'CORPORATE'
          ELSE 'RETAIL_OTHER'
      END,
      'BRCL_RISK_v2.1',
      TO_NUMBER(TO_CHAR(CURRENT_DATE(), 'YYYYMMDD') || '002'),
      CURRENT_TIMESTAMP()
  FROM risk_factors rf;

  LET v_scored INTEGER := SQLROWCOUNT;

  INSERT INTO barclays_dwh.etl_audit_log (
      pipeline_name, step_name, status, row_count, started_at, completed_at
  )
  VALUES ('DAILY_ETL', 'CUSTOMER_RISK_SCORING', 'SUCCESS', v_scored,
          CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP());
END;

-- =============================================================================
-- CHILD TASK: AML screening
-- Replaces: sp_aml_screening
-- Depends on: daily_transaction_load (needs today's transactions)
-- Migrated from: teradata/stored_procedures/sp_aml_screening.sql
-- Note: Runs in parallel with customer_risk_scoring
-- =============================================================================
CREATE OR REPLACE TASK aml_screening
  WAREHOUSE = compute_wh
  AFTER daily_transaction_load
  COMMENT   = 'AML sanctions/structuring/velocity screening; replaces sp_aml_screening'
AS
BEGIN
  INSERT INTO barclays_dwh.etl_audit_log (
      pipeline_name, step_name, status, row_count, started_at
  )
  VALUES ('DAILY_ETL', 'AML_SCREENING', 'RUNNING', 0, CURRENT_TIMESTAMP());

  -- Execute via dbt: dbt run --select tag:aml_screening
  -- Or run the migrated SQL directly:

  LET v_batch_id BIGINT := TO_NUMBER(TO_CHAR(CURRENT_DATE(), 'YYYYMMDD') || '004');

  -- Clear today's alerts for idempotent re-run
  DELETE FROM barclays_mart.mart_aml_alerts
  WHERE alert_date = CURRENT_DATE()
    AND etl_batch_id = :v_batch_id;

  -- Step 1: Name screening against sanctions watchlist
  -- (Replaces SOUNDEX + LIKE ANY matching from sp_aml_screening lines 55-85)
  INSERT INTO barclays_mart.mart_aml_alerts (
      customer_id, account_id, alert_date, alert_type, severity,
      match_score, matched_entity, alert_status,
      etl_batch_id, etl_loaded_ts
  )
  SELECT
      t.account_id,
      t.account_id,
      CURRENT_DATE(),
      'SANCTIONS_HIT',
      CASE
          WHEN nm.match_score >= 90 THEN 'CRITICAL'
          WHEN nm.match_score >= 70 THEN 'HIGH'
          ELSE 'MEDIUM'
      END,
      nm.match_score,
      nm.matched_entity,
      'OPEN',
      :v_batch_id,
      CURRENT_TIMESTAMP()
  FROM (
      SELECT
          cp.counterparty_id,
          cp.counterparty_name,
          sl.entity_name AS matched_entity,
          sl.list_source,
          CASE
              WHEN UPPER(REPLACE(cp.counterparty_name, '.', ''))
                 = UPPER(REPLACE(sl.entity_name, '.', ''))
              THEN 100.0
              WHEN SOUNDEX(cp.counterparty_name) = SOUNDEX(sl.entity_name)
              THEN 75.0
              WHEN UPPER(cp.counterparty_name) LIKE '%' || UPPER(TRIM(sl.entity_name)) || '%'
              THEN 60.0
              ELSE 0.0
          END AS match_score
      FROM barclays_stg.v_counterparty_screened cp
      CROSS JOIN barclays_ref.sanctions_watchlist sl
      WHERE UPPER(cp.counterparty_name) LIKE '%' || UPPER(TRIM(sl.entity_name)) || '%'
         OR SOUNDEX(cp.counterparty_name) = SOUNDEX(sl.entity_name)
  ) nm
  INNER JOIN barclays_raw.transaction t
      ON t.counterparty_id = nm.counterparty_id
     AND t.transaction_date = CURRENT_DATE()
  WHERE nm.match_score > 0;

  -- Step 2: Detect structuring (transactions just below GBP 10k threshold)
  -- (Replaces vt_structuring_alerts from sp_aml_screening lines 90-107)
  INSERT INTO barclays_mart.mart_aml_alerts (
      customer_id, account_id, alert_date, alert_type, severity,
      match_score, matched_entity, alert_status,
      etl_batch_id, etl_loaded_ts
  )
  SELECT
      a.customer_id,
      a.account_id,
      CURRENT_DATE(),
      'STRUCTURING',
      CASE
          WHEN SUM(t.amount) > 50000 THEN 'HIGH'
          WHEN COUNT(*) >= 5 THEN 'HIGH'
          ELSE 'MEDIUM'
      END,
      CAST(COUNT(*) AS DECIMAL(5,2)) * 10,
      'STRUCTURING PATTERN DETECTED',
      'OPEN',
      :v_batch_id,
      CURRENT_TIMESTAMP()
  FROM barclays_stg.v_transaction_enriched t
  INNER JOIN barclays_raw.account a ON t.account_id = a.account_id
  WHERE t.transaction_date BETWEEN DATEADD('day', -7, CURRENT_DATE()) AND CURRENT_DATE()
    AND t.amount BETWEEN 8000.00 AND 9999.99
    AND t.transaction_type IN ('CREDIT', 'TRANSFER')
  GROUP BY a.customer_id, a.account_id, t.transaction_date
  HAVING COUNT(*) >= 3;

  -- Step 3: Detect velocity breaches (unusual transaction frequency)
  -- (Replaces vt_velocity_alerts from sp_aml_screening lines 112-127)
  INSERT INTO barclays_mart.mart_aml_alerts (
      customer_id, account_id, alert_date, alert_type, severity,
      match_score, matched_entity, alert_status,
      etl_batch_id, etl_loaded_ts
  )
  SELECT
      a.customer_id,
      a.account_id,
      CURRENT_DATE(),
      'VELOCITY_BREACH',
      CASE
          WHEN SUM(t.amount) > 500000 THEN 'CRITICAL'
          WHEN SUM(t.amount) > 100000 THEN 'HIGH'
          ELSE 'MEDIUM'
      END,
      CAST(COUNT(*) AS DECIMAL(5,2)),
      'VELOCITY BREACH - ' || TRIM(CAST(COUNT(*) AS VARCHAR(10))) || ' TXNS',
      'OPEN',
      :v_batch_id,
      CURRENT_TIMESTAMP()
  FROM barclays_stg.v_transaction_enriched t
  INNER JOIN barclays_raw.account a ON t.account_id = a.account_id
  WHERE t.transaction_date = CURRENT_DATE()
  GROUP BY a.customer_id, a.account_id, t.transaction_date
  HAVING COUNT(*) > 20 OR SUM(t.amount) > 100000.00;

  LET v_alert_count INTEGER := (
      SELECT COUNT(*) FROM barclays_mart.mart_aml_alerts
      WHERE alert_date = CURRENT_DATE()
        AND etl_batch_id = :v_batch_id
  );

  INSERT INTO barclays_dwh.etl_audit_log (
      pipeline_name, step_name, status, row_count, started_at, completed_at
  )
  VALUES ('DAILY_ETL', 'AML_SCREENING', 'SUCCESS', v_alert_count,
          CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP());
END;

-- =============================================================================
-- CHILD TASK: Daily post-load validation
-- Replaces: BRCL_DAILY_008_VALIDATION
-- Depends on: customer_risk_scoring AND aml_screening (both must complete)
-- =============================================================================
CREATE OR REPLACE TASK daily_validation
  WAREHOUSE = compute_wh
  AFTER customer_risk_scoring, aml_screening
  COMMENT   = 'Post-load validation checks; replaces BRCL_DAILY_008_VALIDATION'
AS
BEGIN
  INSERT INTO barclays_dwh.etl_audit_log (
      pipeline_name, step_name, status, row_count, started_at
  )
  VALUES ('DAILY_ETL', 'DAILY_VALIDATION', 'RUNNING', 0, CURRENT_TIMESTAMP());

  LET v_issues_found INTEGER := 0;

  -- Validation 1: Transaction count consistency (raw vs DWH)
  LET v_raw_count INTEGER := (
      SELECT COUNT(*) FROM barclays_raw.transaction
      WHERE transaction_date = CURRENT_DATE()
  );
  LET v_dwh_count INTEGER := (
      SELECT COUNT(*) FROM barclays_dwh.fct_transaction
      WHERE date_key = TO_NUMBER(TO_CHAR(CURRENT_DATE(), 'YYYYMMDD'))
  );
  IF (ABS(v_raw_count - v_dwh_count) > v_raw_count * 0.01) THEN
      v_issues_found := v_issues_found + 1;
      INSERT INTO barclays_dwh.etl_validation_results (
          validation_date, check_name, status, expected_value, actual_value, details
      ) VALUES (
          CURRENT_DATE(), 'TXN_COUNT_CONSISTENCY', 'FAIL',
          v_raw_count::STRING, v_dwh_count::STRING,
          'Raw vs DWH transaction count mismatch exceeds 1% threshold'
      );
  END IF;

  -- Validation 2: No orphan transactions (all accounts have valid customers)
  LET v_orphan_count INTEGER := (
      SELECT COUNT(*)
      FROM barclays_dwh.fct_transaction ft
      WHERE ft.date_key = TO_NUMBER(TO_CHAR(CURRENT_DATE(), 'YYYYMMDD'))
        AND ft.customer_sk NOT IN (
            SELECT customer_sk FROM barclays_dwh.dim_customer WHERE is_current = 'Y'
        )
  );
  IF (v_orphan_count > 0) THEN
      v_issues_found := v_issues_found + 1;
      INSERT INTO barclays_dwh.etl_validation_results (
          validation_date, check_name, status, expected_value, actual_value, details
      ) VALUES (
          CURRENT_DATE(), 'ORPHAN_TRANSACTIONS', 'FAIL',
          '0', v_orphan_count::STRING,
          'Transactions found with invalid customer surrogate keys'
      );
  END IF;

  -- Validation 3: Risk scores completeness
  LET v_customers_without_risk INTEGER := (
      SELECT COUNT(*)
      FROM barclays_dwh.dim_customer dc
      WHERE dc.is_current = 'Y'
        AND dc.customer_id NOT IN (
            SELECT customer_id FROM barclays_mart.mart_credit_risk
            WHERE assessment_date = CURRENT_DATE()
        )
  );

  -- Validation 4: AML alert count check
  LET v_aml_alerts INTEGER := (
      SELECT COUNT(*) FROM barclays_mart.mart_aml_alerts
      WHERE alert_date = CURRENT_DATE()
  );

  -- Log validation summary
  INSERT INTO barclays_dwh.etl_audit_log (
      pipeline_name, step_name, status, row_count, started_at, completed_at, details
  )
  VALUES (
      'DAILY_ETL', 'DAILY_VALIDATION',
      CASE WHEN v_issues_found = 0 THEN 'SUCCESS' ELSE 'WARNING' END,
      v_issues_found,
      CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(),
      OBJECT_CONSTRUCT(
          'raw_txn_count', v_raw_count,
          'dwh_txn_count', v_dwh_count,
          'orphan_transactions', v_orphan_count,
          'customers_without_risk', v_customers_without_risk,
          'aml_alerts_today', v_aml_alerts,
          'issues_found', v_issues_found
      )::STRING
  );
END;

-- =============================================================================
-- CHILD TASK: Daily notification
-- Replaces: BRCL_DAILY_009_NOTIFICATION
-- =============================================================================
CREATE OR REPLACE TASK daily_notification
  WAREHOUSE = compute_wh
  AFTER daily_validation
  COMMENT   = 'Send completion notification; replaces BRCL_DAILY_009_NOTIFICATION'
AS
BEGIN
  LET v_status STRING := (
      SELECT COALESCE(MAX(status), 'UNKNOWN')
      FROM barclays_dwh.etl_audit_log
      WHERE pipeline_name = 'DAILY_ETL'
        AND step_name = 'DAILY_VALIDATION'
        AND started_at::DATE = CURRENT_DATE()
  );

  CALL SYSTEM$SEND_NOTIFICATION(
      'daily_etl_alerts',
      CONCAT('Daily ETL ', v_status, ' - ', CURRENT_DATE()::STRING),
      CONCAT(
          'Daily ETL pipeline completed at ', CURRENT_TIMESTAMP()::STRING, '\n',
          'Status: ', v_status, '\n',
          'Check barclays_dwh.etl_audit_log for full details.'
      )
  );

  INSERT INTO barclays_dwh.etl_audit_log (
      pipeline_name, step_name, status, row_count, started_at, completed_at
  )
  VALUES ('DAILY_ETL', 'NOTIFICATION_SENT', 'SUCCESS', 0,
          CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP());
END;

-- =============================================================================
-- ENABLE THE TASK DAG (root task must be resumed last)
-- =============================================================================
ALTER TASK daily_notification      RESUME;
ALTER TASK daily_validation        RESUME;
ALTER TASK aml_screening           RESUME;
ALTER TASK customer_risk_scoring   RESUME;
ALTER TASK market_data_load        RESUME;
ALTER TASK daily_transaction_load  RESUME;
ALTER TASK raw_data_validation     RESUME;  -- Root task: resume LAST

-- =============================================================================
-- MONITORING QUERIES
-- =============================================================================

-- View task execution history (last 24 hours)
-- SELECT *
-- FROM TABLE(information_schema.task_history(
--     scheduled_time_range_start => DATEADD('hour', -24, CURRENT_TIMESTAMP()),
--     result_limit => 100
-- ))
-- ORDER BY scheduled_time DESC;

-- View current task DAG state
-- SHOW TASKS IN SCHEMA orchestration;

-- Check for failed tasks in the last 7 days
-- SELECT name, state, error_code, error_message, scheduled_time, completed_time
-- FROM TABLE(information_schema.task_history(
--     scheduled_time_range_start => DATEADD('day', -7, CURRENT_TIMESTAMP())
-- ))
-- WHERE state = 'FAILED'
-- ORDER BY scheduled_time DESC;
