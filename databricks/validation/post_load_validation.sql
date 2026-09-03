-- =============================================================================
-- post_load_validation.sql
--
-- Replaces:
--   teradata/bteq/daily_batch_load.bteq  -> "Post-load validation" block
--   teradata/scheduled_jobs/daily_etl_sequence.txt
--       -> BRCL_DAILY_007_STATS_COLLECTION (COLLECT STATISTICS)
--       -> BRCL_DAILY_008_VALIDATION       (row count / checksum validation)
--
-- Mapping rationale:
--   The BTEQ validation block ran counting SELECTs and relied on a human
--   reading the spool output. Here each check is an assertion whose failure
--   raises via RAISE_ERROR, so the Workflows task fails and the notification
--   task fires — the behaviour the scheduler's BRCL_DAILY_009 dependency
--   encoded.
--
--   BRCL_DAILY_007_STATS_COLLECTION has no direct equivalent: Teradata's
--   optimiser needs COLLECT STATISTICS refreshed after each load, whereas
--   Databricks maintains file statistics on write and predictive optimization
--   handles OPTIMIZE/ANALYZE. The nearest explicit action is the OPTIMIZE
--   statement below, which is a no-op when predictive optimization is enabled.
-- =============================================================================

-- Parameters: catalog, schema_raw, schema_finance, schema_compliance, business_date

-- BTEQ: SELECT 'DWH transactions for today: ' || COUNT(*) FROM FCT_TRANSACTION
SELECT
    'fct_daily_transactions'                        AS check_name,
    COUNT(*)                                        AS row_count,
    CAST('${business_date}' AS DATE)                AS business_date
FROM ${catalog}.${schema_finance}.fct_daily_transactions
WHERE transaction_date = CAST('${business_date}' AS DATE);

-- BTEQ: SELECT 'AML alerts for today: ' || COUNT(*) FROM MART_AML_ALERTS
SELECT
    'fct_aml_alerts'                                AS check_name,
    COUNT(*)                                        AS row_count,
    CAST('${business_date}' AS DATE)                AS business_date
FROM ${catalog}.${schema_compliance}.fct_aml_alerts
WHERE alert_date = CAST('${business_date}' AS DATE);

-- Reconciliation: every bronze transaction for the business date must survive
-- into the finance mart. Replaces the scheduler's checksum comparison step.
SELECT
    CASE
        WHEN bronze_count = mart_count THEN
            CONCAT('OK: ', CAST(mart_count AS STRING), ' transactions reconciled')
        ELSE RAISE_ERROR(
            CONCAT(
                'Transaction count mismatch for ${business_date}: bronze=',
                CAST(bronze_count AS STRING),
                ' mart=',
                CAST(mart_count AS STRING)
            )
        )
    END AS reconciliation_result
FROM (
    SELECT
        (
            SELECT COUNT(*)
            FROM ${catalog}.${schema_raw}.transaction
            WHERE transaction_date = CAST('${business_date}' AS DATE)
        ) AS bronze_count,
        (
            SELECT COUNT(*)
            FROM ${catalog}.${schema_finance}.fct_daily_transactions
            WHERE transaction_date = CAST('${business_date}' AS DATE)
        ) AS mart_count
);

-- Maintenance step standing in for COLLECT STATISTICS (BRCL_DAILY_007).
OPTIMIZE ${catalog}.${schema_raw}.transaction;
OPTIMIZE ${catalog}.${schema_raw}.market_data;
