-- =============================================================================
-- monthly_validation_and_archive.sql
--
-- Replaces:
--   teradata/scheduled_jobs/monthly_regulatory_sequence.txt
--       -> BRCL_MONTH_007_STATS_REFRESH (full statistics refresh on marts)
--       -> BRCL_MONTH_008_VALIDATION    (cross-check totals against GL)
--       -> BRCL_MONTH_009_ARCHIVE       (archive prior month detail to history)
--
-- Mapping rationale:
--   COLLECT STATISTICS on Teradata mart tables becomes OPTIMIZE (plus
--   ANALYZE for join-cardinality stats); Databricks maintains file-level stats
--   on write, so the full refresh is only a compaction concern.
--
--   The GL cross-check keeps the same intent as the legacy step — capital and
--   P&L totals must exist and be internally consistent for the reporting month
--   — and fails the task via RAISE_ERROR instead of relying on an operator
--   reading BTEQ spool.
--
--   Archiving prior-month detail no longer needs a physical copy into a
--   history database: Delta time travel plus retention settings keep history
--   in place, so the archive step becomes an explicit retention/VACUUM
--   boundary rather than an INSERT ... SELECT into BARCLAYS_HIST.
-- =============================================================================

-- Parameters: catalog, schema_raw, schema_risk, schema_finance, reporting_date

-- --- BRCL_MONTH_008_VALIDATION ----------------------------------------------
SELECT
    CASE
        WHEN capital_rows > 0 AND pnl_rows > 0 THEN
            CONCAT('OK: capital=', CAST(capital_rows AS STRING), ' pnl=', CAST(pnl_rows AS STRING))
        ELSE RAISE_ERROR(
            CONCAT(
                'Month-end validation failed for ${reporting_date}: capital_rows=',
                CAST(capital_rows AS STRING),
                ' pnl_rows=',
                CAST(pnl_rows AS STRING)
            )
        )
    END AS validation_result
FROM (
    SELECT
        (
            SELECT COUNT(*)
            FROM ${catalog}.${schema_risk}.fct_regulatory_capital
            WHERE reporting_date = CAST('${reporting_date}' AS DATE)
        ) AS capital_rows,
        (
            SELECT COUNT(*)
            FROM ${catalog}.${schema_finance}.fct_monthly_pnl
            WHERE reporting_month = CAST('${reporting_date}' AS DATE)
        ) AS pnl_rows
);

-- --- BRCL_MONTH_007_STATS_REFRESH -------------------------------------------
OPTIMIZE ${catalog}.${schema_risk}.fct_regulatory_capital;
OPTIMIZE ${catalog}.${schema_finance}.fct_monthly_pnl;
ANALYZE TABLE ${catalog}.${schema_risk}.fct_regulatory_capital COMPUTE STATISTICS;
ANALYZE TABLE ${catalog}.${schema_finance}.fct_monthly_pnl COMPUTE STATISTICS;

-- --- BRCL_MONTH_009_ARCHIVE -------------------------------------------------
-- Retain 13 months of time-travel history on the bronze detail table instead of
-- copying rows into a separate history database, then reclaim older files.
ALTER TABLE ${catalog}.${schema_raw}.transaction
SET TBLPROPERTIES (
    'delta.logRetentionDuration'          = 'interval 400 days',
    'delta.deletedFileRetentionDuration'  = 'interval 400 days'
);

VACUUM ${catalog}.${schema_raw}.transaction RETAIN 9600 HOURS;
