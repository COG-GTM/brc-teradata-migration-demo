-- =============================================================================
-- transactions_copy_into.sql
--
-- Replaces: teradata/tpt/transaction_load.tpt
--
-- Mapping rationale:
--   The TPT job wires a DATACONNECTOR PRODUCER (delimited file reader) to a
--   LOAD operator with MAXSESSIONS 4, a log table and two error tables. On
--   Databricks the producer/consumer operator pair collapses into a single
--   COPY INTO statement: parallelism is the cluster/warehouse's concern rather
--   than a session count, the Delta transaction log replaces TXN_TPT_LOG, and
--   COPY INTO's per-file ledger plus PERMISSIVE parsing replace TXN_TPT_ET and
--   TXN_TPT_UV. SKIPROWS '1' becomes 'header' = 'true'.
--
--   Teradata CASTs from the APPLY clause are reproduced verbatim in the
--   SELECT: DATE FORMAT 'YYYY-MM-DD' -> TO_DATE(..., 'yyyy-MM-dd'),
--   DECIMAL(18,2) -> DECIMAL(18,2). transaction_id / account_id /
--   counterparty_id stay STRING in bronze because sample_data/transactions.csv
--   uses natural keys ('TXN0001', 'ACC001'); the numeric surrogate keys the
--   Teradata BIGINT/INTEGER casts implied are generated downstream in dbt.
--   sample_data/transactions.csv has no transaction_time or reference_number
--   column, so both are emitted as NULL and remain nullable in bronze.
-- =============================================================================

-- Parameters: catalog, schema_raw, landing_path, delimiter
COPY INTO ${catalog}.${schema_raw}.transaction
FROM (
    SELECT
        CAST(transaction_id AS STRING)                      AS transaction_id,
        CAST(account_id AS STRING)                          AS account_id,
        TO_DATE(transaction_date, 'yyyy-MM-dd')             AS transaction_date,
        CAST(NULL AS STRING)                                AS transaction_time,
        CAST(amount AS DECIMAL(18,2))                       AS amount,
        CAST(currency AS STRING)                            AS currency,
        CAST(transaction_type AS STRING)                    AS transaction_type,
        NULLIF(CAST(counterparty_id AS STRING), '')         AS counterparty_id,
        CAST(description AS STRING)                         AS description,
        CAST(channel AS STRING)                             AS channel,
        CAST(NULL AS STRING)                                AS reference_number,
        _metadata.file_path                                 AS _source_file,
        CURRENT_TIMESTAMP()                                 AS _ingested_at,
        CAST(NULL AS STRING)                                AS _rescued_data
    FROM '${landing_path}'
)
FILEFORMAT = CSV
PATTERN = 'transactions*.csv'
FORMAT_OPTIONS (
    'header'      = 'true',
    'sep'         = '${delimiter}',
    'inferSchema' = 'false',
    'mode'        = 'PERMISSIVE'
)
COPY_OPTIONS (
    'mergeSchema' = 'false'
);

-- Row-count feedback replacing the TPT job's end-of-job statistics.
SELECT
    COUNT(*)                        AS rows_in_target,
    COUNT(DISTINCT transaction_id)  AS distinct_transaction_ids,
    MAX(transaction_date)           AS max_transaction_date
FROM ${catalog}.${schema_raw}.transaction;
