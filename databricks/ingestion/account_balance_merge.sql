-- =============================================================================
-- account_balance_merge.sql
--
-- Replaces: teradata/multiload/account_balance_upsert.ml
--
-- Mapping rationale:
--   MultiLoad's `.DML LABEL ... DO INSERT FOR MISSING UPDATE ROWS` is an upsert
--   keyed on the UPDATE predicate (account_sk, date_key). Delta Lake expresses
--   the same semantics natively with MERGE INTO: WHEN MATCHED THEN UPDATE is
--   the .ml UPDATE block, WHEN NOT MATCHED THEN INSERT is the "insert for
--   missing update rows" fallback. The MultiLoad phases (acquisition /
--   application), the .LOGTABLE (BALANCE_ML_LOG) and the table-level MLOAD lock
--   all disappear: Delta gives ACID commits and snapshot isolation, so readers
--   are never blocked mid-load.
--
--   Step 1 replaces `.IMPORT INFILE ${INPUT_DIR}/daily_balances_${YYYYMMDD}.dat
--   FORMAT VARTEXT '|'` (the acquisition phase) with COPY INTO of the landing
--   file into a staging Delta table. Step 2 is the application phase.
--
--   CURRENT_TIMESTAMP(6) -> CURRENT_TIMESTAMP() (Databricks TIMESTAMP is
--   microsecond precision already, and the (6) argument is not valid syntax).
--
--   Duplicate keys within one input file would raise
--   DELTA_MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW_IN_MERGE, whereas MultiLoad
--   applied them in file order; step 1b therefore de-duplicates on
--   (account_sk, date_key) keeping the last row per key.
-- =============================================================================

-- Parameters: catalog, schema_dwh, landing_path, delimiter

-- -----------------------------------------------------------------------------
-- Step 1a: acquisition phase — load the delimited extract into staging.
-- -----------------------------------------------------------------------------
TRUNCATE TABLE ${catalog}.${schema_dwh}.stg_daily_balance_load;

COPY INTO ${catalog}.${schema_dwh}.stg_daily_balance_load
FROM (
    SELECT
        CAST(account_sk AS INT)                     AS account_sk,
        CAST(date_key AS INT)                       AS date_key,
        CAST(opening_balance AS DECIMAL(18,2))      AS opening_balance,
        CAST(closing_balance AS DECIMAL(18,2))      AS closing_balance,
        CAST(total_debits AS DECIMAL(18,2))         AS total_debits,
        CAST(total_credits AS DECIMAL(18,2))        AS total_credits,
        CAST(transaction_count AS INT)              AS transaction_count,
        CAST(currency AS STRING)                    AS currency,
        _metadata.file_path                         AS _source_file,
        CURRENT_TIMESTAMP()                         AS _ingested_at
    FROM '${landing_path}'
)
FILEFORMAT = CSV
PATTERN = 'daily_balances*.csv'
FORMAT_OPTIONS (
    'header'      = 'true',
    'sep'         = '${delimiter}',
    'inferSchema' = 'false',
    'mode'        = 'PERMISSIVE'
)
COPY_OPTIONS (
    'force'       = 'true'   -- staging is truncated each run, so replay the file
);

-- -----------------------------------------------------------------------------
-- Step 2: application phase — upsert into the balance fact.
-- (Step 1b de-duplication is inlined as the MERGE source.)
-- -----------------------------------------------------------------------------
MERGE INTO ${catalog}.${schema_dwh}.fct_daily_balance AS tgt
USING (
    SELECT
        account_sk,
        date_key,
        opening_balance,
        closing_balance,
        total_debits,
        total_credits,
        transaction_count,
        currency
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY account_sk, date_key
                ORDER BY _ingested_at DESC, _source_file DESC
            ) AS rn
        FROM ${catalog}.${schema_dwh}.stg_daily_balance_load
        WHERE account_sk IS NOT NULL
          AND date_key IS NOT NULL
    )
    WHERE rn = 1
) AS src
ON  tgt.account_sk = src.account_sk
AND tgt.date_key   = src.date_key

WHEN MATCHED THEN UPDATE SET
    tgt.opening_balance   = src.opening_balance,
    tgt.closing_balance   = src.closing_balance,
    tgt.total_debits      = src.total_debits,
    tgt.total_credits     = src.total_credits,
    tgt.transaction_count = src.transaction_count,
    tgt.etl_loaded_ts     = CURRENT_TIMESTAMP()

WHEN NOT MATCHED THEN INSERT (
    account_sk,
    date_key,
    opening_balance,
    closing_balance,
    total_debits,
    total_credits,
    transaction_count,
    currency,
    etl_loaded_ts
) VALUES (
    src.account_sk,
    src.date_key,
    src.opening_balance,
    src.closing_balance,
    src.total_debits,
    src.total_credits,
    src.transaction_count,
    src.currency,
    CURRENT_TIMESTAMP()
);

-- MultiLoad printed applied/inserted/updated counts at .END MLOAD; the same
-- numbers are available from the Delta history operationMetrics.
SELECT
    version,
    operation,
    operationMetrics['numTargetRowsInserted'] AS rows_inserted,
    operationMetrics['numTargetRowsUpdated']  AS rows_updated
FROM (DESCRIBE HISTORY ${catalog}.${schema_dwh}.fct_daily_balance)
WHERE operation = 'MERGE'
ORDER BY version DESC
LIMIT 1;
