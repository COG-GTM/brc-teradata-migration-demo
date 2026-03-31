/*******************************************************************************
 * account_balance_upsert.ml
 *
 * MultiLoad script for upserting daily account balance snapshots.
 * Uses the DO INSERT FOR MISSING UPDATE ROWS pattern for upsert behaviour.
 *
 * Pattern based on COG-GTM/Teradata-Utilities-Script CustomerUPDATE.ml
 ******************************************************************************/

.LOGTABLE BARCLAYS_DWH.BALANCE_ML_LOG;
.LOGON ${TERADATA_HOST}/barclays_etl,${ETL_PASSWORD};

.BEGIN MLOAD TABLES BARCLAYS_DWH.FCT_DAILY_BALANCE;

.LAYOUT BALANCE_LAYOUT;
.FIELD in_account_sk        * VARCHAR(10);
.FIELD in_date_key          * VARCHAR(8);
.FIELD in_opening_balance   * VARCHAR(20);
.FIELD in_closing_balance   * VARCHAR(20);
.FIELD in_total_debits      * VARCHAR(20);
.FIELD in_total_credits     * VARCHAR(20);
.FIELD in_transaction_count * VARCHAR(10);
.FIELD in_currency          * VARCHAR(3);

.DML LABEL UPD_BALANCE
DO INSERT FOR MISSING UPDATE ROWS;

UPDATE BARCLAYS_DWH.FCT_DAILY_BALANCE
SET
    opening_balance   = CAST(:in_opening_balance AS DECIMAL(18,2)),
    closing_balance   = CAST(:in_closing_balance AS DECIMAL(18,2)),
    total_debits      = CAST(:in_total_debits AS DECIMAL(18,2)),
    total_credits     = CAST(:in_total_credits AS DECIMAL(18,2)),
    transaction_count = CAST(:in_transaction_count AS INTEGER),
    etl_loaded_ts     = CURRENT_TIMESTAMP(6)
WHERE account_sk = CAST(:in_account_sk AS INTEGER)
  AND date_key   = CAST(:in_date_key AS INTEGER);

INSERT INTO BARCLAYS_DWH.FCT_DAILY_BALANCE
(
    account_sk,
    date_key,
    opening_balance,
    closing_balance,
    total_debits,
    total_credits,
    transaction_count,
    currency,
    etl_loaded_ts
)
VALUES
(
    CAST(:in_account_sk AS INTEGER),
    CAST(:in_date_key AS INTEGER),
    CAST(:in_opening_balance AS DECIMAL(18,2)),
    CAST(:in_closing_balance AS DECIMAL(18,2)),
    CAST(:in_total_debits AS DECIMAL(18,2)),
    CAST(:in_total_credits AS DECIMAL(18,2)),
    CAST(:in_transaction_count AS INTEGER),
    :in_currency,
    CURRENT_TIMESTAMP(6)
);

.IMPORT INFILE ${INPUT_DIR}/daily_balances_${YYYYMMDD}.dat
FORMAT VARTEXT '|'
LAYOUT BALANCE_LAYOUT
APPLY UPD_BALANCE;

.END MLOAD;
.LOGOFF;
