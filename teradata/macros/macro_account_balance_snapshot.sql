/*******************************************************************************
 * macro_account_balance_snapshot
 *
 * Teradata MACRO that retrieves the balance snapshot for an account
 * on a given date, with prior-day comparison.
 ******************************************************************************/

REPLACE MACRO BARCLAYS_DWH.macro_account_balance_snapshot (
    p_account_id INTEGER,
    p_snapshot_date DATE DEFAULT CURRENT_DATE
) AS (
    SELECT
        b.account_sk,
        da.account_id,
        da.account_type,
        da.currency,
        b.date_key,
        b.opening_balance,
        b.closing_balance,
        b.total_debits,
        b.total_credits,
        b.transaction_count,
        b.closing_balance - ZEROIFNULL(prev.closing_balance) AS daily_change,
        CASE
            WHEN NULLIFZERO(prev.closing_balance) IS NOT NULL
            THEN (b.closing_balance - prev.closing_balance) / prev.closing_balance * 100
            ELSE 0
        END AS daily_change_pct
    FROM BARCLAYS_DWH.FCT_DAILY_BALANCE b
    INNER JOIN BARCLAYS_DWH.DIM_ACCOUNT da
        ON b.account_sk = da.account_sk
    LEFT JOIN BARCLAYS_DWH.FCT_DAILY_BALANCE prev
        ON b.account_sk = prev.account_sk
       AND prev.date_key = b.date_key - 1
    WHERE da.account_id = :p_account_id
      AND b.date_key = CAST(CAST(:p_snapshot_date AS FORMAT 'YYYYMMDD') AS INTEGER);
);
