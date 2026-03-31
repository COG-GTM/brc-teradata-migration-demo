/*******************************************************************************
 * sp_daily_transaction_load
 *
 * Daily ETL procedure that:
 *   1. Loads new transactions from RAW into the DWH fact table
 *   2. Implements SCD Type 2 logic for the customer dimension
 *   3. Updates daily balance snapshots
 *   4. Collects statistics after load
 *
 * Teradata-specific features:
 *   - DECLARE / SET / CALL
 *   - Cursor loops
 *   - ACTIVITY_COUNT
 *   - SQLSTATE error handling
 *   - MERGE INTO for upserts
 *   - LOCK ROW FOR ACCESS
 *   - COLLECT STATISTICS
 ******************************************************************************/

REPLACE PROCEDURE BARCLAYS_DWH.sp_daily_transaction_load (
    IN p_business_date DATE
)
BEGIN
    -- Local variable declarations
    DECLARE v_batch_id       BIGINT;
    DECLARE v_row_count      INTEGER;
    DECLARE v_error_code     INTEGER DEFAULT 0;
    DECLARE v_sqlstate       CHAR(5);
    DECLARE v_cust_id        INTEGER;
    DECLARE v_old_risk       CHAR(1);
    DECLARE v_new_risk       CHAR(1);
    DECLARE v_old_kyc        VARCHAR(20);
    DECLARE v_new_kyc        VARCHAR(20);

    -- Error handler
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        SET v_sqlstate = SQLSTATE;
        INSERT INTO BARCLAYS_DWH.ETL_ERROR_LOG (
            procedure_name, error_code, error_state, error_ts, batch_id
        ) VALUES (
            'sp_daily_transaction_load', SQLCODE, v_sqlstate, CURRENT_TIMESTAMP, v_batch_id
        );
    END;

    -- Generate batch ID from timestamp
    SET v_batch_id = CAST(
        CAST(p_business_date AS FORMAT 'YYYYMMDD') || '001' AS BIGINT
    );

    ---------------------------------------------------------------------------
    -- STEP 1: SCD Type 2 - Detect changed customers
    ---------------------------------------------------------------------------
    -- Cursor to iterate over customers with changed attributes
    FOR cur_changed AS c_changed CURSOR FOR
        SELECT
            src.customer_id,
            tgt.risk_rating   AS old_risk_rating,
            src.risk_rating   AS new_risk_rating,
            tgt.kyc_status    AS old_kyc_status,
            src.kyc_status    AS new_kyc_status
        FROM BARCLAYS_STG.V_CUSTOMER_LATEST src
        INNER JOIN BARCLAYS_DWH.DIM_CUSTOMER tgt
            ON src.customer_id = tgt.customer_id
           AND tgt.is_current = 'Y'
        WHERE src.risk_rating <> tgt.risk_rating
           OR src.kyc_status  <> tgt.kyc_status
           OR src.segment     <> tgt.segment
    DO
        -- Close the current record
        UPDATE BARCLAYS_DWH.DIM_CUSTOMER
        SET is_current    = 'N',
            effective_to  = p_business_date - 1,
            etl_batch_id  = v_batch_id,
            etl_loaded_ts = CURRENT_TIMESTAMP(6)
        WHERE customer_id = cur_changed.customer_id
          AND is_current  = 'Y';

        -- Insert the new version
        INSERT INTO BARCLAYS_DWH.DIM_CUSTOMER (
            customer_id, first_name, last_name, date_of_birth,
            nationality, kyc_status, risk_rating, segment,
            postcode, country,
            validity_period, is_current, effective_from, effective_to,
            etl_batch_id, etl_loaded_ts
        )
        SELECT
            customer_id, first_name, last_name, date_of_birth,
            nationality, kyc_status, risk_rating, segment,
            postcode, country,
            PERIOD(p_business_date, DATE '9999-12-31'),
            'Y', p_business_date, DATE '9999-12-31',
            v_batch_id, CURRENT_TIMESTAMP(6)
        FROM BARCLAYS_STG.V_CUSTOMER_LATEST
        WHERE customer_id = cur_changed.customer_id;
    END FOR;

    -- Insert brand-new customers (not yet in dimension)
    INSERT INTO BARCLAYS_DWH.DIM_CUSTOMER (
        customer_id, first_name, last_name, date_of_birth,
        nationality, kyc_status, risk_rating, segment,
        postcode, country,
        validity_period, is_current, effective_from, effective_to,
        etl_batch_id, etl_loaded_ts
    )
    SELECT
        src.customer_id, src.first_name, src.last_name, src.date_of_birth,
        src.nationality, src.kyc_status, src.risk_rating, src.segment,
        src.postcode, src.country,
        PERIOD(p_business_date, DATE '9999-12-31'),
        'Y', p_business_date, DATE '9999-12-31',
        v_batch_id, CURRENT_TIMESTAMP(6)
    FROM BARCLAYS_STG.V_CUSTOMER_LATEST src
    LEFT JOIN BARCLAYS_DWH.DIM_CUSTOMER tgt
        ON src.customer_id = tgt.customer_id
    WHERE tgt.customer_id IS NULL;

    SET v_row_count = ACTIVITY_COUNT;

    ---------------------------------------------------------------------------
    -- STEP 2: Upsert accounts via MERGE
    ---------------------------------------------------------------------------
    MERGE INTO BARCLAYS_DWH.DIM_ACCOUNT tgt
    USING BARCLAYS_STG.V_ACCOUNT_CURRENT src
    ON tgt.account_id = src.account_id AND tgt.is_current = 'Y'
    WHEN MATCHED THEN UPDATE SET
        status          = src.status,
        close_date      = src.close_date,
        credit_limit    = ZEROIFNULL(src.credit_limit),
        overdraft_limit = ZEROIFNULL(src.overdraft_limit),
        etl_batch_id    = v_batch_id,
        etl_loaded_ts   = CURRENT_TIMESTAMP(6)
    WHEN NOT MATCHED THEN INSERT (
        account_id, customer_id, account_type, currency,
        branch_code, sort_code, status, open_date, close_date,
        credit_limit, overdraft_limit, is_current,
        etl_batch_id, etl_loaded_ts
    ) VALUES (
        src.account_id, src.customer_id, src.account_type, src.currency,
        src.branch_code, src.sort_code, src.status, src.open_date, src.close_date,
        ZEROIFNULL(src.credit_limit), ZEROIFNULL(src.overdraft_limit), 'Y',
        v_batch_id, CURRENT_TIMESTAMP(6)
    );

    ---------------------------------------------------------------------------
    -- STEP 3: Load transactions into fact table
    ---------------------------------------------------------------------------
    INSERT INTO BARCLAYS_DWH.FCT_TRANSACTION (
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
        CAST(CAST(t.transaction_date AS FORMAT 'YYYYMMDD') AS INTEGER) AS date_key,
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
        v_batch_id,
        CURRENT_TIMESTAMP(6)
    FROM BARCLAYS_STG.V_TRANSACTION_ENRICHED t
    LOCK ROW FOR ACCESS
    INNER JOIN BARCLAYS_DWH.DIM_ACCOUNT da
        ON t.account_id = da.account_id AND da.is_current = 'Y'
    INNER JOIN BARCLAYS_DWH.DIM_CUSTOMER dc
        ON da.customer_id = dc.customer_id AND dc.is_current = 'Y'
    WHERE t.transaction_date = p_business_date;

    SET v_row_count = ACTIVITY_COUNT;

    ---------------------------------------------------------------------------
    -- STEP 4: Update daily balance snapshots
    ---------------------------------------------------------------------------
    MERGE INTO BARCLAYS_DWH.FCT_DAILY_BALANCE tgt
    USING (
        SELECT
            da.account_sk,
            CAST(CAST(p_business_date AS FORMAT 'YYYYMMDD') AS INTEGER) AS date_key,
            ZEROIFNULL(prev.closing_balance) AS opening_balance,
            ZEROIFNULL(prev.closing_balance) + ZEROIFNULL(txn.net_amount) AS closing_balance,
            ZEROIFNULL(txn.total_debits) AS total_debits,
            ZEROIFNULL(txn.total_credits) AS total_credits,
            ZEROIFNULL(txn.txn_count) AS transaction_count,
            da.currency
        FROM BARCLAYS_DWH.DIM_ACCOUNT da
        LEFT JOIN BARCLAYS_DWH.FCT_DAILY_BALANCE prev
            ON da.account_sk = prev.account_sk
           AND prev.date_key = CAST(
               CAST(p_business_date - 1 AS FORMAT 'YYYYMMDD') AS INTEGER
           )
        LEFT JOIN (
            SELECT
                account_sk,
                SUM(CASE WHEN signed_amount < 0 THEN ABS(signed_amount) ELSE 0 END) AS total_debits,
                SUM(CASE WHEN signed_amount > 0 THEN signed_amount ELSE 0 END) AS total_credits,
                SUM(signed_amount) AS net_amount,
                COUNT(*) AS txn_count
            FROM BARCLAYS_DWH.FCT_TRANSACTION
            WHERE date_key = CAST(CAST(p_business_date AS FORMAT 'YYYYMMDD') AS INTEGER)
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
        etl_batch_id      = v_batch_id,
        etl_loaded_ts     = CURRENT_TIMESTAMP(6)
    WHEN NOT MATCHED THEN INSERT (
        account_sk, date_key, opening_balance, closing_balance,
        total_debits, total_credits, transaction_count, currency,
        etl_batch_id, etl_loaded_ts
    ) VALUES (
        src.account_sk, src.date_key, src.opening_balance, src.closing_balance,
        src.total_debits, src.total_credits, src.transaction_count, src.currency,
        v_batch_id, CURRENT_TIMESTAMP(6)
    );

    ---------------------------------------------------------------------------
    -- STEP 5: Collect statistics on modified tables
    ---------------------------------------------------------------------------
    COLLECT STATISTICS ON BARCLAYS_DWH.DIM_CUSTOMER   COLUMN (customer_id);
    COLLECT STATISTICS ON BARCLAYS_DWH.DIM_ACCOUNT    COLUMN (account_id);
    COLLECT STATISTICS ON BARCLAYS_DWH.FCT_TRANSACTION COLUMN (date_key);
    COLLECT STATISTICS ON BARCLAYS_DWH.FCT_DAILY_BALANCE COLUMN (date_key);

END;
