/*******************************************************************************
 * sp_customer_risk_scoring
 *
 * Calculates customer credit risk scores using:
 *   - QUALIFY ROW_NUMBER()
 *   - ZEROIFNULL / NULLIFZERO
 *   - Teradata date arithmetic (date - date = integer days)
 *   - CASE expressions with Teradata date formats
 *   - HASHROW / HASHBUCKET for deterministic bucketing
 *
 ******************************************************************************/

REPLACE PROCEDURE BARCLAYS_DWH.sp_customer_risk_scoring (
    IN p_assessment_date DATE
)
BEGIN
    DECLARE v_batch_id    BIGINT;
    DECLARE v_row_count   INTEGER;
    DECLARE v_sqlstate    CHAR(5);

    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        SET v_sqlstate = SQLSTATE;
        INSERT INTO BARCLAYS_DWH.ETL_ERROR_LOG (
            procedure_name, error_code, error_state, error_ts
        ) VALUES (
            'sp_customer_risk_scoring', SQLCODE, v_sqlstate, CURRENT_TIMESTAMP
        );
    END;

    SET v_batch_id = CAST(
        CAST(p_assessment_date AS FORMAT 'YYYYMMDD') || '002' AS BIGINT
    );

    ---------------------------------------------------------------------------
    -- Create volatile working table with risk factors
    ---------------------------------------------------------------------------
    CREATE VOLATILE TABLE vt_risk_factors AS (
        SELECT
            c.customer_id,
            c.risk_rating                            AS current_risk_rating,
            c.kyc_status,
            c.segment,
            -- Age in years using Teradata date arithmetic
            (p_assessment_date - c.date_of_birth) / 365 AS customer_age,
            -- Tenure in days
            (p_assessment_date - c.onboarding_date)     AS tenure_days,
            -- Account metrics
            ZEROIFNULL(acct.total_accounts)          AS total_accounts,
            ZEROIFNULL(acct.active_accounts)         AS active_accounts,
            ZEROIFNULL(acct.total_credit_limit)      AS total_credit_limit,
            ZEROIFNULL(acct.total_overdraft)          AS total_overdraft,
            -- Transaction metrics (last 90 days)
            ZEROIFNULL(txn.txn_count_90d)            AS txn_count_90d,
            ZEROIFNULL(txn.total_debit_90d)          AS total_debit_90d,
            ZEROIFNULL(txn.total_credit_90d)         AS total_credit_90d,
            ZEROIFNULL(txn.avg_txn_amount_90d)       AS avg_txn_amount_90d,
            ZEROIFNULL(txn.max_txn_amount_90d)       AS max_txn_amount_90d,
            NULLIFZERO(txn.high_value_txn_count)     AS high_value_txn_count,
            -- Latest balance
            ZEROIFNULL(bal.closing_balance)           AS latest_balance,
            -- Deterministic hash bucket for model segmentation
            HASHBUCKET(HASHROW(c.customer_id)) MOD 10 AS model_segment
        FROM BARCLAYS_STG.V_CUSTOMER_LATEST c

        LEFT JOIN (
            SELECT
                customer_id,
                COUNT(*)                                       AS total_accounts,
                SUM(CASE WHEN status = 'ACTIVE' THEN 1 ELSE 0 END) AS active_accounts,
                SUM(ZEROIFNULL(credit_limit))                  AS total_credit_limit,
                SUM(ZEROIFNULL(overdraft_limit))               AS total_overdraft
            FROM BARCLAYS_STG.V_ACCOUNT_CURRENT
            GROUP BY customer_id
        ) acct ON c.customer_id = acct.customer_id

        LEFT JOIN (
            SELECT
                a.customer_id,
                COUNT(*)                                                    AS txn_count_90d,
                SUM(CASE WHEN t.signed_amount < 0
                    THEN ABS(t.signed_amount) ELSE 0 END)                  AS total_debit_90d,
                SUM(CASE WHEN t.signed_amount > 0
                    THEN t.signed_amount ELSE 0 END)                       AS total_credit_90d,
                AVG(t.amount)                                              AS avg_txn_amount_90d,
                MAX(t.amount)                                              AS max_txn_amount_90d,
                SUM(CASE WHEN t.value_band = 'HIGH_VALUE' THEN 1 ELSE 0 END) AS high_value_txn_count
            FROM BARCLAYS_STG.V_TRANSACTION_ENRICHED t
            INNER JOIN BARCLAYS_RAW.ACCOUNT a ON t.account_id = a.account_id
            WHERE t.transaction_date BETWEEN p_assessment_date - 90 AND p_assessment_date
            GROUP BY a.customer_id
        ) txn ON c.customer_id = txn.customer_id

        LEFT JOIN (
            SELECT
                account_sk,
                closing_balance
            FROM BARCLAYS_DWH.FCT_DAILY_BALANCE
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY account_sk
                ORDER BY date_key DESC
            ) = 1
        ) bal ON bal.account_sk IN (
            SELECT account_sk FROM BARCLAYS_DWH.DIM_ACCOUNT
            WHERE customer_id = c.customer_id AND is_current = 'Y'
        )
    ) WITH DATA
    ON COMMIT PRESERVE ROWS;

    ---------------------------------------------------------------------------
    -- Calculate risk scores and insert into mart
    ---------------------------------------------------------------------------
    DELETE FROM BARCLAYS_MART.MART_CREDIT_RISK
    WHERE assessment_date = p_assessment_date;

    INSERT INTO BARCLAYS_MART.MART_CREDIT_RISK (
        customer_id, assessment_date, risk_rating,
        probability_default, loss_given_default, exposure_at_default,
        risk_weighted_asset, expected_loss, unexpected_loss,
        risk_weight_pct, asset_class, model_version,
        etl_batch_id, etl_loaded_ts
    )
    SELECT
        rf.customer_id,
        p_assessment_date,
        -- Derived risk rating
        CASE
            WHEN rf.kyc_status = 'EXPIRED'                        THEN 'H'
            WHEN NULLIFZERO(rf.high_value_txn_count) IS NOT NULL
                 AND rf.tenure_days < 365                         THEN 'H'
            WHEN rf.total_debit_90d > rf.total_credit_limit * 0.8 THEN 'H'
            WHEN rf.txn_count_90d < 3 AND rf.tenure_days > 730   THEN 'M'
            WHEN rf.latest_balance < 0                            THEN 'M'
            ELSE 'L'
        END AS risk_rating,

        -- Probability of Default (PD) - simplified model
        CASE
            WHEN rf.kyc_status = 'EXPIRED'                        THEN 0.05
            WHEN rf.tenure_days < 365                             THEN 0.03
            WHEN rf.latest_balance < 0                            THEN 0.02
            ELSE 0.005
        END AS probability_default,

        -- Loss Given Default (LGD) - asset class dependent
        CASE rf.segment
            WHEN 'RETAIL'    THEN 0.45
            WHEN 'WEALTH'    THEN 0.30
            WHEN 'CORPORATE' THEN 0.40
            ELSE 0.45
        END AS loss_given_default,

        -- Exposure at Default (EAD)
        ZEROIFNULL(rf.total_credit_limit) + ZEROIFNULL(rf.total_overdraft) AS exposure_at_default,

        -- Risk Weighted Asset (RWA) = EAD * Risk Weight
        (ZEROIFNULL(rf.total_credit_limit) + ZEROIFNULL(rf.total_overdraft))
            * CASE rf.segment
                WHEN 'RETAIL'    THEN 0.75
                WHEN 'WEALTH'    THEN 0.50
                WHEN 'CORPORATE' THEN 1.00
                ELSE 0.75
            END AS risk_weighted_asset,

        -- Expected Loss = PD * LGD * EAD
        CASE
            WHEN rf.kyc_status = 'EXPIRED' THEN 0.05
            WHEN rf.tenure_days < 365      THEN 0.03
            ELSE 0.005
        END
        * CASE rf.segment WHEN 'RETAIL' THEN 0.45 WHEN 'WEALTH' THEN 0.30 ELSE 0.40 END
        * (ZEROIFNULL(rf.total_credit_limit) + ZEROIFNULL(rf.total_overdraft))
            AS expected_loss,

        0 AS unexpected_loss,  -- placeholder

        -- Risk weight percentage
        CASE rf.segment
            WHEN 'RETAIL'    THEN 75.00
            WHEN 'WEALTH'    THEN 50.00
            WHEN 'CORPORATE' THEN 100.00
            ELSE 75.00
        END AS risk_weight_pct,

        -- Asset class
        CASE rf.segment
            WHEN 'RETAIL'    THEN 'RETAIL_OTHER'
            WHEN 'WEALTH'    THEN 'RETAIL_REVOLVING'
            WHEN 'CORPORATE' THEN 'CORPORATE'
            ELSE 'RETAIL_OTHER'
        END AS asset_class,

        'BRCL_RISK_v2.1' AS model_version,
        v_batch_id,
        CURRENT_TIMESTAMP(6)
    FROM vt_risk_factors rf;

    SET v_row_count = ACTIVITY_COUNT;

    -- Cleanup
    DROP TABLE vt_risk_factors;

    -- Collect statistics
    COLLECT STATISTICS ON BARCLAYS_MART.MART_CREDIT_RISK
        COLUMN (customer_id, assessment_date);

END;
