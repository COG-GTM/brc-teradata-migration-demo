/*******************************************************************************
 * sp_aml_screening
 *
 * Anti-Money Laundering screening procedure using:
 *   - LIKE ANY / LIKE ALL patterns
 *   - SOUNDEX function for phonetic matching
 *   - Teradata string functions: OREPLACE, OTRANSLATE
 *   - VOLATILE TABLE creation within procedure
 *   - Pattern-based suspicious activity detection
 *
 ******************************************************************************/

REPLACE PROCEDURE BARCLAYS_DWH.sp_aml_screening (
    IN p_screening_date DATE
)
BEGIN
    DECLARE v_batch_id      BIGINT;
    DECLARE v_row_count     INTEGER;
    DECLARE v_sqlstate       CHAR(5);

    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        SET v_sqlstate = SQLSTATE;
        INSERT INTO BARCLAYS_DWH.ETL_ERROR_LOG (
            procedure_name, error_code, error_state, error_ts
        ) VALUES (
            'sp_aml_screening', SQLCODE, v_sqlstate, CURRENT_TIMESTAMP
        );
    END;

    SET v_batch_id = CAST(
        CAST(p_screening_date AS FORMAT 'YYYYMMDD') || '004' AS BIGINT
    );

    ---------------------------------------------------------------------------
    -- STEP 1: Build sanctions watchlist volatile table
    ---------------------------------------------------------------------------
    CREATE VOLATILE TABLE vt_sanctions_list (
        entity_name     VARCHAR(200)  CHARACTER SET LATIN NOT CASESPECIFIC,
        entity_type     VARCHAR(30),
        country_code    CHAR(2),
        list_source     VARCHAR(50),
        soundex_name    CHAR(4)
    )
    ON COMMIT PRESERVE ROWS;

    -- In production this would be loaded from an external sanctions feed
    INSERT INTO vt_sanctions_list VALUES ('KNOWN SANCTIONED ENTITY', 'CORPORATE', 'IR', 'OFAC', SOUNDEX('KNOWN SANCTIONED ENTITY'));
    INSERT INTO vt_sanctions_list VALUES ('SUSPICIOUS TRADING CO', 'CORPORATE', 'KP', 'UN', SOUNDEX('SUSPICIOUS TRADING CO'));
    INSERT INTO vt_sanctions_list VALUES ('HIGH RISK INDIVIDUAL', 'INDIVIDUAL', 'SY', 'HMT', SOUNDEX('HIGH RISK INDIVIDUAL'));

    ---------------------------------------------------------------------------
    -- STEP 2: Name screening using SOUNDEX and LIKE ANY
    ---------------------------------------------------------------------------
    CREATE VOLATILE TABLE vt_name_matches AS (
        SELECT
            cp.counterparty_id,
            cp.counterparty_name,
            sl.entity_name AS matched_entity,
            sl.list_source,
            -- Exact match score
            CASE
                WHEN UPPER(OREPLACE(OTRANSLATE(cp.counterparty_name, '.-,''', ''), '  ', ' '))
                   = UPPER(OREPLACE(OTRANSLATE(sl.entity_name, '.-,''', ''), '  ', ' '))
                THEN 100.0
                -- SOUNDEX phonetic match
                WHEN SOUNDEX(cp.counterparty_name) = sl.soundex_name
                THEN 75.0
                -- Partial name match using LIKE ANY
                WHEN cp.counterparty_name (NOT CASESPECIFIC) LIKE ANY (
                    '%' || TRIM(sl.entity_name) || '%',
                    '%' || TRIM(OREPLACE(sl.entity_name, ' ', '%')) || '%'
                )
                THEN 60.0
                ELSE 0.0
            END AS match_score
        FROM BARCLAYS_STG.V_COUNTERPARTY_SCREENED cp
        CROSS JOIN vt_sanctions_list sl
        WHERE cp.counterparty_name (NOT CASESPECIFIC) LIKE ANY (
            '%' || TRIM(sl.entity_name) || '%',
            '%' || TRIM(OREPLACE(sl.entity_name, ' ', '%')) || '%'
        )
        OR SOUNDEX(cp.counterparty_name) = sl.soundex_name
    ) WITH DATA
    ON COMMIT PRESERVE ROWS;

    ---------------------------------------------------------------------------
    -- STEP 3: Detect structuring (transactions just below reporting threshold)
    ---------------------------------------------------------------------------
    CREATE VOLATILE TABLE vt_structuring_alerts AS (
        SELECT
            a.customer_id,
            t.account_id,
            t.transaction_date,
            COUNT(*) AS txn_count,
            SUM(t.amount) AS total_amount,
            MAX(t.amount) AS max_single_txn,
            'STRUCTURING' AS alert_type
        FROM BARCLAYS_STG.V_TRANSACTION_ENRICHED t
        INNER JOIN BARCLAYS_RAW.ACCOUNT a ON t.account_id = a.account_id
        WHERE t.transaction_date BETWEEN p_screening_date - 7 AND p_screening_date
          AND t.amount BETWEEN 8000.00 AND 9999.99  -- Just below GBP 10k threshold
          AND t.transaction_type IN ('CREDIT', 'TRANSFER')
        GROUP BY a.customer_id, t.account_id, t.transaction_date
        HAVING COUNT(*) >= 3  -- 3+ structured transactions in a day
    ) WITH DATA
    ON COMMIT PRESERVE ROWS;

    ---------------------------------------------------------------------------
    -- STEP 4: Detect velocity breaches (unusual transaction frequency)
    ---------------------------------------------------------------------------
    CREATE VOLATILE TABLE vt_velocity_alerts AS (
        SELECT
            a.customer_id,
            t.account_id,
            t.transaction_date,
            COUNT(*) AS txn_count,
            SUM(t.amount) AS total_amount,
            'VELOCITY_BREACH' AS alert_type
        FROM BARCLAYS_STG.V_TRANSACTION_ENRICHED t
        INNER JOIN BARCLAYS_RAW.ACCOUNT a ON t.account_id = a.account_id
        WHERE t.transaction_date = p_screening_date
        GROUP BY a.customer_id, t.account_id, t.transaction_date
        HAVING COUNT(*) > 20  -- More than 20 transactions in a single day
            OR SUM(t.amount) > 100000.00  -- Or total exceeds GBP 100k
    ) WITH DATA
    ON COMMIT PRESERVE ROWS;

    ---------------------------------------------------------------------------
    -- STEP 5: Insert all alerts into mart
    ---------------------------------------------------------------------------
    DELETE FROM BARCLAYS_MART.MART_AML_ALERTS
    WHERE alert_date = p_screening_date
      AND etl_batch_id = v_batch_id;

    -- Name screening alerts
    INSERT INTO BARCLAYS_MART.MART_AML_ALERTS (
        customer_id, account_id, alert_date, alert_type, severity,
        match_score, matched_entity, alert_status,
        etl_batch_id, etl_loaded_ts
    )
    SELECT
        t.account_id,  -- placeholder: would join to get customer_id
        t.account_id,
        p_screening_date,
        'SANCTIONS_HIT',
        CASE
            WHEN nm.match_score >= 90 THEN 'CRITICAL'
            WHEN nm.match_score >= 70 THEN 'HIGH'
            ELSE 'MEDIUM'
        END,
        nm.match_score,
        nm.matched_entity,
        'OPEN',
        v_batch_id,
        CURRENT_TIMESTAMP(6)
    FROM vt_name_matches nm
    INNER JOIN BARCLAYS_RAW.TRANSACTION t
        ON t.counterparty_id = nm.counterparty_id
       AND t.transaction_date = p_screening_date;

    -- Structuring alerts
    INSERT INTO BARCLAYS_MART.MART_AML_ALERTS (
        customer_id, account_id, alert_date, alert_type, severity,
        match_score, matched_entity, alert_status,
        etl_batch_id, etl_loaded_ts
    )
    SELECT
        sa.customer_id,
        sa.account_id,
        p_screening_date,
        sa.alert_type,
        CASE
            WHEN sa.total_amount > 50000 THEN 'HIGH'
            WHEN sa.txn_count >= 5 THEN 'HIGH'
            ELSE 'MEDIUM'
        END,
        CAST(sa.txn_count AS DECIMAL(5,2)) * 10,  -- score based on count
        'STRUCTURING PATTERN DETECTED',
        'OPEN',
        v_batch_id,
        CURRENT_TIMESTAMP(6)
    FROM vt_structuring_alerts sa;

    -- Velocity alerts
    INSERT INTO BARCLAYS_MART.MART_AML_ALERTS (
        customer_id, account_id, alert_date, alert_type, severity,
        match_score, matched_entity, alert_status,
        etl_batch_id, etl_loaded_ts
    )
    SELECT
        va.customer_id,
        va.account_id,
        p_screening_date,
        va.alert_type,
        CASE
            WHEN va.total_amount > 500000 THEN 'CRITICAL'
            WHEN va.total_amount > 100000 THEN 'HIGH'
            ELSE 'MEDIUM'
        END,
        CAST(va.txn_count AS DECIMAL(5,2)),
        'VELOCITY BREACH - ' || TRIM(CAST(va.txn_count AS VARCHAR(10))) || ' TXNS',
        'OPEN',
        v_batch_id,
        CURRENT_TIMESTAMP(6)
    FROM vt_velocity_alerts va;

    SET v_row_count = ACTIVITY_COUNT;

    -- Cleanup
    DROP TABLE vt_sanctions_list;
    DROP TABLE vt_name_matches;
    DROP TABLE vt_structuring_alerts;
    DROP TABLE vt_velocity_alerts;

    COLLECT STATISTICS ON BARCLAYS_MART.MART_AML_ALERTS
        COLUMN (alert_date);

END;
