/*******************************************************************************
 * sp_regulatory_capital_calc
 *
 * Basel III regulatory capital calculation:
 *   - Complex joins across risk, account, and market data
 *   - Teradata OLAP functions: CSUM, MAVG, MDIFF
 *   - NORMALIZE ON for period normalization
 *   - GROUP BY ROLLUP for multi-level aggregation
 *
 ******************************************************************************/

REPLACE PROCEDURE BARCLAYS_DWH.sp_regulatory_capital_calc (
    IN p_reporting_date DATE
)
BEGIN
    DECLARE v_batch_id      BIGINT;
    DECLARE v_row_count     INTEGER;
    DECLARE v_sqlstate       CHAR(5);
    DECLARE v_min_capital_ratio DECIMAL(10,6) DEFAULT 0.08;  -- Basel III 8% minimum
    DECLARE v_ccb_rate       DECIMAL(10,6) DEFAULT 0.025;    -- 2.5% conservation buffer
    DECLARE v_systemic_rate  DECIMAL(10,6) DEFAULT 0.01;     -- 1% G-SIB buffer

    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        SET v_sqlstate = SQLSTATE;
        INSERT INTO BARCLAYS_DWH.ETL_ERROR_LOG (
            procedure_name, error_code, error_state, error_ts
        ) VALUES (
            'sp_regulatory_capital_calc', SQLCODE, v_sqlstate, CURRENT_TIMESTAMP
        );
    END;

    SET v_batch_id = CAST(
        CAST(p_reporting_date AS FORMAT 'YYYYMMDD') || '003' AS BIGINT
    );

    ---------------------------------------------------------------------------
    -- STEP 1: Build exposure summary with OLAP trending
    ---------------------------------------------------------------------------
    CREATE VOLATILE TABLE vt_exposure_trend AS (
        SELECT
            cr.asset_class,
            cr.assessment_date,
            SUM(cr.exposure_at_default) AS total_ead,
            SUM(cr.risk_weighted_asset) AS total_rwa,
            SUM(cr.expected_loss)       AS total_el,
            COUNT(DISTINCT cr.customer_id) AS customer_count,
            -- Teradata OLAP: cumulative sum of RWA over time
            CSUM(SUM(cr.risk_weighted_asset), cr.assessment_date)
                AS cumulative_rwa,
            -- Teradata OLAP: 3-month moving average of RWA
            MAVG(SUM(cr.risk_weighted_asset), 3, cr.assessment_date)
                AS ma3_rwa,
            -- Teradata OLAP: month-over-month difference
            MDIFF(SUM(cr.risk_weighted_asset), 1, cr.assessment_date)
                AS mom_rwa_change
        FROM BARCLAYS_MART.MART_CREDIT_RISK cr
        WHERE cr.assessment_date BETWEEN ADD_MONTHS(p_reporting_date, -12)
                                     AND p_reporting_date
        GROUP BY cr.asset_class, cr.assessment_date
    ) WITH DATA
    ON COMMIT PRESERVE ROWS;

    ---------------------------------------------------------------------------
    -- STEP 2: Period normalization for overlapping exposures
    ---------------------------------------------------------------------------
    /*
     * NORMALIZE ON merges overlapping or adjacent periods into a single row.
     * This is used to deduplicate exposure windows for customers with
     * multiple accounts whose risk periods overlap.
     */
    CREATE VOLATILE TABLE vt_normalized_exposure AS (
        SELECT
            dc.customer_id,
            dc.segment,
            NORMALIZE ON dc.validity_period AS normalized_period,
            MAX(cr.exposure_at_default) AS peak_exposure,
            MAX(cr.risk_weighted_asset) AS peak_rwa
        FROM BARCLAYS_DWH.DIM_CUSTOMER dc
        INNER JOIN BARCLAYS_MART.MART_CREDIT_RISK cr
            ON dc.customer_id = cr.customer_id
           AND cr.assessment_date = p_reporting_date
        WHERE dc.validity_period OVERLAPS PERIOD(p_reporting_date, p_reporting_date + 1)
        GROUP BY dc.customer_id, dc.segment, NORMALIZE ON dc.validity_period
    ) WITH DATA
    ON COMMIT PRESERVE ROWS;

    ---------------------------------------------------------------------------
    -- STEP 3: Calculate regulatory capital with GROUP BY ROLLUP
    ---------------------------------------------------------------------------
    DELETE FROM BARCLAYS_MART.MART_REGULATORY_CAPITAL
    WHERE reporting_date = p_reporting_date;

    INSERT INTO BARCLAYS_MART.MART_REGULATORY_CAPITAL (
        reporting_date, asset_class,
        total_exposure, total_rwa, capital_required, capital_ratio,
        tier1_capital, tier2_capital, total_capital,
        leverage_ratio, countercyclical_buf, systemic_buf,
        rollup_level, etl_batch_id, etl_loaded_ts
    )
    SELECT
        p_reporting_date,
        COALESCE(et.asset_class, 'ALL_CLASSES')     AS asset_class,
        SUM(et.total_ead)                            AS total_exposure,
        SUM(et.total_rwa)                            AS total_rwa,
        SUM(et.total_rwa) * v_min_capital_ratio      AS capital_required,
        -- Capital ratio (assuming fixed capital base for demo)
        CASE
            WHEN NULLIFZERO(SUM(et.total_rwa)) IS NOT NULL
            THEN 50000000000.00 / SUM(et.total_rwa)  -- GBP 50bn capital base
            ELSE 0
        END                                          AS capital_ratio,
        50000000000.00 * 0.70                        AS tier1_capital,   -- 70% Tier 1
        50000000000.00 * 0.30                        AS tier2_capital,   -- 30% Tier 2
        50000000000.00                               AS total_capital,
        -- Leverage ratio = Tier1 / Total Exposure
        CASE
            WHEN NULLIFZERO(SUM(et.total_ead)) IS NOT NULL
            THEN (50000000000.00 * 0.70) / SUM(et.total_ead)
            ELSE 0
        END                                          AS leverage_ratio,
        v_ccb_rate                                   AS countercyclical_buf,
        v_systemic_rate                              AS systemic_buf,
        CASE
            WHEN GROUPING(et.asset_class) = 1 THEN 'TOTAL'
            ELSE 'ASSET_CLASS'
        END                                          AS rollup_level,
        v_batch_id,
        CURRENT_TIMESTAMP(6)
    FROM vt_exposure_trend et
    WHERE et.assessment_date = p_reporting_date
    GROUP BY ROLLUP (et.asset_class);

    SET v_row_count = ACTIVITY_COUNT;

    -- Cleanup
    DROP TABLE vt_exposure_trend;
    DROP TABLE vt_normalized_exposure;

    COLLECT STATISTICS ON BARCLAYS_MART.MART_REGULATORY_CAPITAL
        COLUMN (reporting_date, asset_class);

END;
