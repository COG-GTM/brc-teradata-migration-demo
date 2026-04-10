/*******************************************************************************
 * Healthcare Claims - ADR (Adjustment/Denial/Reversal) Deduplication
 *
 * Snowflake JavaScript stored procedure for claim deduplication.
 *
 * IMPORTANT - ADR Priority (SNOWFLAKE-SPECIFIC - DIFFERENT FROM BOTH PLATFORMS):
 *   PAID     = 1   (highest priority)
 *   ADJUSTED = 2
 *   REVERSED = 3   << Reversed BEFORE Denied (unique to Snowflake)
 *   DENIED   = 4
 *
 *   Teradata:    PAID=1, ADJUSTED=2, DENIED=3, REVERSED=4
 *   Databricks:  PAID=1, DENIED=2, ADJUSTED=3, REVERSED=4
 *   Snowflake:   PAID=1, ADJUSTED=2, REVERSED=3, DENIED=4
 *
 * Uses Snowflake JavaScript stored procedure syntax.
 ******************************************************************************/

CREATE OR REPLACE PROCEDURE CLAIMS_DW.WAREHOUSE.SP_CLAIMS_ADR_DEDUP(
    P_CLAIM_CATEGORY VARCHAR,      -- 'MEDICAL' or 'PHARMACY'
    P_START_DATE     DATE,         -- Process claims from this date
    P_END_DATE       DATE          -- Process claims through this date
)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
COMMENT = 'ADR dedup: PAID=1, ADJUSTED=2, REVERSED=3, DENIED=4 (Snowflake-specific priority)'
AS
$$
    var result = '';
    var rowCount = 0;

    try {
        // =====================================================================
        // Step 1: Validate inputs
        // =====================================================================
        if (!P_CLAIM_CATEGORY || (P_CLAIM_CATEGORY !== 'MEDICAL' && P_CLAIM_CATEGORY !== 'PHARMACY')) {
            return 'ERROR: P_CLAIM_CATEGORY must be MEDICAL or PHARMACY';
        }

        var startTs = new Date().toISOString();
        result += 'ADR Dedup started at ' + startTs + ' for ' + P_CLAIM_CATEGORY + '\n';

        // =====================================================================
        // Step 2: Build dedup SQL based on claim category
        // =====================================================================
        var dedupSql = '';

        if (P_CLAIM_CATEGORY === 'MEDICAL') {
            // -----------------------------------------------------------------
            // MEDICAL claims: dedup by original_claim_id + claim_line_number
            // ADR Priority: PAID=1, ADJUSTED=2, REVERSED=3, DENIED=4
            // -----------------------------------------------------------------
            dedupSql = `
                MERGE INTO CLAIMS_DW.WAREHOUSE.FCT_MEDICAL_CLAIM AS tgt
                USING (
                    SELECT *
                    FROM CLAIMS_DW.STAGING.V_MEDICAL_CLAIM_CURRENT
                    WHERE start_date BETWEEN '${P_START_DATE}' AND '${P_END_DATE}'
                      AND status_code IN ('PAID', 'ADJUSTED', 'REVERSED', 'DENIED')
                ) AS src
                ON  tgt.claim_id = src.claim_id
                AND tgt.claim_line_number = src.claim_line_number
                WHEN MATCHED AND src.load_timestamp > tgt.etl_load_timestamp THEN
                    UPDATE SET
                        tgt.status_code         = src.status_code,
                        tgt.paid_amount         = src.paid_amount,
                        tgt.net_paid_amount     = src.net_paid_amount,
                        tgt.allowed_amount      = src.allowed_amount,
                        tgt.billed_amount       = src.billed_amount,
                        tgt.copay_amount        = src.copay_amount,
                        tgt.coinsurance_amount  = src.coinsurance_amount,
                        tgt.deductible_amount   = src.deductible_amount,
                        tgt.cob_amount          = src.cob_amount,
                        tgt.withhold_amount     = src.withhold_amount,
                        tgt.member_liability    = COALESCE(src.copay_amount, 0)
                                                + COALESCE(src.coinsurance_amount, 0)
                                                + COALESCE(src.deductible_amount, 0),
                        tgt.adjudication_date   = src.adjudication_date,
                        tgt.diagnosis_codes     = src.diagnosis_codes,
                        tgt.primary_diagnosis_code = src.icd_diagnosis_code_1,
                        tgt.etl_load_timestamp  = CURRENT_TIMESTAMP()
                WHEN NOT MATCHED THEN
                    INSERT (
                        claim_id, claim_line_number, member_id,
                        rendering_provider_key, billing_provider_key,
                        start_date_key, end_date_key,
                        admission_date_key, discharge_date_key, adjudication_date_key,
                        status_code, claim_type, place_of_service, bill_type,
                        cpt_code, cpt_modifier_1, revenue_code, drg_code,
                        diagnosis_codes, primary_diagnosis_code,
                        billed_amount, allowed_amount, paid_amount, net_paid_amount,
                        copay_amount, coinsurance_amount, deductible_amount,
                        cob_amount, member_liability, units,
                        source_system, etl_load_timestamp
                    )
                    VALUES (
                        src.claim_id, src.claim_line_number, src.member_id,
                        NULL, NULL,
                        TO_NUMBER(TO_CHAR(src.start_date, 'YYYYMMDD')),
                        TO_NUMBER(TO_CHAR(src.end_date, 'YYYYMMDD')),
                        TO_NUMBER(TO_CHAR(src.admission_date, 'YYYYMMDD')),
                        TO_NUMBER(TO_CHAR(src.discharge_date, 'YYYYMMDD')),
                        TO_NUMBER(TO_CHAR(src.adjudication_date, 'YYYYMMDD')),
                        src.status_code, src.claim_type, src.place_of_service, src.bill_type,
                        src.cpt_code, src.cpt_modifier_1, src.revenue_code, src.drg_code,
                        src.diagnosis_codes, src.icd_diagnosis_code_1,
                        src.billed_amount, src.allowed_amount, src.paid_amount, src.net_paid_amount,
                        src.copay_amount, src.coinsurance_amount, src.deductible_amount,
                        src.cob_amount,
                        COALESCE(src.copay_amount, 0) + COALESCE(src.coinsurance_amount, 0)
                            + COALESCE(src.deductible_amount, 0),
                        src.units,
                        src.source_system, CURRENT_TIMESTAMP()
                    )
            `;
        } else {
            // -----------------------------------------------------------------
            // PHARMACY claims: dedup by original_claim_id
            // Same ADR priority: PAID=1, ADJUSTED=2, REVERSED=3, DENIED=4
            // -----------------------------------------------------------------
            dedupSql = `
                MERGE INTO CLAIMS_DW.WAREHOUSE.FCT_PHARMACY_CLAIM AS tgt
                USING (
                    SELECT *
                    FROM CLAIMS_DW.STAGING.V_PHARMACY_CLAIM_CURRENT
                    WHERE fill_date BETWEEN '${P_START_DATE}' AND '${P_END_DATE}'
                      AND status_code IN ('PAID', 'ADJUSTED', 'REVERSED', 'DENIED')
                ) AS src
                ON  tgt.claim_id = src.claim_id
                WHEN MATCHED AND src.load_timestamp > tgt.etl_load_timestamp THEN
                    UPDATE SET
                        tgt.status_code         = src.status_code,
                        tgt.paid_amount         = src.paid_amount,
                        tgt.net_paid_amount     = src.net_paid_amount,
                        tgt.allowed_amount      = src.allowed_amount,
                        tgt.billed_amount       = src.billed_amount,
                        tgt.copay_amount        = src.copay_amount,
                        tgt.coinsurance_amount  = src.coinsurance_amount,
                        tgt.deductible_amount   = src.deductible_amount,
                        tgt.ingredient_cost     = src.ingredient_cost,
                        tgt.dispensing_fee       = src.dispensing_fee,
                        tgt.member_liability    = COALESCE(src.copay_amount, 0)
                                                + COALESCE(src.coinsurance_amount, 0)
                                                + COALESCE(src.deductible_amount, 0),
                        tgt.etl_load_timestamp  = CURRENT_TIMESTAMP()
                WHEN NOT MATCHED THEN
                    INSERT (
                        claim_id, member_id,
                        prescribing_provider_key,
                        fill_date_key, written_date_key,
                        status_code, ndc_code, drug_name, generic_name,
                        therapeutic_class_code, therapeutic_class_desc, gpi_code,
                        pharmacy_id, pharmacy_name, mail_order_flag,
                        daw_code, formulary_flag, prior_auth_flag,
                        quantity_dispensed, days_supply, refill_number,
                        billed_amount, allowed_amount, paid_amount, net_paid_amount,
                        copay_amount, coinsurance_amount, deductible_amount,
                        ingredient_cost, dispensing_fee, member_liability,
                        icd_diagnosis_code,
                        source_system, etl_load_timestamp
                    )
                    VALUES (
                        src.claim_id, src.member_id,
                        NULL,
                        TO_NUMBER(TO_CHAR(src.fill_date, 'YYYYMMDD')),
                        TO_NUMBER(TO_CHAR(src.written_date, 'YYYYMMDD')),
                        src.status_code, src.ndc_code, src.drug_name, src.generic_name,
                        src.therapeutic_class_code, src.therapeutic_class_desc, src.gpi_code,
                        src.pharmacy_id, src.pharmacy_name, src.mail_order_flag,
                        src.daw_code, src.formulary_flag, src.prior_auth_flag,
                        src.quantity_dispensed, src.days_supply, src.refill_number,
                        src.billed_amount, src.allowed_amount, src.paid_amount, src.net_paid_amount,
                        src.copay_amount, src.coinsurance_amount, src.deductible_amount,
                        src.ingredient_cost, src.dispensing_fee,
                        COALESCE(src.copay_amount, 0) + COALESCE(src.coinsurance_amount, 0)
                            + COALESCE(src.deductible_amount, 0),
                        src.icd_diagnosis_code,
                        src.source_system, CURRENT_TIMESTAMP()
                    )
            `;
        }

        // =====================================================================
        // Step 3: Execute the MERGE
        // =====================================================================
        var stmt = snowflake.createStatement({ sqlText: dedupSql });
        var rs = stmt.execute();
        rs.next();
        rowCount = stmt.getRowCount();

        result += 'MERGE completed: ' + rowCount + ' rows affected\n';

        // =====================================================================
        // Step 4: Log the execution
        // =====================================================================
        var logSql = `
            INSERT INTO CLAIMS_DW.RAW.ETL_EXECUTION_LOG (procedure_name, claim_category,
                start_date, end_date, rows_affected, status, message, execution_timestamp)
            SELECT 'SP_CLAIMS_ADR_DEDUP', '${P_CLAIM_CATEGORY}',
                '${P_START_DATE}', '${P_END_DATE}', ${rowCount}, 'SUCCESS',
                '${result.replace(/'/g, "''")}', CURRENT_TIMESTAMP()
        `;

        try {
            snowflake.execute({ sqlText: logSql });
        } catch (logErr) {
            // Log table may not exist yet - non-fatal
            result += 'WARNING: Could not write to execution log: ' + logErr.message + '\n';
        }

        var endTs = new Date().toISOString();
        result += 'ADR Dedup completed at ' + endTs;

        return result;

    } catch (err) {
        // =====================================================================
        // Error handling
        // =====================================================================
        var errMsg = 'ERROR in SP_CLAIMS_ADR_DEDUP: ' + err.message;

        try {
            var errLogSql = `
                INSERT INTO CLAIMS_DW.RAW.ETL_EXECUTION_LOG (procedure_name, claim_category,
                    start_date, end_date, rows_affected, status, message, execution_timestamp)
                SELECT 'SP_CLAIMS_ADR_DEDUP', '${P_CLAIM_CATEGORY}',
                    '${P_START_DATE}', '${P_END_DATE}', 0, 'FAILED',
                    '${errMsg.replace(/'/g, "''")}', CURRENT_TIMESTAMP()
            `;
            snowflake.execute({ sqlText: errLogSql });
        } catch (logErr) {
            // Swallow log errors
        }

        return errMsg;
    }
$$;
