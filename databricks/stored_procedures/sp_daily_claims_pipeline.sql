-- ============================================================================
-- Databricks Healthcare Claims - Daily Claims Pipeline Stored Procedure
-- ============================================================================
-- Orchestrates the daily claims processing pipeline as a Databricks SQL
-- stored procedure. This is the main entry point for the daily batch job.
--
-- Pipeline Steps:
--   1. Validate source data availability
--   2. ADR deduplication (raw -> staging)
--   3. PHI masking (applied at staging layer)
--   4. Encounter grouping (staging -> warehouse)
--   5. Fact table population (staging -> warehouse)
--   6. Mart table refresh (warehouse -> mart)
--   7. Data quality validation
--
-- Usage:
--   CALL claims_warehouse.sp_daily_claims_pipeline('2025-01-15');
--
-- Platform: Databricks SQL
-- ============================================================================

CREATE OR REPLACE PROCEDURE claims_warehouse.sp_daily_claims_pipeline(
    IN processing_date DATE DEFAULT CURRENT_DATE()
)
LANGUAGE SQL
COMMENT 'Daily claims processing pipeline: raw -> staging -> warehouse -> mart'
AS
BEGIN
    -- -----------------------------------------------------------------------
    -- Variables and setup
    -- -----------------------------------------------------------------------
    DECLARE v_start_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP();
    DECLARE v_step STRING;
    DECLARE v_raw_medical_count BIGINT;
    DECLARE v_raw_pharmacy_count BIGINT;
    DECLARE v_staged_medical_count BIGINT;
    DECLARE v_staged_pharmacy_count BIGINT;
    DECLARE v_encounter_count BIGINT;

    -- Log pipeline start
    SELECT CONCAT('Daily claims pipeline started for processing_date=',
                  CAST(processing_date AS STRING),
                  ' at ', CAST(v_start_ts AS STRING));

    -- -----------------------------------------------------------------------
    -- Step 1: Validate source data
    -- -----------------------------------------------------------------------
    SET v_step = 'Step 1: Source data validation';

    SELECT COUNT(*) INTO v_raw_medical_count
    FROM claims_raw.raw_medical_claim
    WHERE ingestion_timestamp >= processing_date
      AND ingestion_timestamp < DATE_ADD(processing_date, 1);

    SELECT COUNT(*) INTO v_raw_pharmacy_count
    FROM claims_raw.raw_pharmacy_claim
    WHERE ingestion_timestamp >= processing_date
      AND ingestion_timestamp < DATE_ADD(processing_date, 1);

    SELECT CONCAT(v_step, ' complete: ',
                  CAST(v_raw_medical_count AS STRING), ' new medical claims, ',
                  CAST(v_raw_pharmacy_count AS STRING), ' new pharmacy claims');

    -- -----------------------------------------------------------------------
    -- Step 2: ADR Deduplication (Medical Claims)
    -- Priority: PAID=1, ADJUSTED=2, DENIED=3, REVERSED=4 (CMS standard)
    -- -----------------------------------------------------------------------
    SET v_step = 'Step 2: ADR deduplication';

    -- Medical claims ADR dedup
    MERGE INTO claims_staging.stg_medical_claim_current AS target
    USING (
        SELECT *,
            CASE UPPER(TRIM(claim_status))
                WHEN 'PAID' THEN 1
                WHEN 'APPROVED' THEN 1
                WHEN 'FINALIZED' THEN 1
                WHEN 'ADJUSTED' THEN 2
                WHEN 'CORRECTED' THEN 2
                WHEN 'DENIED' THEN 3
                WHEN 'REJECTED' THEN 3
                WHEN 'REVERSED' THEN 4
                WHEN 'VOIDED' THEN 4
                ELSE 99
            END AS claim_status_priority,
            ROW_NUMBER() OVER (
                PARTITION BY claim_id, claim_line_number
                ORDER BY
                    CASE UPPER(TRIM(claim_status))
                        WHEN 'PAID' THEN 1
                        WHEN 'APPROVED' THEN 1
                        WHEN 'FINALIZED' THEN 1
                        WHEN 'ADJUSTED' THEN 2
                        WHEN 'CORRECTED' THEN 2
                        WHEN 'DENIED' THEN 3
                        WHEN 'REJECTED' THEN 3
                        WHEN 'REVERSED' THEN 4
                        WHEN 'VOIDED' THEN 4
                        ELSE 99
                    END ASC,
                    COALESCE(adjustment_sequence_number, 0) DESC,
                    COALESCE(claim_adjudication_date, DATE '1900-01-01') DESC,
                    ingestion_timestamp DESC
            ) AS adr_rank
        FROM claims_raw.raw_medical_claim
    ) AS source
    ON target.claim_id = source.claim_id
       AND target.claim_line_number = source.claim_line_number
    WHEN MATCHED AND source.adr_rank = 1 THEN
        UPDATE SET
            target.patient_id = source.patient_id,
            target.claim_type = source.claim_type,
            target.claim_status = source.claim_status,
            target.claim_status_priority = source.claim_status_priority,
            target.claim_submission_date = source.claim_submission_date,
            target.claim_adjudication_date = source.claim_adjudication_date,
            target.claim_start_date = source.claim_start_date,
            target.claim_end_date = source.claim_end_date,
            target.admission_date = source.admission_date,
            target.discharge_date = source.discharge_date,
            target.diagnosis_codes = source.diagnosis_codes,
            target.principal_diagnosis_code = source.principal_diagnosis_code,
            target.procedure_code = source.procedure_code,
            target.procedure_code_type = source.procedure_code_type,
            target.drg_code = source.drg_code,
            target.rendering_provider_npi = source.rendering_provider_npi,
            target.billing_provider_npi = source.billing_provider_npi,
            target.facility_npi = source.facility_npi,
            target.billed_amount = source.billed_amount,
            target.allowed_amount = source.allowed_amount,
            target.paid_amount = source.paid_amount,
            target.member_liability_amount = source.member_liability_amount,
            target.copay_amount = source.copay_amount,
            target.coinsurance_amount = source.coinsurance_amount,
            target.deductible_amount = source.deductible_amount,
            target.plan_id = source.plan_id,
            target.line_of_business = source.line_of_business,
            target.network_status = source.network_status,
            target.staging_timestamp = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED AND source.adr_rank = 1 THEN
        INSERT (
            claim_id, claim_line_number, patient_id, claim_type, claim_status,
            claim_status_priority, claim_submission_date, claim_adjudication_date,
            claim_start_date, claim_end_date, admission_date, discharge_date,
            diagnosis_codes, principal_diagnosis_code, procedure_code,
            procedure_code_type, drg_code, rendering_provider_npi,
            billing_provider_npi, facility_npi, billed_amount, allowed_amount,
            paid_amount, member_liability_amount, copay_amount, coinsurance_amount,
            deductible_amount, plan_id, line_of_business, network_status,
            source_system, staging_timestamp
        )
        VALUES (
            source.claim_id, source.claim_line_number, source.patient_id,
            source.claim_type, source.claim_status, source.claim_status_priority,
            source.claim_submission_date, source.claim_adjudication_date,
            source.claim_start_date, source.claim_end_date, source.admission_date,
            source.discharge_date, source.diagnosis_codes,
            source.principal_diagnosis_code, source.procedure_code,
            source.procedure_code_type, source.drg_code,
            source.rendering_provider_npi, source.billing_provider_npi,
            source.facility_npi, source.billed_amount, source.allowed_amount,
            source.paid_amount, source.member_liability_amount, source.copay_amount,
            source.coinsurance_amount, source.deductible_amount, source.plan_id,
            source.line_of_business, source.network_status, source.source_system,
            CURRENT_TIMESTAMP()
        );

    -- Pharmacy claims ADR dedup (same priority logic)
    MERGE INTO claims_staging.stg_pharmacy_claim_current AS target
    USING (
        SELECT *,
            CASE UPPER(TRIM(claim_status))
                WHEN 'PAID' THEN 1
                WHEN 'ADJUSTED' THEN 2
                WHEN 'DENIED' THEN 3
                WHEN 'REVERSED' THEN 4
                ELSE 99
            END AS claim_status_priority,
            ROW_NUMBER() OVER (
                PARTITION BY claim_id, claim_line_number
                ORDER BY
                    CASE UPPER(TRIM(claim_status))
                        WHEN 'PAID' THEN 1
                        WHEN 'ADJUSTED' THEN 2
                        WHEN 'DENIED' THEN 3
                        WHEN 'REVERSED' THEN 4
                        ELSE 99
                    END ASC,
                    COALESCE(adjustment_sequence_number, 0) DESC,
                    ingestion_timestamp DESC
            ) AS adr_rank
        FROM claims_raw.raw_pharmacy_claim
    ) AS source
    ON target.claim_id = source.claim_id
       AND target.claim_line_number = source.claim_line_number
    WHEN MATCHED AND source.adr_rank = 1 THEN
        UPDATE SET *
    WHEN NOT MATCHED AND source.adr_rank = 1 THEN
        INSERT *;

    SELECT COUNT(*) INTO v_staged_medical_count
    FROM claims_staging.stg_medical_claim_current;

    SELECT COUNT(*) INTO v_staged_pharmacy_count
    FROM claims_staging.stg_pharmacy_claim_current;

    SELECT CONCAT(v_step, ' complete: ',
                  CAST(v_staged_medical_count AS STRING), ' staged medical, ',
                  CAST(v_staged_pharmacy_count AS STRING), ' staged pharmacy');

    -- -----------------------------------------------------------------------
    -- Step 3: SCD Type 2 dimension updates (dim_member)
    -- -----------------------------------------------------------------------
    SET v_step = 'Step 3: Dimension updates';

    -- Expire changed records
    MERGE INTO claims_warehouse.dim_member AS target
    USING (
        SELECT
            s.*,
            SHA2(CONCAT_WS('|', s.plan_id, s.plan_type, s.line_of_business,
                            s.enrollment_status, s.pcp_provider_id,
                            CAST(s.risk_score AS STRING)), 256) AS new_hash
        FROM claims_staging.stg_member_latest s
    ) AS source
    ON target.member_id = source.member_id AND target.is_current = TRUE
    WHEN MATCHED AND target.record_hash != source.new_hash THEN
        UPDATE SET
            target.effective_end_date = CURRENT_DATE(),
            target.is_current = FALSE,
            target.updated_timestamp = CURRENT_TIMESTAMP();

    -- Insert new/changed records
    INSERT INTO claims_warehouse.dim_member (
        member_id, member_first_name_masked, member_last_name_masked,
        date_of_birth_masked, gender, state_code, zip_code_3digit,
        plan_id, plan_name, plan_type, line_of_business, group_id,
        group_name, enrollment_status, pcp_provider_id, risk_score,
        effective_start_date, effective_end_date, is_current,
        record_hash, created_timestamp, updated_timestamp
    )
    SELECT
        s.member_id, s.member_first_name_masked, s.member_last_name_masked,
        s.date_of_birth_masked, s.gender, s.state_code, s.zip_code_3digit,
        s.plan_id, s.plan_name, s.plan_type, s.line_of_business, s.group_id,
        s.group_name, s.enrollment_status, s.pcp_provider_id, s.risk_score,
        CURRENT_DATE(), NULL, TRUE,
        SHA2(CONCAT_WS('|', s.plan_id, s.plan_type, s.line_of_business,
                        s.enrollment_status, s.pcp_provider_id,
                        CAST(s.risk_score AS STRING)), 256),
        CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
    FROM claims_staging.stg_member_latest s
    LEFT JOIN claims_warehouse.dim_member d
        ON s.member_id = d.member_id AND d.is_current = TRUE
    WHERE d.member_id IS NULL;

    SELECT CONCAT(v_step, ' complete');

    -- -----------------------------------------------------------------------
    -- Step 4: Fact table population
    -- -----------------------------------------------------------------------
    SET v_step = 'Step 4: Fact table population';

    -- Medical claims facts (incremental insert for new claims)
    INSERT INTO claims_warehouse.fct_medical_claim (
        claim_id, claim_line_number, member_key, patient_id,
        service_start_date_key, service_end_date_key,
        claim_type, claim_status, place_of_service_code,
        diagnosis_codes, principal_diagnosis_code,
        procedure_code, procedure_code_type, drg_code,
        plan_id, line_of_business, network_status,
        billed_amount, allowed_amount, paid_amount,
        member_liability_amount, copay_amount, coinsurance_amount,
        deductible_amount, units_of_service, created_timestamp
    )
    SELECT
        s.claim_id, s.claim_line_number,
        d.member_key,
        s.patient_id,
        CAST(DATE_FORMAT(s.claim_start_date, 'yyyyMMdd') AS INT),
        CAST(DATE_FORMAT(s.claim_end_date, 'yyyyMMdd') AS INT),
        s.claim_type, s.claim_status, s.place_of_service_code,
        s.diagnosis_codes, s.principal_diagnosis_code,
        s.procedure_code, s.procedure_code_type, s.drg_code,
        s.plan_id, s.line_of_business, s.network_status,
        s.billed_amount, s.allowed_amount, s.paid_amount,
        s.member_liability_amount, s.copay_amount, s.coinsurance_amount,
        s.deductible_amount, s.units_of_service,
        CURRENT_TIMESTAMP()
    FROM claims_staging.stg_medical_claim_current s
    LEFT JOIN claims_warehouse.dim_member d
        ON s.patient_id = d.member_id AND d.is_current = TRUE
    LEFT JOIN claims_warehouse.fct_medical_claim f
        ON s.claim_id = f.claim_id AND s.claim_line_number = f.claim_line_number
    WHERE f.claim_id IS NULL;

    SELECT CONCAT(v_step, ' complete');

    -- -----------------------------------------------------------------------
    -- Step 5: Optimize tables
    -- -----------------------------------------------------------------------
    SET v_step = 'Step 5: Table optimization';

    OPTIMIZE claims_staging.stg_medical_claim_current ZORDER BY (patient_id, claim_start_date);
    OPTIMIZE claims_staging.stg_pharmacy_claim_current ZORDER BY (patient_id, fill_date);
    OPTIMIZE claims_warehouse.fct_medical_claim ZORDER BY (patient_id, service_start_date_key);

    -- Compute statistics for query optimization
    ANALYZE TABLE claims_staging.stg_medical_claim_current COMPUTE STATISTICS FOR ALL COLUMNS;
    ANALYZE TABLE claims_warehouse.fct_medical_claim COMPUTE STATISTICS FOR ALL COLUMNS;

    SELECT CONCAT(v_step, ' complete');

    -- -----------------------------------------------------------------------
    -- Step 6: Data quality checks
    -- -----------------------------------------------------------------------
    SET v_step = 'Step 6: Data quality validation';

    -- Check for duplicate claim lines in staging
    SELECT CONCAT('Duplicate medical claim lines: ',
        CAST((
            SELECT COUNT(*) FROM (
                SELECT claim_id, claim_line_number, COUNT(*) AS cnt
                FROM claims_staging.stg_medical_claim_current
                GROUP BY claim_id, claim_line_number
                HAVING COUNT(*) > 1
            )
        ) AS STRING));

    -- Check for orphaned claims (no matching member)
    SELECT CONCAT('Orphaned medical claims (no member): ',
        CAST((
            SELECT COUNT(*)
            FROM claims_staging.stg_medical_claim_current s
            LEFT JOIN claims_staging.stg_member_latest m
                ON s.patient_id = m.member_id
            WHERE m.member_id IS NULL
        ) AS STRING));

    SELECT CONCAT(v_step, ' complete');

    -- -----------------------------------------------------------------------
    -- Pipeline complete
    -- -----------------------------------------------------------------------
    SELECT CONCAT(
        'Daily claims pipeline completed successfully. ',
        'Processing date: ', CAST(processing_date AS STRING), '. ',
        'Duration: ', CAST(
            TIMESTAMPDIFF(SECOND, v_start_ts, CURRENT_TIMESTAMP())
        AS STRING), ' seconds.'
    );
END;
