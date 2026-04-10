/*******************************************************************************
 * Healthcare Claims - Encounter Grouping (Gap-and-Island Overlap Detection)
 *
 * Groups overlapping or contiguous medical claims into encounters (episodes
 * of care). Uses Snowflake SQL with proper window functions.
 *
 * Algorithm: CORRECT gap-and-island overlap detection
 *   - Same as Databricks (proper overlap detection)
 *   - Different from Teradata (which used simplified date-range grouping)
 *
 * Uses CONDITIONAL_TRUE_EVENT-style logic via window functions to detect
 * when a new island (encounter) begins.
 ******************************************************************************/

CREATE OR REPLACE PROCEDURE CLAIMS_DW.WAREHOUSE.SP_ENCOUNTER_GROUPING(
    P_START_DATE     DATE,         -- Process claims from this date
    P_END_DATE       DATE          -- Process claims through this date
)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
COMMENT = 'Groups overlapping/contiguous claims into encounters using gap-and-island detection'
AS
$$
DECLARE
    v_rows_inserted INTEGER DEFAULT 0;
    v_result        VARCHAR DEFAULT '';
BEGIN

    -- =========================================================================
    -- Step 1: Build encounters using gap-and-island overlap detection
    --
    -- Logic:
    --   1. Order claims by member + start_date
    --   2. Track running max of end_date within each member
    --   3. When start_date > previous running max end_date, a new island begins
    --   4. Use CONDITIONAL_TRUE_EVENT pattern to assign group IDs
    -- =========================================================================

    -- Create temporary table for encounter assignments
    CREATE OR REPLACE TEMPORARY TABLE CLAIMS_DW.WAREHOUSE.TMP_ENCOUNTER_GROUPS AS
    WITH ordered_claims AS (
        -- Get all paid/adjusted claims in the date range
        SELECT
            claim_id,
            claim_line_number,
            member_id,
            start_date,
            COALESCE(end_date, start_date)              AS end_date,
            rendering_provider_id,
            facility_id,
            claim_type,
            place_of_service,
            drg_code,
            icd_diagnosis_code_1                        AS primary_diagnosis_code,
            diagnosis_codes,
            billed_amount,
            allowed_amount,
            paid_amount,
            net_paid_amount,
            copay_amount,
            coinsurance_amount,
            deductible_amount,
            source_system
        FROM CLAIMS_DW.STAGING.V_MEDICAL_CLAIM_CURRENT
        WHERE start_date BETWEEN :P_START_DATE AND :P_END_DATE
          AND UPPER(status_code) IN ('PAID', 'ADJUSTED')
    ),
    with_prev_end AS (
        -- Track the maximum end_date seen so far (running max)
        SELECT
            *,
            MAX(end_date) OVER (
                PARTITION BY member_id
                ORDER BY start_date, end_date
                ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
            )                                           AS prev_max_end_date
        FROM ordered_claims
    ),
    island_starts AS (
        -- Detect new island: start_date is after previous running max end_date
        -- First claim per member always starts a new island (prev_max_end_date IS NULL)
        SELECT
            *,
            CASE
                WHEN prev_max_end_date IS NULL THEN 1
                WHEN start_date > prev_max_end_date     THEN 1
                ELSE 0
            END                                         AS is_new_island
        FROM with_prev_end
    ),
    island_groups AS (
        -- Assign encounter group ID using cumulative sum of island starts
        -- This is the CONDITIONAL_TRUE_EVENT pattern
        SELECT
            *,
            SUM(is_new_island) OVER (
                PARTITION BY member_id
                ORDER BY start_date, end_date
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            )                                           AS encounter_group_id
        FROM island_starts
    ),
    -- Pick first-claim attributes per encounter using ROW_NUMBER
    first_claim_attrs AS (
        SELECT
            member_id,
            encounter_group_id,
            rendering_provider_id,
            facility_id,
            primary_diagnosis_code,
            place_of_service,
            ROW_NUMBER() OVER (
                PARTITION BY member_id, encounter_group_id
                ORDER BY start_date, claim_id
            )                                           AS rn
        FROM island_groups
    ),
    encounter_first AS (
        SELECT member_id, encounter_group_id,
               rendering_provider_id, facility_id,
               primary_diagnosis_code, place_of_service
        FROM first_claim_attrs
        WHERE rn = 1
    ),
    -- Aggregate claims into encounters (GROUP BY only member + encounter group)
    encounter_agg AS (
        SELECT
            member_id,
            encounter_group_id,
            member_id || '-' || TO_CHAR(MIN(start_date), 'YYYYMMDD') || '-' ||
                LPAD(encounter_group_id::VARCHAR, 4, '0')   AS encounter_id,
            MIN(start_date)                                 AS encounter_start_date,
            MAX(end_date)                                   AS encounter_end_date,
            -- Determine encounter type from place of service and claim type
            CASE
                WHEN MAX(CASE WHEN claim_type = 'I' AND place_of_service = '21' THEN 1 ELSE 0 END) = 1
                    THEN 'INPATIENT'
                WHEN MAX(CASE WHEN place_of_service = '23' THEN 1 ELSE 0 END) = 1
                    THEN 'ED'
                WHEN MAX(CASE WHEN claim_type = 'I' THEN 1 ELSE 0 END) = 1
                    THEN 'OUTPATIENT'
                WHEN MAX(CASE WHEN place_of_service = '02' THEN 1 ELSE 0 END) = 1
                    THEN 'TELEHEALTH'
                ELSE 'OFFICE_VISIT'
            END                                             AS encounter_type,
            -- DRG from institutional claims
            MAX(drg_code)                                   AS drg_code,
            -- Aggregate diagnosis codes into a VARIANT array
            ARRAY_AGG(DISTINCT primary_diagnosis_code)
                WITHIN GROUP (ORDER BY primary_diagnosis_code)
                                                            AS diagnosis_codes,
            -- Measures
            COUNT(DISTINCT claim_id)                        AS total_claim_lines,
            SUM(COALESCE(billed_amount, 0))                 AS total_billed_amount,
            SUM(COALESCE(allowed_amount, 0))                AS total_allowed_amount,
            SUM(COALESCE(paid_amount, 0))                   AS total_paid_amount,
            SUM(COALESCE(net_paid_amount, 0))               AS total_net_paid_amount,
            SUM(COALESCE(copay_amount, 0))
                + SUM(COALESCE(coinsurance_amount, 0))
                + SUM(COALESCE(deductible_amount, 0))       AS total_member_liability,
            DATEDIFF('day', MIN(start_date), MAX(end_date)) AS length_of_stay,
            MAX(source_system)                              AS source_system
        FROM island_groups
        GROUP BY member_id, encounter_group_id
    )
    -- Join aggregated metrics with first-claim attributes
    SELECT
        a.encounter_id,
        a.member_id,
        a.encounter_start_date,
        a.encounter_end_date,
        f.rendering_provider_id,
        f.facility_id,
        a.encounter_type,
        a.drg_code,
        f.primary_diagnosis_code,
        a.diagnosis_codes,
        f.place_of_service,
        a.total_claim_lines,
        a.total_billed_amount,
        a.total_allowed_amount,
        a.total_paid_amount,
        a.total_net_paid_amount,
        a.total_member_liability,
        a.length_of_stay,
        a.source_system,
        a.encounter_group_id
    FROM encounter_agg a
    INNER JOIN encounter_first f
        ON a.member_id = f.member_id
       AND a.encounter_group_id = f.encounter_group_id;

    -- =========================================================================
    -- Step 2: Merge encounters into the fact table
    -- =========================================================================
    MERGE INTO CLAIMS_DW.WAREHOUSE.FCT_ENCOUNTER AS tgt
    USING CLAIMS_DW.WAREHOUSE.TMP_ENCOUNTER_GROUPS AS src
    ON tgt.encounter_id = src.encounter_id
    WHEN MATCHED THEN
        UPDATE SET
            tgt.encounter_start_date    = src.encounter_start_date,
            tgt.encounter_end_date      = src.encounter_end_date,
            tgt.encounter_start_date_key = TO_NUMBER(TO_CHAR(src.encounter_start_date, 'YYYYMMDD')),
            tgt.encounter_end_date_key  = TO_NUMBER(TO_CHAR(src.encounter_end_date, 'YYYYMMDD')),
            tgt.encounter_type          = src.encounter_type,
            tgt.drg_code                = src.drg_code,
            tgt.primary_diagnosis_code  = src.primary_diagnosis_code,
            tgt.diagnosis_codes         = src.diagnosis_codes,
            tgt.place_of_service        = src.place_of_service,
            tgt.total_claim_lines       = src.total_claim_lines,
            tgt.total_billed_amount     = src.total_billed_amount,
            tgt.total_allowed_amount    = src.total_allowed_amount,
            tgt.total_paid_amount       = src.total_paid_amount,
            tgt.total_net_paid_amount   = src.total_net_paid_amount,
            tgt.total_member_liability  = src.total_member_liability,
            tgt.length_of_stay          = src.length_of_stay,
            tgt.etl_load_timestamp      = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN
        INSERT (
            encounter_id, member_id,
            facility_id,
            encounter_start_date, encounter_end_date,
            encounter_start_date_key, encounter_end_date_key,
            encounter_type, drg_code, primary_diagnosis_code, diagnosis_codes,
            place_of_service,
            total_claim_lines, total_billed_amount, total_allowed_amount,
            total_paid_amount, total_net_paid_amount, total_member_liability,
            length_of_stay, source_system, etl_load_timestamp
        )
        VALUES (
            src.encounter_id, src.member_id,
            src.facility_id,
            src.encounter_start_date, src.encounter_end_date,
            TO_NUMBER(TO_CHAR(src.encounter_start_date, 'YYYYMMDD')),
            TO_NUMBER(TO_CHAR(src.encounter_end_date, 'YYYYMMDD')),
            src.encounter_type, src.drg_code, src.primary_diagnosis_code, src.diagnosis_codes,
            src.place_of_service,
            src.total_claim_lines, src.total_billed_amount, src.total_allowed_amount,
            src.total_paid_amount, src.total_net_paid_amount, src.total_member_liability,
            src.length_of_stay, src.source_system, CURRENT_TIMESTAMP()
        );

    LET v_rows_inserted := SQLROWCOUNT;

    -- Clean up temp table
    DROP TABLE IF EXISTS CLAIMS_DW.WAREHOUSE.TMP_ENCOUNTER_GROUPS;

    v_result := 'Encounter grouping completed. Rows merged: ' || v_rows_inserted::VARCHAR;
    RETURN v_result;

EXCEPTION
    WHEN OTHER THEN
        DROP TABLE IF EXISTS CLAIMS_DW.WAREHOUSE.TMP_ENCOUNTER_GROUPS;
        RETURN 'ERROR in SP_ENCOUNTER_GROUPING: ' || SQLERRM;
END;
$$;
