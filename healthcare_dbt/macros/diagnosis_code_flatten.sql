/*
 * Macro: Diagnosis Code Flattening
 *
 * Provides utilities for working with diagnosis codes across the unified model.
 * Handles the structural drift (DRIFT-003) where:
 *   - Teradata: 25 individual columns (icd_diagnosis_code_1..25)
 *   - Databricks: ARRAY<STRING> (diagnosis_codes)
 *   - Snowflake: Hybrid VARIANT JSON + 10 individual columns
 *
 * In the unified model, diagnosis codes are stored as 25 individual columns
 * (diagnosis_code_1..25) to match the Tuva input layer expectations.
 * This macro provides a way to flatten them into rows for analytics.
 *
 * Migration source: teradata/claims/stored_procedures/sp_diagnosis_code_flatten.sql
 *                   databricks/notebooks/04_diagnosis_code_processing.py
 *                   snowflake/stored_procedures/sp_diagnosis_code_flatten.sql
 */

{% macro flatten_diagnosis_codes(relation, claim_id_col='claim_id', claim_line_col='claim_line_number') %}
    /*
     * Unpivots diagnosis_code_1..25 into rows with (claim_id, claim_line_number,
     * diagnosis_position, diagnosis_code, diagnosis_code_type).
     *
     * Uses UNION ALL across all 25 positions, filtering out NULLs.
     */
    {% for i in range(1, 26) %}
    select
        {{ claim_id_col }},
        {{ claim_line_col }},
        {{ i }}                                             as diagnosis_position,
        diagnosis_code_{{ i }}                              as diagnosis_code,
        diagnosis_code_type
    from {{ relation }}
    where diagnosis_code_{{ i }} is not null
    {% if not loop.last %}union all{% endif %}
    {% endfor %}
{% endmacro %}


{% macro count_diagnosis_codes(prefix='diagnosis_code_') %}
    /*
     * Counts the number of non-null diagnosis codes for a claim row.
     * Useful for data quality checks and analytics.
     */
    (
        {% for i in range(1, 26) %}
        case when {{ prefix }}{{ i }} is not null then 1 else 0 end
        {% if not loop.last %} + {% endif %}
        {% endfor %}
    )
{% endmacro %}
