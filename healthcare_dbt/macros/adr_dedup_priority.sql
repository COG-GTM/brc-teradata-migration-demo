/*
 * Macro: ADR Deduplication Priority
 *
 * Implements the CMS-correct ADR (Adjustment/Denial/Reversal) priority order
 * for claim deduplication. This resolves DRIFT-001.
 *
 * Correct CMS priority (adopted from Databricks):
 *   PAID     = 1 (highest - final adjudicated payment)
 *   ADJUSTED = 2 (correction to a paid claim)
 *   DENIED   = 3 (claim denied by payer)
 *   REVERSED = 4 (lowest - claim voided)
 *
 * Legacy platform deviations:
 *   Teradata:  PAID=1, DENIED=2, ADJUSTED=3, REVERSED=4 (WRONG: Denied before Adjusted)
 *   Snowflake: PAID=1, ADJUSTED=2, REVERSED=3, DENIED=4 (WRONG: Reversed before Denied)
 *
 * Migration source: teradata/claims/stored_procedures/sp_claims_adr_dedup.sql
 *                   databricks/notebooks/02_adr_deduplication.py
 *                   snowflake/stored_procedures/sp_claims_adr_dedup.sql
 */

{% macro adr_priority(claim_status_column) %}
    case upper({{ claim_status_column }})
        when 'PAID'     then {{ var('adr_priority_paid', 1) }}
        when 'ADJUSTED' then {{ var('adr_priority_adjusted', 2) }}
        when 'DENIED'   then {{ var('adr_priority_denied', 3) }}
        when 'REVERSED' then {{ var('adr_priority_reversed', 4) }}
        else 99
    end
{% endmacro %}


/*
 * Macro: ADR Dedup Window Function
 *
 * Applies the ADR dedup priority as a ROW_NUMBER() window function,
 * partitioned by the claim grouping key, ordered by priority then
 * by recency (load timestamp descending).
 *
 * Usage:
 *   {{ adr_dedup_row_number('claim_status', 'original_claim_id', 'claim_id',
 *                            'claim_line_number', 'source_load_timestamp') }}
 */
{% macro adr_dedup_row_number(status_col, original_claim_id_col, claim_id_col, claim_line_col, timestamp_col) %}
    row_number() over (
        partition by coalesce({{ original_claim_id_col }}, {{ claim_id_col }}),
                     {{ claim_line_col }}
        order by
            {{ adr_priority(status_col) }} asc,
            {{ timestamp_col }} desc
    )
{% endmacro %}
