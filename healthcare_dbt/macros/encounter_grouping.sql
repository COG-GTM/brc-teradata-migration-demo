/*
 * Macro: Gap-and-Island Encounter Grouping
 *
 * Implements proper overlap detection for grouping overlapping or contiguous
 * medical claims into encounters (episodes of care). This resolves DRIFT-002.
 *
 * Algorithm: Gap-and-island with running max end_date
 *   1. Order claims by member + start_date
 *   2. Track running max of end_date within each member
 *   3. When start_date > previous running max end_date, a new island begins
 *   4. Cumulative sum of island-start flags = encounter group ID
 *
 * Correct implementation adopted from: Databricks (03_encounter_grouping.py)
 *                                      Snowflake (sp_encounter_grouping.sql)
 *
 * Legacy Teradata deviation: Used naive GROUP BY member_id, service_date_from
 *   which does NOT merge overlapping date ranges. (DRIFT-002)
 *
 * Migration source: teradata/claims/stored_procedures/sp_encounter_grouping.sql
 *                   databricks/notebooks/03_encounter_grouping.py
 *                   snowflake/stored_procedures/sp_encounter_grouping.sql
 */

{% macro gap_and_island_encounter_groups(
    member_id_col,
    start_date_col,
    end_date_col
) %}
    /*
     * Step 1: Calculate running max end_date for each member,
     * looking at all preceding rows.
     */
    max({{ end_date_col }}) over (
        partition by {{ member_id_col }}
        order by {{ start_date_col }}, {{ end_date_col }}
        rows between unbounded preceding and 1 preceding
    ) as prev_max_end_date
{% endmacro %}


{% macro is_new_encounter_island(start_date_col, prev_max_end_col) %}
    /*
     * Step 2: Detect new island starts.
     * A new island begins when:
     *   - This is the first claim for the member (prev_max_end_date IS NULL)
     *   - start_date is AFTER the previous running max end_date (gap detected)
     */
    case
        when {{ prev_max_end_col }} is null then 1
        when {{ start_date_col }} > {{ prev_max_end_col }} then 1
        else 0
    end
{% endmacro %}


{% macro encounter_group_id(member_id_col, start_date_col, end_date_col, is_new_island_col) %}
    /*
     * Step 3: Assign encounter group IDs using cumulative sum of island starts.
     * This is the CONDITIONAL_TRUE_EVENT pattern.
     */
    sum({{ is_new_island_col }}) over (
        partition by {{ member_id_col }}
        order by {{ start_date_col }}, {{ end_date_col }}
        rows between unbounded preceding and current row
    )
{% endmacro %}
