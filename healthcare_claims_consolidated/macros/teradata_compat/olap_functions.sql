-- =============================================================================
-- Teradata OLAP Function Compatibility Macros
-- Migrated from: teradata/stored_procedures/sp_monthly_pnl_rollup.sql
--   and teradata/stored_procedures/sp_regulatory_capital_calc.sql
--
-- Teradata OLAP functions and their standard SQL equivalents:
--   CSUM(expr, sort_col) -> SUM(expr) OVER (ORDER BY sort_col ROWS UNBOUNDED PRECEDING)
--   MAVG(expr, n, sort_col) -> AVG(expr) OVER (ORDER BY sort_col ROWS BETWEEN n-1 PRECEDING AND CURRENT ROW)
--   MDIFF(expr, n, sort_col) -> expr - LAG(expr, n) OVER (ORDER BY sort_col)
-- =============================================================================

-- CSUM: Cumulative Sum (Teradata OLAP function)
-- Replaces: CSUM(amount, txn_date) OVER (PARTITION BY account_id)
{% macro teradata_csum(expr, order_by, partition_by=none) %}
    sum({{ expr }}) over (
        {% if partition_by %}partition by {{ partition_by }}{% endif %}
        order by {{ order_by }}
        rows unbounded preceding
    )
{% endmacro %}


-- MAVG: Moving Average (Teradata OLAP function)
-- Replaces: MAVG(amount, 3, txn_date) OVER (PARTITION BY account_id)
{% macro teradata_mavg(expr, window_size, order_by, partition_by=none) %}
    avg({{ expr }}) over (
        {% if partition_by %}partition by {{ partition_by }}{% endif %}
        order by {{ order_by }}
        rows between {{ window_size - 1 }} preceding and current row
    )
{% endmacro %}


-- MDIFF: Moving Difference (Teradata OLAP function)
-- Replaces: MDIFF(amount, 1, txn_date) OVER (PARTITION BY account_id)
{% macro teradata_mdiff(expr, lag_periods, order_by, partition_by=none) %}
    {{ expr }} - lag({{ expr }}, {{ lag_periods }}) over (
        {% if partition_by %}partition by {{ partition_by }}{% endif %}
        order by {{ order_by }}
    )
{% endmacro %}
