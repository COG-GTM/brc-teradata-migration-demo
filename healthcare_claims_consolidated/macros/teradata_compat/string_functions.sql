-- =============================================================================
-- Teradata String Function Compatibility Macros
-- Migrated from: teradata/stored_procedures/sp_aml_screening.sql
--
-- Teradata string functions and their standard SQL equivalents:
--   OREPLACE(str, from, to) -> REPLACE(str, from, to)
--   OTRANSLATE(str, from, to) -> TRANSLATE(str, from, to)
--   INDEX(str, substr)      -> POSITION(substr IN str)  or  CHARINDEX(substr, str)
-- =============================================================================

-- OREPLACE: Teradata string replacement
-- Teradata: OREPLACE(customer_name, '-', ' ')
-- Standard: REPLACE(customer_name, '-', ' ')
{% macro oreplace(string_expr, from_str, to_str) %}
    replace({{ string_expr }}, {{ from_str }}, {{ to_str }})
{% endmacro %}


-- OTRANSLATE: Teradata character-by-character translation
-- Teradata: OTRANSLATE(phone, '()-. ', '')
-- Standard: TRANSLATE(phone, '()-. ', '     ') + TRIM
{% macro otranslate(string_expr, from_chars, to_chars) %}
    {% if target.type == 'snowflake' %}
        translate({{ string_expr }}, {{ from_chars }}, {{ to_chars }})
    {% elif target.type == 'databricks' %}
        translate({{ string_expr }}, {{ from_chars }}, {{ to_chars }})
    {% elif target.type == 'duckdb' %}
        replace(replace(replace(replace({{ string_expr }}, '(', ''), ')', ''), '-', ''), '.', '')
    {% else %}
        translate({{ string_expr }}, {{ from_chars }}, {{ to_chars }})
    {% endif %}
{% endmacro %}


-- Teradata SOUNDEX: Available in Snowflake and most databases natively
-- No macro wrapper needed; just document the equivalence.
-- Teradata: SOUNDEX(name) = SOUNDEX('search_term')
-- Snowflake: SOUNDEX(name) = SOUNDEX('search_term')  -- native support


-- Teradata LIKE ANY: Multiple LIKE patterns
-- Teradata: name LIKE ANY ('%BANK%', '%TRUST%', '%FINANCE%')
-- Standard: name LIKE '%BANK%' OR name LIKE '%TRUST%' OR name LIKE '%FINANCE%'
{% macro like_any(column, patterns) %}
    (
        {% for pattern in patterns %}
            {{ column }} like '{{ pattern }}'
            {% if not loop.last %} or {% endif %}
        {% endfor %}
    )
{% endmacro %}
