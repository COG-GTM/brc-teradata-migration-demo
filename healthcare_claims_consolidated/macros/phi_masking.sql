-- =============================================================================
-- PHI/PII Masking Macros
-- Used in compliance-facing models to mask sensitive data.
--
-- Logic drift resolution: Teradata applied masking at the mart layer only;
-- Databricks applied at staging; Snowflake used Dynamic Data Masking policies.
-- Unified approach: masking applied at mart layer via these macros,
-- with Snowflake DDM policies as an additional layer in production.
-- =============================================================================

-- Mask email: show first char + domain
{% macro mask_email(column_name) %}
    case
        when {{ column_name }} is null then null
        when position('@' in {{ column_name }}) > 0 then
            left({{ column_name }}, 1) || '***@' || split_part({{ column_name }}, '@', 2)
        else '***'
    end
{% endmacro %}

-- Mask phone: show last 4 digits
{% macro mask_phone(column_name) %}
    case
        when {{ column_name }} is null then null
        when length({{ column_name }}) >= 4 then
            repeat('*', length({{ column_name }}) - 4) || right({{ column_name }}, 4)
        else '****'
    end
{% endmacro %}

-- Mask name: show first initial + asterisks
{% macro mask_name(column_name) %}
    case
        when {{ column_name }} is null then null
        when length({{ column_name }}) > 0 then
            left({{ column_name }}, 1) || repeat('*', length({{ column_name }}) - 1)
        else '***'
    end
{% endmacro %}

-- Mask postcode: show outward code only (first half)
{% macro mask_postcode(column_name) %}
    case
        when {{ column_name }} is null then null
        when position(' ' in {{ column_name }}) > 0 then
            split_part({{ column_name }}, ' ', 1) || ' ***'
        else left({{ column_name }}, 3) || '***'
    end
{% endmacro %}

-- Mask address: replace with generic placeholder
{% macro mask_address(column_name) %}
    case
        when {{ column_name }} is null then null
        else '*** REDACTED ***'
    end
{% endmacro %}
