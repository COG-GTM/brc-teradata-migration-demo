-- Migrated from: teradata/stored_procedures/sp_regulatory_capital_calc.sql
--   and teradata/ddl/05_mart_tables.sql (MART_REGULATORY_CAPITAL)
-- Teradata constructs replaced:
--   GROUP BY ROLLUP        -> union all approach for rollup levels
--   CSUM (cumulative sum)  -> sum() over (order by)
--   MAVG (moving average)  -> avg() over (rows between)
--   MDIFF (moving diff)    -> lag() based difference
--   NORMALIZE ON           -> removed (period normalization not applicable)
--   GROUPING() function    -> explicit rollup_level column
--
-- Databricks notes:
--   * Spark widens aggregates (sum -> p+10, avg -> s+4) and inflates DECIMAL
--     division precision, so both the aggregates and the ratio operands are
--     narrowed with explicit casts to stay under the precision-38 ceiling.
--   * Ratios divide DECIMALs, never INTEGERs: Spark's `/` is true division,
--     but keeping both operands DECIMAL makes the behaviour identical on
--     Databricks, Snowflake and Postgres (where `int / int` truncates).
--   * nullif() guards the denominator in addition to the case expression,
--     because Databricks runs with ANSI mode on and raises on divide-by-zero.

{%- set is_databricks = target.type == 'databricks' -%}

{% if is_databricks %}
{{
    config(
        materialized='table',
        file_format='delta',
        liquid_clustered_by=['reporting_date', 'asset_class'],
        tblproperties={
            'delta.autoOptimize.optimizeWrite': 'true',
            'delta.autoOptimize.autoCompact': 'true'
        }
    )
}}
{% endif %}

with credit_risk as (

    select * from {{ ref('fct_credit_risk_scores') }}

),

-- Detail level: by segment (acting as asset_class proxy)
by_asset_class as (

    select
        segment as asset_class,
        count(distinct customer_id) as customer_count,
        cast(sum(exposure_at_default) as decimal(30, 6)) as total_exposure,
        cast(sum(risk_weighted_assets) as decimal(30, 6)) as total_rwa,
        cast(sum(expected_loss) as decimal(30, 6)) as total_expected_loss,
        cast(avg(probability_of_default) as decimal(18, 8)) as avg_pd,
        cast(avg(loss_given_default) as decimal(18, 8)) as avg_lgd,
        'ASSET_CLASS' as rollup_level

    from credit_risk
    group by segment

),

-- Total level: all classes combined
total_level as (

    select
        'ALL_CLASSES' as asset_class,
        count(distinct customer_id) as customer_count,
        cast(sum(exposure_at_default) as decimal(30, 6)) as total_exposure,
        cast(sum(risk_weighted_assets) as decimal(30, 6)) as total_rwa,
        cast(sum(expected_loss) as decimal(30, 6)) as total_expected_loss,
        cast(avg(probability_of_default) as decimal(18, 8)) as avg_pd,
        cast(avg(loss_given_default) as decimal(18, 8)) as avg_lgd,
        'TOTAL' as rollup_level

    from credit_risk

),

combined as (

    select * from by_asset_class
    union all
    select * from total_level

)

select
    {{ dbt_utils.generate_surrogate_key(['asset_class', 'rollup_level']) }} as capital_id,
    cast(current_date as date) as reporting_date,
    asset_class,
    rollup_level,
    customer_count,
    total_exposure,
    total_rwa,
    total_expected_loss,
    avg_pd,
    avg_lgd,

    -- Capital required (8% of RWA per Basel III)
    cast(
        total_rwa * cast(0.08 as decimal(5, 4))
        as decimal(38, 6)
    ) as capital_required,

    -- Capital ratio (simplified: capital_required / total_exposure)
    case
        when total_exposure > 0
            then cast(
                cast(total_rwa * cast(0.08 as decimal(5, 4)) as decimal(16, 4))
                / nullif(cast(total_exposure as decimal(16, 4)), 0)
                as decimal(18, 8)
            )
        else null
    end as capital_ratio,

    -- Leverage ratio (simplified)
    case
        when total_exposure > 0
            then cast(
                cast(total_rwa as decimal(16, 4))
                / nullif(cast(total_exposure as decimal(16, 4)), 0)
                as decimal(18, 8)
            )
        else null
    end as leverage_ratio,

    {{ dbt.current_timestamp() }} as etl_loaded_ts

from combined
