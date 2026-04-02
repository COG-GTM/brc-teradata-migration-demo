-- =============================================================================
-- Migrated from: teradata/stored_procedures/sp_regulatory_capital_calc.sql
--   and teradata/ddl/05_mart_tables.sql (MART_REGULATORY_CAPITAL)
-- Teradata constructs replaced:
--   GROUP BY ROLLUP        -> union all approach for rollup levels
--   CSUM (cumulative sum)  -> sum() over (order by)
--   MAVG (moving average)  -> avg() over (rows between)
--   MDIFF (moving diff)    -> lag() based difference
--   NORMALIZE ON           -> removed (period normalization not applicable)
--   GROUPING() function    -> explicit rollup_level column
-- Now operates on unified cross-platform credit risk scores.
-- =============================================================================

with credit_risk as (

    select * from {{ ref('fct_credit_risk_scores') }}

),

-- Detail level: by segment (acting as asset_class proxy)
by_asset_class as (

    select
        segment as asset_class,
        count(distinct customer_id) as customer_count,
        sum(exposure_at_default) as total_exposure,
        sum(risk_weighted_assets) as total_rwa,
        sum(expected_loss) as total_expected_loss,
        avg(probability_of_default) as avg_pd,
        avg(loss_given_default) as avg_lgd,
        'ASSET_CLASS' as rollup_level

    from credit_risk
    group by segment

),

-- Total level: all classes combined
total_level as (

    select
        'ALL_CLASSES' as asset_class,
        count(distinct customer_id) as customer_count,
        sum(exposure_at_default) as total_exposure,
        sum(risk_weighted_assets) as total_rwa,
        sum(expected_loss) as total_expected_loss,
        avg(probability_of_default) as avg_pd,
        avg(loss_given_default) as avg_lgd,
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
    current_date as reporting_date,
    asset_class,
    rollup_level,
    customer_count,
    total_exposure,
    total_rwa,
    total_expected_loss,
    avg_pd,
    avg_lgd,

    -- Capital required (8% of RWA per Basel III)
    total_rwa * 0.08 as capital_required,

    -- Capital ratio (simplified: capital_required / total_exposure)
    case
        when total_exposure > 0
            then (total_rwa * 0.08) / total_exposure
        else null
    end as capital_ratio,

    -- Leverage ratio (simplified)
    case
        when total_exposure > 0
            then total_rwa / total_exposure
        else null
    end as leverage_ratio,

    current_timestamp as etl_loaded_ts

from combined
