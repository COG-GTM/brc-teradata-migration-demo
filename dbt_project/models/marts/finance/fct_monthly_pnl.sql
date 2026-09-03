-- Migrated from: teradata/stored_procedures/sp_monthly_pnl_rollup.sql
-- Teradata constructs replaced:
--   VOLATILE TABLE vt_monthly_detail -> CTE
--   GROUP BY ROLLUP                  -> grouping sets / union approach
--   CSUM (running total)             -> sum() over (order by)
--   MAVG (moving average)            -> avg() over (rows between)
--   EXTRACT(YEAR/MONTH FROM x)       -> date_trunc / extract
--
-- Databricks physical design:
--   Delta file format. The table holds a handful of rows per month per business
--   line, so partitioning on reporting_month would produce many tiny files.
--   Liquid clustering on (reporting_month, business_line) instead: reporting_month
--   is the dominant range filter and business_line the dominant equality filter,
--   and liquid clustering re-balances incrementally without a ZORDER pass.
--   Snowflake keeps an automatic clustering key on the same columns.

{{
    config(
        file_format='delta' if target.type == 'databricks' else none,
        liquid_clustered_by=['reporting_month', 'business_line'] if target.type == 'databricks' else none,
        cluster_by=['reporting_month', 'business_line'] if target.type == 'snowflake' else none
    )
}}

with monthly_detail as (

    select
        cast(date_trunc('month', t.transaction_date) as date) as reporting_month,

        -- Business line from account type
        case
            when t.account_type in ('CURRENT', 'SAVINGS') then 'RETAIL_BANKING'
            when t.account_type in ('LOAN', 'MORTGAGE') then 'LENDING'
            when t.account_type = 'CREDIT_CARD' then 'CARDS'
            else 'OTHER'
        end as business_line,

        t.account_type as product_type,

        -- Revenue components
        sum(case when t.transaction_type = 'INTEREST' and t.signed_amount > 0
            then t.signed_amount else 0 end) as interest_income,
        sum(case when t.transaction_type = 'FEE'
            then abs(t.signed_amount) else 0 end) as fee_income,

        -- Cost components (simplified)
        sum(case when t.transaction_type = 'INTEREST' and t.signed_amount < 0
            then abs(t.signed_amount) else 0 end) as interest_expense,

        count(distinct t.account_id) as active_accounts,
        count(t.transaction_id) as transaction_count

    from {{ ref('int_transaction_enriched') }} t
    group by 1, 2, 3

),

pnl_summary as (

    select
        reporting_month,
        business_line,
        product_type,
        interest_income,
        fee_income,
        interest_expense,
        (interest_income + fee_income) as gross_revenue,
        (interest_income + fee_income - interest_expense) as net_revenue,
        active_accounts,
        transaction_count,

        -- YTD running totals (replaces Teradata CSUM)
        sum(interest_income + fee_income - interest_expense) over (
            partition by business_line, extract(year from reporting_month)
            order by reporting_month
            rows between unbounded preceding and current row
        ) as ytd_net_revenue,

        -- 3-month moving average (replaces Teradata MAVG)
        avg(interest_income + fee_income - interest_expense) over (
            partition by business_line
            order by reporting_month
            rows between 2 preceding and current row
        ) as ma3_net_revenue

    from monthly_detail

)

select
    {{ dbt_utils.generate_surrogate_key(['reporting_month', 'business_line', 'product_type']) }} as pnl_id,
    reporting_month,
    business_line,
    product_type,
    interest_income,
    fee_income,
    interest_expense,
    gross_revenue,
    net_revenue,
    ytd_net_revenue,
    ma3_net_revenue,
    active_accounts,
    transaction_count,
    case
        when gross_revenue > 0
            then cast(interest_expense as {{ dbt.type_numeric() }})
                 / cast(gross_revenue as {{ dbt.type_numeric() }})
        else null
    end as cost_income_ratio,
    current_timestamp as etl_loaded_ts

from pnl_summary
