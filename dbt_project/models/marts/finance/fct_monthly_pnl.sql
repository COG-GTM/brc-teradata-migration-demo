-- Migrated from: teradata/stored_procedures/sp_monthly_pnl_rollup.sql
-- Teradata constructs replaced:
--   VOLATILE TABLE vt_monthly_detail -> CTE
--   GROUP BY ROLLUP                  -> UNION ALL for cross-db compatibility
--   GROUPING() function             -> explicit rollup_level column
--   CSUM (running total)             -> sum() over (order by)
--   MAVG (moving average)            -> avg() over (rows between)
--   NULLIFZERO(x)                    -> nullif(x, 0)
--   EXTRACT(YEAR/MONTH FROM x)       -> date_trunc / extract

with monthly_detail as (

    select
        date_trunc('month', t.transaction_date) as reporting_month,

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

-- Detail level: by business_line and product_type (replaces GROUPING() = 0 for both)
detail_level as (

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
        'DETAIL' as rollup_level

    from monthly_detail

),

-- Business line level: aggregated by business_line (replaces GROUPING(product_type) = 1)
business_line_level as (

    select
        reporting_month,
        business_line,
        'ALL_PRODUCTS' as product_type,
        sum(interest_income) as interest_income,
        sum(fee_income) as fee_income,
        sum(interest_expense) as interest_expense,
        sum(interest_income + fee_income) as gross_revenue,
        sum(interest_income + fee_income - interest_expense) as net_revenue,
        sum(active_accounts) as active_accounts,
        sum(transaction_count) as transaction_count,
        'BUSINESS_LINE' as rollup_level

    from monthly_detail
    group by reporting_month, business_line

),

-- Total level: all business lines combined (replaces GROUPING(business_line) = 1)
total_level as (

    select
        reporting_month,
        'ALL_LINES' as business_line,
        'ALL_PRODUCTS' as product_type,
        sum(interest_income) as interest_income,
        sum(fee_income) as fee_income,
        sum(interest_expense) as interest_expense,
        sum(interest_income + fee_income) as gross_revenue,
        sum(interest_income + fee_income - interest_expense) as net_revenue,
        sum(active_accounts) as active_accounts,
        sum(transaction_count) as transaction_count,
        'TOTAL' as rollup_level

    from monthly_detail
    group by reporting_month

),

combined as (

    select * from detail_level
    union all
    select * from business_line_level
    union all
    select * from total_level

),

with_analytics as (

    select
        c.*,

        -- YTD running totals (replaces Teradata CSUM)
        sum(c.net_revenue) over (
            partition by c.business_line, c.product_type, c.rollup_level, extract(year from c.reporting_month)
            order by c.reporting_month
            rows unbounded preceding
        ) as ytd_net_revenue,

        -- 3-month moving average (replaces Teradata MAVG)
        avg(c.net_revenue) over (
            partition by c.business_line, c.product_type, c.rollup_level
            order by c.reporting_month
            rows between 2 preceding and current row
        ) as ma3_net_revenue

    from combined c

)

select
    {{ dbt_utils.generate_surrogate_key(['reporting_month', 'business_line', 'product_type', 'rollup_level']) }} as pnl_id,
    reporting_month,
    business_line,
    product_type,
    rollup_level,
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
        when nullif(gross_revenue, 0) is not null
            then interest_expense / gross_revenue
        else null
    end as cost_income_ratio,
    current_timestamp as etl_loaded_ts

from with_analytics
