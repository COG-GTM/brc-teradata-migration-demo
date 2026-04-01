-- Migrated from: teradata/stored_procedures/sp_customer_risk_scoring.sql
-- Teradata constructs replaced:
--   VOLATILE TABLE vt_risk_factors -> CTE
--   QUALIFY ROW_NUMBER()           -> sub-query with window function
--   ZEROIFNULL(x)                  -> coalesce(x, 0)
--   NULLIFZERO(x)                  -> nullif(x, 0)
--   (date - date_of_birth) / 365   -> datediff('year', date_of_birth, current_date)
--   HASHBUCKET(HASHROW(x)) MOD 10  -> mod(abs(hash(x)), 10)

with customer_accounts as (

    select
        c.customer_id,
        c.kyc_status,
        c.risk_rating as current_risk_rating,
        c.segment,
        c.onboarding_date,
        c.date_of_birth,

        -- Age calculation (replaces Teradata date arithmetic)
        {{ datediff('c.date_of_birth', 'current_date', 'year') }} as customer_age,

        -- Tenure in years
        {{ datediff('c.onboarding_date', 'current_date', 'year') }} as tenure_years,

        count(distinct a.account_id) as account_count,
        sum(case when a.status = 'ACTIVE' then 1 else 0 end) as active_account_count

    from {{ ref('stg_customers') }} c
    left join {{ ref('stg_accounts') }} a
        on c.customer_id = a.customer_id
    group by 1, 2, 3, 4, 5, 6

),

transaction_metrics as (

    select
        a.customer_id,
        count(t.transaction_id) as total_transactions,
        coalesce(sum(t.amount), 0) as total_transaction_volume,
        coalesce(avg(t.amount), 0) as avg_transaction_amount,
        coalesce(max(t.amount), 0) as max_transaction_amount,
        count(distinct t.transaction_date) as active_days,
        count(case when t.value_band = 'VERY_LARGE' then 1 end) as large_transaction_count

    from {{ ref('stg_accounts') }} a
    inner join {{ ref('stg_transactions') }} t
        on a.account_id = t.account_id
    group by 1

),

risk_factors as (

    select
        ca.customer_id,
        ca.kyc_status,
        ca.current_risk_rating,
        ca.segment,
        ca.customer_age,
        ca.tenure_years,
        ca.account_count,
        ca.active_account_count,

        coalesce(tm.total_transactions, 0) as total_transactions,
        coalesce(tm.total_transaction_volume, 0) as total_transaction_volume,
        coalesce(tm.avg_transaction_amount, 0) as avg_transaction_amount,
        coalesce(tm.max_transaction_amount, 0) as max_transaction_amount,
        coalesce(tm.active_days, 0) as active_days,
        coalesce(tm.large_transaction_count, 0) as large_transaction_count,

        -- Derived risk rating (replaces Teradata stored procedure logic)
        case
            when ca.kyc_status != 'VERIFIED' then 'E'
            when ca.tenure_years < 1
              or coalesce(tm.large_transaction_count, 0) > 10 then 'D'
            when coalesce(tm.total_transaction_volume, 0) > 1000000
              or ca.customer_age < 25 then 'C'
            when ca.tenure_years > 5
              and coalesce(tm.total_transactions, 0) > 100 then 'A'
            else 'B'
        end as derived_risk_rating,

        -- Deterministic bucket (replaces HASHBUCKET(HASHROW(customer_id)) MOD 10)
        -- generate_surrogate_key returns text hash; convert hex prefix to int for modulo
        mod(abs(('x' || substring({{ dbt_utils.generate_surrogate_key(['ca.customer_id']) }}, 1, 8))::bit(32)::int), 10) as risk_bucket

    from customer_accounts ca
    left join transaction_metrics tm
        on ca.customer_id = tm.customer_id

)

select * from risk_factors
