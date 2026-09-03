-- Migrated from: teradata/stored_procedures/sp_daily_transaction_load.sql
--   (FCT_DAILY_BALANCE MERGE section)
-- Teradata constructs replaced:
--   MERGE INTO ... WHEN MATCHED / NOT MATCHED -> dbt incremental
--   ZEROIFNULL(x)                             -> coalesce(x, 0)
--   COLLECT STATISTICS                        -> not needed
--
-- Databricks dialect notes:
--   * Row-value (tuple) IN sub-queries -- `where (a, b) in (select a, max(b) ...)` --
--     are not portable to Spark SQL; the latest balance per account is now selected
--     with a row_number() window instead.
--   * Spark SQL rejects a `select ... where ...` with no `from` clause, so the
--     first-run placeholder branch selects from a one-row inline sub-query.
--   * Numeric literal casts use dbt.type_numeric() rather than a hard-coded
--     `numeric`, which is not the canonical Databricks type name.

{% if target.type == 'databricks' %}
{{
    config(
        materialized='incremental',
        unique_key=['account_id', 'balance_date'],
        incremental_strategy='merge',
        file_format='delta',
        liquid_clustered_by=['account_id', 'balance_date']
    )
}}
{% else %}
{{
    config(
        materialized='incremental',
        unique_key=['account_id', 'balance_date']
    )
}}
{% endif %}

with daily_transactions as (

    select
        account_id,
        transaction_date as balance_date,
        sum(case when signed_amount > 0 then signed_amount else 0 end) as total_credits,
        sum(case when signed_amount < 0 then abs(signed_amount) else 0 end) as total_debits,
        sum(signed_amount) as net_movement,
        count(*) as transaction_count

    from {{ ref('stg_transactions') }}

    {% if is_incremental() %}
    where transaction_date > (select max(balance_date) from {{ this }})
    {% endif %}

    group by account_id, transaction_date

),

prior_balances as (

    {% if is_incremental() %}
    select
        account_id,
        prior_closing_balance
    from (
        select
            account_id,
            closing_balance as prior_closing_balance,
            row_number() over (
                partition by account_id
                order by balance_date desc
            ) as _rn
        from {{ this }}
    ) latest_balance
    where _rn = 1
    {% else %}
    select
        cast(null as {{ dbt.type_string() }}) as account_id,
        cast(0 as {{ dbt.type_numeric() }}) as prior_closing_balance
    from (select 1 as _placeholder) _empty_source
    where 1 = 0
    {% endif %}

)

select
    dt.account_id,
    dt.balance_date,
    coalesce(pb.prior_closing_balance, 0) as opening_balance,
    coalesce(pb.prior_closing_balance, 0) + dt.net_movement as closing_balance,
    dt.total_debits,
    dt.total_credits,
    dt.transaction_count

from daily_transactions dt
left join prior_balances pb
    on dt.account_id = pb.account_id
