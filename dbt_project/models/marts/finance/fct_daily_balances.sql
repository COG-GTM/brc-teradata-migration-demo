-- Migrated from: teradata/ddl/04_warehouse_tables.sql (FCT_DAILY_BALANCE)
--   and teradata/macros/macro_account_balance_snapshot.sql
-- Teradata constructs replaced:
--   MERGE INTO with upsert      -> dbt incremental materialization
--   ZEROIFNULL(x)               -> coalesce(x, 0)
--   NULLIFZERO(x)               -> nullif(x, 0)
--   Teradata date key FORMAT    -> integer date key via to_char

{{
    config(
        materialized='incremental',
        unique_key=['account_id', 'balance_date']
    )
}}

with new_balances as (

    select * from {{ ref('int_daily_account_balances') }}

    {% if is_incremental() %}
    where balance_date > (select max(balance_date) from {{ this }})
    {% endif %}

),

{% if is_incremental() %}
-- Fetch the latest existing row per account from the target table so that
-- lag() can look back across the incremental boundary.
prior_boundary as (

    select
        account_id,
        balance_date,
        opening_balance,
        closing_balance,
        total_debits,
        total_credits,
        transaction_count
    from {{ this }}
    where (account_id, balance_date) in (
        select account_id, max(balance_date)
        from {{ this }}
        group by account_id
    )

),

-- Union boundary rows with new rows so lag() sees the prior closing balance
balances as (

    select *, false as _is_boundary from new_balances
    union all
    select *, true as _is_boundary from prior_boundary

),
{% else %}
balances as (

    select *, false as _is_boundary from new_balances

),
{% endif %}

accounts as (

    select
        account_id,
        customer_id,
        account_type,
        currency
    from {{ ref('stg_accounts') }}

),

with_prior as (

    select
        b.account_id,
        b.balance_date,
        b.opening_balance,
        b.closing_balance,
        b.total_debits,
        b.total_credits,
        b.transaction_count,
        b._is_boundary,

        -- Prior day closing balance for daily change calculation
        lag(b.closing_balance) over (
            partition by b.account_id
            order by b.balance_date
        ) as prior_closing_balance

    from balances b

)

select
    {{ dbt_utils.generate_surrogate_key(['wp.account_id', 'wp.balance_date']) }} as balance_sk,
    wp.account_id,
    a.customer_id,
    a.account_type,
    a.currency,
    wp.balance_date,
    wp.opening_balance,
    wp.closing_balance,
    wp.total_debits,
    wp.total_credits,
    wp.transaction_count,

    -- Daily change (replaces macro_account_balance_snapshot logic)
    wp.closing_balance - coalesce(wp.prior_closing_balance, 0) as daily_change,
    case
        when nullif(wp.prior_closing_balance, 0) is not null
            then (wp.closing_balance - wp.prior_closing_balance)
                 / wp.prior_closing_balance * 100
        else 0
    end as daily_change_pct,

    current_timestamp as etl_loaded_ts

from with_prior wp
inner join accounts a
    on wp.account_id = a.account_id
-- Exclude the boundary rows so they are not re-inserted into the target
where not wp._is_boundary
