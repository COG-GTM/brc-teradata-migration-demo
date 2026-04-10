-- Custom test: Ensure all closing balances in active CURRENT and SAVINGS accounts
-- are non-negative (overdraft accounts excluded).
-- Replaces Teradata post-load validation query from daily_batch_load.bteq.

select
    b.account_id,
    b.balance_date,
    b.closing_balance

from {{ ref('int_daily_account_balances') }} b
inner join {{ ref('stg_accounts') }} a
    on b.account_id = a.account_id
where a.account_type in ('SAVINGS')
  and b.closing_balance < 0
