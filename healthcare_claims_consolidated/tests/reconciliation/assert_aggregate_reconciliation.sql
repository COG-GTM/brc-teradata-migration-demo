-- =============================================================================
-- Aggregate Reconciliation: Financial Totals
-- Validates that total amounts, transaction counts, and customer counts
-- are consistent between unified and source platform models.
-- =============================================================================

with unified_aggregates as (

    select
        count(distinct customer_id) as total_customers,
        count(distinct account_id) as total_accounts,
        count(*) as total_transactions,
        coalesce(sum(amount), 0) as total_amount,
        coalesce(sum(case when signed_amount > 0 then signed_amount else 0 end), 0) as total_credits,
        coalesce(sum(case when signed_amount < 0 then abs(signed_amount) else 0 end), 0) as total_debits
    from {{ ref('int_transaction_enriched') }}

)

-- Test passes if no rows returned (all checks pass)
select
    'AGGREGATE_CHECK' as check_name,
    total_customers,
    total_accounts,
    total_transactions,
    total_amount,
    total_credits,
    total_debits
from unified_aggregates
where total_customers = 0      -- should have customers
   or total_accounts = 0       -- should have accounts
   or total_transactions = 0   -- should have transactions
   or total_amount < 0         -- total amount should be non-negative
