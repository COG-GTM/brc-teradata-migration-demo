-- =============================================================================
-- Edge Case: Date Boundary Conditions
-- Validates that date fields are within reasonable ranges.
-- =============================================================================

-- Transactions should not have future dates
select
    'FUTURE_TRANSACTION' as check_name,
    transaction_id,
    transaction_date
from {{ ref('int_unified_transaction') }}
where transaction_date > current_date

union all

-- Customers should not have future onboarding dates
select
    'FUTURE_ONBOARDING',
    cast(customer_id as varchar),
    onboarding_date
from {{ ref('int_unified_customer') }}
where onboarding_date > current_date

union all

-- Accounts should not have close dates before open dates
select
    'CLOSE_BEFORE_OPEN',
    cast(account_id as varchar),
    close_date
from {{ ref('int_unified_account') }}
where close_date is not null
  and close_date < open_date
