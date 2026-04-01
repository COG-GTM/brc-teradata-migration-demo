-- Custom test: Basel III capital ratio must be between 0 and 1.
-- A ratio > 1 or < 0 indicates a calculation error.

select
    capital_id,
    asset_class,
    capital_ratio
from {{ ref('fct_regulatory_capital') }}
where capital_ratio is not null
  and (capital_ratio < 0 or capital_ratio > 1)
