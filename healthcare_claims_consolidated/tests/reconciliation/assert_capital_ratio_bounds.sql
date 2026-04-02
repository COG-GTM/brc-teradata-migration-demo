-- =============================================================================
-- Capital Ratio Bounds Check
-- Validates Basel III capital ratios are within reasonable bounds.
-- =============================================================================

select
    capital_id,
    asset_class,
    capital_ratio,
    leverage_ratio
from {{ ref('fct_regulatory_capital') }}
where (capital_ratio is not null and (capital_ratio < 0 or capital_ratio > 1))
   or (leverage_ratio is not null and (leverage_ratio < 0 or leverage_ratio > 100))
