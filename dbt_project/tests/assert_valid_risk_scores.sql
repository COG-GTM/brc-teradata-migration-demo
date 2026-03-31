-- Custom test: Ensure all risk scores have valid PD, LGD, and EAD values.
-- PD should be between 0 and 1, LGD between 0 and 1, EAD >= 0.
-- Replaces Teradata validation logic from sp_customer_risk_scoring.sql.

select
    customer_id,
    probability_of_default,
    loss_given_default,
    exposure_at_default

from {{ ref('fct_credit_risk_scores') }}
where probability_of_default < 0
   or probability_of_default > 1
   or loss_given_default < 0
   or loss_given_default > 1
   or exposure_at_default < 0
