-- =============================================================================
-- Tuva Input Layer: Pharmacy Claim
-- Maps fee-based transactions to Tuva pharmacy_claim input schema.
-- Banking fee transactions are mapped as the closest analogue to pharmacy claims.
--
-- Tuva Reference: https://thetuvaproject.com/data-dictionaries/input-layer
-- =============================================================================

with fee_transactions as (

    select *
    from {{ ref('int_transaction_enriched') }}
    where transaction_type in ('FEE', 'INTEREST')

)

select
    transaction_id as claim_id,
    1 as claim_line_number,

    -- Member identification
    customer_id as member_id,

    -- Dates
    transaction_date as dispensing_date,
    transaction_date as paid_date,

    -- Financial
    amount as paid_amount,
    amount as charge_amount,
    amount as allowed_amount,
    0 as coinsurance_amount,
    0 as copayment_amount,
    0 as deductible_amount,

    -- Prescriber/pharmacy mapping
    branch_code as prescribing_provider_npi,
    branch_code as dispensing_provider_npi,

    -- Product details
    transaction_type as ndc_code,
    1 as quantity,
    30 as days_supply,
    1 as refill_number,

    -- Data source
    '{{ var("data_source") }}' as data_source

from fee_transactions
