-- =============================================================================
-- Tuva Input Layer: Medical Claim
-- Maps unified transaction data to Tuva medical_claim input schema.
-- Banking transactions are mapped to the claims model for data quality
-- testing and analytical framework compatibility.
--
-- Tuva Reference: https://thetuvaproject.com/data-dictionaries/input-layer
-- =============================================================================

with enriched_transactions as (

    select * from {{ ref('int_transaction_enriched') }}

)

select
    transaction_id as claim_id,
    transaction_id as claim_line_number,
    'institutional' as claim_type,

    -- Member identification
    customer_id as member_id,

    -- Dates
    transaction_date as claim_start_date,
    transaction_date as claim_end_date,
    transaction_date as admission_date,
    transaction_date as discharge_date,

    -- Financial
    amount as paid_amount,
    amount as charge_amount,
    amount as allowed_amount,
    signed_amount as coinsurance_amount,
    0 as copayment_amount,
    0 as deductible_amount,
    0 as total_cost_amount,

    -- Provider mapping (account/branch as proxy)
    branch_code as billing_npi,
    branch_code as rendering_npi,
    branch_code as facility_npi,

    -- Service details
    transaction_type as revenue_center_code,
    channel as place_of_service_code,
    account_type as ms_drg_code,

    -- Diagnosis mapping (transaction type as proxy)
    case
        when transaction_type = 'DEBIT' then 'D001'
        when transaction_type = 'CREDIT' then 'C001'
        when transaction_type = 'TRANSFER' then 'T001'
        when transaction_type = 'FEE' then 'F001'
        when transaction_type = 'INTEREST' then 'I001'
        when transaction_type = 'REVERSAL' then 'R001'
        else 'U001'
    end as diagnosis_code_1,

    cast(null as {{ dbt.type_string() }}) as diagnosis_code_2,
    cast(null as {{ dbt.type_string() }}) as diagnosis_code_3,

    -- Status
    case
        when transaction_type = 'REVERSAL' then 'reversed'
        else 'paid'
    end as claim_status,

    -- Data source
    '{{ var("data_source") }}' as data_source

from enriched_transactions
