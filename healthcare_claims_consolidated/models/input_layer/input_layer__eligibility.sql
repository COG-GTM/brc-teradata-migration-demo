-- =============================================================================
-- Tuva Input Layer: Eligibility
-- Maps unified customer + account data to Tuva eligibility input schema.
-- This model creates the member enrollment records required by Tuva's
-- claims preprocessing and data quality framework.
--
-- Tuva Reference: https://thetuvaproject.com/data-dictionaries/input-layer
-- =============================================================================

with customers as (

    select * from {{ ref('int_unified_customer') }}

),

accounts as (

    select * from {{ ref('int_unified_account') }}

),

-- Each customer-account combination represents an enrollment period
enrollment_periods as (

    select
        c.customer_id as member_id,
        c.customer_id as subscriber_id,

        -- Gender mapping (not available in banking data; default to 'unknown')
        'unknown' as gender,

        -- Race (not available in banking data)
        cast(null as {{ dbt.type_string() }}) as race,

        c.date_of_birth as birth_date,
        cast(null as date) as death_date,
        cast(null as {{ dbt.type_string() }}) as death_flag,

        -- Enrollment dates derived from account open/close
        a.open_date as enrollment_start_date,
        coalesce(a.close_date, current_date) as enrollment_end_date,

        -- Payer mapping
        'BARCLAYS' as payer,
        a.account_type as payer_type,
        a.account_type as plan,

        -- Source identifiers
        {{ dbt_utils.generate_surrogate_key(['c.customer_id', 'a.account_id']) }} as original_reason_entitlement_code,
        cast(null as {{ dbt.type_string() }}) as dual_status_code,
        cast(null as {{ dbt.type_string() }}) as medicare_status_code,

        -- Address
        c.city as city,
        cast(null as {{ dbt.type_string() }}) as state,
        c.postcode as zip_code,
        c.country as country,

        -- Data source tracking
        '{{ var("data_source") }}' as data_source

    from customers c
    inner join accounts a
        on c.customer_id = a.customer_id

)

select * from enrollment_periods
