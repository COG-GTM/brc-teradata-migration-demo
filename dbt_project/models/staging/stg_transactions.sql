-- Migrated from: teradata/ddl/03_staging_views.sql (V_TRANSACTION_ENRICHED)
-- Teradata constructs replaced:
--   ZEROIFNULL(amount)  -> coalesce(amount, 0)
--   date arithmetic     -> datediff()
--   LOCK ROW FOR ACCESS -> removed

with source as (

    select * from {{ source('barclays_raw', 'transaction') }}

),

enriched as (

    select
        transaction_id,
        account_id,
        transaction_date,
        coalesce(amount, 0) as amount,
        upper(trim(currency)) as currency,
        upper(trim(transaction_type)) as transaction_type,
        counterparty_id,
        description,
        upper(trim(channel)) as channel,

        -- Signed amount: negative for debits, positive for credits
        case
            when upper(trim(transaction_type)) in ('DEBIT', 'TRANSFER_OUT', 'FEE')
                then -1 * coalesce(amount, 0)
            else coalesce(amount, 0)
        end as signed_amount,

        -- Value band classification
        case
            when coalesce(amount, 0) < 100 then 'MICRO'
            when coalesce(amount, 0) < 1000 then 'SMALL'
            when coalesce(amount, 0) < 10000 then 'MEDIUM'
            when coalesce(amount, 0) < 100000 then 'LARGE'
            else 'VERY_LARGE'
        end as value_band

    from source

)

select * from enriched
