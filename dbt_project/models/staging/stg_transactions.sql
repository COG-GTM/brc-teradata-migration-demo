-- Migrated from: teradata/ddl/03_staging_views.sql (V_TRANSACTION_ENRICHED)
-- Teradata constructs replaced:
--   ZEROIFNULL(amount)  -> zeroifnull() compat macro
--   date arithmetic     -> handled downstream with datediff()
--   LOCK ROW FOR ACCESS -> removed
--
-- Databricks notes: amount is cast to decimal(18,2) (identical spelling on
-- Databricks, Snowflake and Postgres) so the signed amount keeps exact
-- currency semantics instead of Databricks' inferred double.

with source as (

    select * from {{ source('barclays_raw', 'transaction') }}

),

typed as (

    select
        transaction_id,
        account_id,
        cast(transaction_date as date) as transaction_date,
        {{ zeroifnull('cast(amount as decimal(18,2))') }} as amount,
        upper(trim(currency)) as currency,
        upper(trim(transaction_type)) as transaction_type,
        counterparty_id,
        description,
        upper(trim(channel)) as channel

    from source

),

enriched as (

    select
        transaction_id,
        account_id,
        transaction_date,
        amount,
        currency,
        transaction_type,
        counterparty_id,
        description,
        channel,

        -- Signed amount: negative for debits, positive for credits
        case
            when transaction_type in ('DEBIT', 'TRANSFER_OUT', 'FEE')
                then -1 * amount
            else amount
        end as signed_amount,

        -- Value band classification
        case
            when amount < 100 then 'MICRO'
            when amount < 1000 then 'SMALL'
            when amount < 10000 then 'MEDIUM'
            when amount < 100000 then 'LARGE'
            else 'VERY_LARGE'
        end as value_band

    from typed

)

select * from enriched
