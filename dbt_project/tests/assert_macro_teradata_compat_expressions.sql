-- Macro test: the Teradata compatibility scalar macros must produce the
-- documented values on every target dialect.
--   cast_teradata_date('20240115')  -> 2024-01-15
--   zeroifnull(null)                -> 0
--   nullifzero(0)                   -> null
--   td_char_length('BARCLAYS')      -> 8
-- Fails (returns rows) if any expectation is violated.

with expectations as (

    select
        {{ cast_teradata_date("'20240115'") }} as parsed_date,
        {{ zeroifnull('cast(null as numeric)') }} as zeroified,
        {{ nullifzero('0') }} as nullified,
        {{ td_char_length("'BARCLAYS'") }} as name_length

)

select *
from expectations
where parsed_date <> {{ to_date_expr("'2024-01-15'") }}
   or zeroified <> 0
   or nullified is not null
   or name_length <> 8
