-- Custom test: AML alert type and severity codes must be stored normalised
-- (upper case, no surrounding whitespace).
--
-- Teradata compared these codes with NOT CASESPECIFIC collation, so downstream
-- sanctions / PEP reporting tolerated mixed case. Snowflake and Databricks
-- (Spark SQL) both compare strings case- and whitespace-sensitively, and
-- Databricks offers no session collation override, so the mart normalises the
-- codes and this test guards that contract.

select
    alert_id,
    alert_type,
    alert_severity
from {{ ref('fct_aml_alerts') }}
where alert_type <> upper(trim(alert_type))
   or alert_severity <> upper(trim(alert_severity))
