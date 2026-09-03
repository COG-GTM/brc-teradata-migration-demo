-- Custom test: compliance_status should be consistent with kyc_status.
-- VERIFIED kyc_status must map to COMPLIANT compliance_status.
-- EXPIRED kyc_status must map to OVERDUE compliance_status.
--
-- Codes are compared after upper(trim(...)) so the test behaves identically on
-- Teradata (NOT CASESPECIFIC collation), Snowflake, Databricks and Postgres.

select
    customer_id,
    kyc_status,
    compliance_status
from {{ ref('fct_kyc_status') }}
where (upper(trim(kyc_status)) = 'VERIFIED' and upper(trim(compliance_status)) <> 'COMPLIANT')
   or (upper(trim(kyc_status)) = 'EXPIRED' and upper(trim(compliance_status)) <> 'OVERDUE')
   or (upper(trim(kyc_status)) = 'FAILED' and upper(trim(compliance_status)) <> 'REMEDIATION_REQUIRED')
   or (upper(trim(kyc_status)) = 'PENDING' and upper(trim(compliance_status)) <> 'IN_PROGRESS')
