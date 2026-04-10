-- Custom test: compliance_status should be consistent with kyc_status.
-- VERIFIED kyc_status must map to COMPLIANT compliance_status.
-- EXPIRED kyc_status must map to OVERDUE compliance_status.

select
    customer_id,
    kyc_status,
    compliance_status
from {{ ref('fct_kyc_status') }}
where (kyc_status = 'VERIFIED' and compliance_status != 'COMPLIANT')
   or (kyc_status = 'EXPIRED' and compliance_status != 'OVERDUE')
   or (kyc_status = 'FAILED' and compliance_status != 'REMEDIATION_REQUIRED')
   or (kyc_status = 'PENDING' and compliance_status != 'IN_PROGRESS')
