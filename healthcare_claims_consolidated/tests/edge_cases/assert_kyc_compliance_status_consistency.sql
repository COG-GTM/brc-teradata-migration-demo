-- =============================================================================
-- Edge Case: KYC Compliance Status Consistency
-- Validates that compliance_status derivation is consistent with kyc_status.
-- =============================================================================

select
    customer_id,
    kyc_status,
    compliance_status
from {{ ref('fct_kyc_status') }}
where (kyc_status = 'VERIFIED' and compliance_status != 'COMPLIANT')
   or (kyc_status = 'EXPIRED' and compliance_status != 'OVERDUE')
   or (kyc_status = 'PENDING' and compliance_status != 'IN_PROGRESS')
   or (kyc_status = 'FAILED' and compliance_status != 'REMEDIATION_REQUIRED')
