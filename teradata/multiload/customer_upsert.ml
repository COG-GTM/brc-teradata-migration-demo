/*******************************************************************************
 * customer_upsert.ml
 *
 * MultiLoad UPSERT script for customer records. Updates existing rows and
 * inserts missing ones using the DO INSERT FOR MISSING UPDATE ROWS pattern.
 *
 * This is the MultiLoad equivalent of a MERGE / upsert operation and is
 * the primary mechanism for incremental customer dimension maintenance.
 *
 * Migrated from COG-GTM/Teradata-Utilities-Script CustomerUPDATE.ml
 ******************************************************************************/

.LOGTABLE BARCLAYS_DWH.CUST_UPSERT_ML_LOG;
.LOGON ${TERADATA_HOST}/barclays_etl,${ETL_PASSWORD};

.BEGIN MLOAD TABLES BARCLAYS_DWH.DIM_CUSTOMER_STAGING;

.LAYOUT CUSTOMER_LAYOUT;
.FIELD in_customer_id      * VARCHAR(10);
.FIELD in_first_name       * VARCHAR(100);
.FIELD in_last_name        * VARCHAR(100);
.FIELD in_date_of_birth    * VARCHAR(10);
.FIELD in_nationality      * VARCHAR(2);
.FIELD in_kyc_status       * VARCHAR(20);
.FIELD in_risk_rating      * VARCHAR(1);
.FIELD in_segment          * VARCHAR(20);
.FIELD in_postcode         * VARCHAR(10);
.FIELD in_country          * VARCHAR(2);

.DML LABEL UPSERT_CUSTOMER
DO INSERT FOR MISSING UPDATE ROWS;

UPDATE BARCLAYS_DWH.DIM_CUSTOMER_STAGING
SET
    first_name      = :in_first_name,
    last_name       = :in_last_name,
    date_of_birth   = CAST(:in_date_of_birth AS DATE FORMAT 'YYYY-MM-DD'),
    nationality     = :in_nationality,
    kyc_status      = :in_kyc_status,
    risk_rating     = :in_risk_rating,
    segment         = :in_segment,
    postcode        = :in_postcode,
    country         = :in_country
WHERE customer_id = CAST(:in_customer_id AS INTEGER);

INSERT INTO BARCLAYS_DWH.DIM_CUSTOMER_STAGING
(
    customer_id,
    first_name,
    last_name,
    date_of_birth,
    nationality,
    kyc_status,
    risk_rating,
    segment,
    postcode,
    country
)
VALUES
(
    CAST(:in_customer_id AS INTEGER),
    :in_first_name,
    :in_last_name,
    CAST(:in_date_of_birth AS DATE FORMAT 'YYYY-MM-DD'),
    :in_nationality,
    :in_kyc_status,
    :in_risk_rating,
    :in_segment,
    :in_postcode,
    :in_country
);

.IMPORT INFILE ${INPUT_DIR}/customer_upsert_${YYYYMMDD}.dat
FORMAT VARTEXT '|'
LAYOUT CUSTOMER_LAYOUT
APPLY UPSERT_CUSTOMER;

.END MLOAD;
.LOGOFF;
