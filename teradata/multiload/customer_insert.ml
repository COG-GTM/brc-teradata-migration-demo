/*******************************************************************************
 * customer_insert.ml
 *
 * MultiLoad script to insert customer records from a pipe-delimited file
 * into BARCLAYS_DWH.DIM_CUSTOMER_STAGING.
 *
 * Unlike FastLoad, MultiLoad can target non-empty tables and supports
 * concurrent DML operations (insert, update, delete).
 *
 * Migrated from COG-GTM/Teradata-Utilities-Script Customerinsert.ml.txt
 ******************************************************************************/

.LOGTABLE BARCLAYS_DWH.CUST_INSERT_ML_LOG;
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

.DML LABEL INSERT_CUSTOMER;

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

.IMPORT INFILE ${INPUT_DIR}/customer_insert_${YYYYMMDD}.dat
FORMAT VARTEXT '|'
LAYOUT CUSTOMER_LAYOUT
APPLY INSERT_CUSTOMER;

.END MLOAD;
.LOGOFF;
