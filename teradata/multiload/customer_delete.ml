/*******************************************************************************
 * customer_delete.ml
 *
 * MultiLoad DELETE script to clear all rows from the customer staging table.
 * Used as a pre-step before a full reload, or for periodic table maintenance.
 *
 * Migrated from COG-GTM/Teradata-Utilities-Script Customerdelete.ml.txt
 ******************************************************************************/

.LOGTABLE BARCLAYS_DWH.CUST_DELETE_ML_LOG;
.LOGON ${TERADATA_HOST}/barclays_etl,${ETL_PASSWORD};

.BEGIN DELETE MLOAD TABLES BARCLAYS_DWH.DIM_CUSTOMER_STAGING;

DELETE FROM BARCLAYS_DWH.DIM_CUSTOMER_STAGING;

.END MLOAD;
.LOGOFF;
