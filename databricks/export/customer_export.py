# =============================================================================
# customer_export.py
#
# Replaces:
#   teradata/bteq/customer_export.bteq  (.EXPORT DATA FILE=... , '|' separator)
#   teradata/tpt/customer_export.tpt    (SQL_SELECTOR -> FILE_WRITER, 2 sessions)
#
# Mapping rationale:
#   Both legacy artifacts do the same thing — SELECT the current customer
#   dimension and write a pipe-delimited file for downstream consumers — so
#   they collapse into one Databricks task. The TPT SELECTOR/DATACONNECTOR
#   operator pair and BTEQ's .EXPORT/.EXPORT RESET both become a Spark
#   DataFrame write to a Unity Catalog volume (replacing ${EXPORT_DIR}).
#   `coalesce(1)` plus a rename reproduces the single, header-less, ordered
#   file the CRM feed expects; a single output partition keeps ORDER BY
#   customer_id stable.
#
#   Column mapping. The legacy source BARCLAYS_DWH.DIM_CUSTOMER is now the dbt
#   mart fct_kyc_status, which carries the customer attributes the migrated
#   sources actually contain:
#     customer_id, first_name, last_name, nationality, kyc_status,
#     risk_rating, segment            -> emitted as-is (TRIM preserved on names)
#     date_of_birth                   -> joined from stg_customers
#     effective_from                  -> onboarding_date (the only effective
#                                        date carried by the migrated feed)
#     effective_to, is_current        -> the mart holds current state only, so
#                                        these are constant ''/'Y'; SCD2 history
#                                        lives in snap_customer_risk_rating
#     postcode, country               -> not present in any migrated source
#                                        (sample_data/customers.csv has no
#                                        address columns); emitted empty to keep
#                                        the downstream field positions stable
#   CAST(date AS VARCHAR(10)) becomes date_format(..., 'yyyy-MM-dd').
#
#   The "Exported N customer records" line that BTEQ derived from ACTIVITYCOUNT
#   is printed to the task log.
# =============================================================================

import argparse
from datetime import date

from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Current customer dimension export")
    parser.add_argument("--catalog", default="barclays")
    parser.add_argument("--schema_compliance", default="compliance")
    parser.add_argument("--schema_staging", default="staging")
    parser.add_argument("--source_table", default="fct_kyc_status")
    parser.add_argument("--export_dir", default="/Volumes/barclays/raw/exports/customer")
    parser.add_argument("--business_date", default="", help="yyyyMMdd; empty -> today")
    parser.add_argument("--delimiter", default="|")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    spark = SparkSession.builder.getOrCreate()
    dbutils = DBUtils(spark)

    business_date = args.business_date or date.today().strftime("%Y%m%d")
    file_stamp = business_date.replace("-", "")
    fq_source = f"{args.catalog}.{args.schema_compliance}.{args.source_table}"
    fq_staging = f"{args.catalog}.{args.schema_staging}.stg_customers"

    kyc = spark.table(fq_source).alias("k")
    dob = spark.table(fq_staging).select("customer_id", "date_of_birth").alias("s")

    customers = (
        kyc.join(dob, on="customer_id", how="left")
        .select(
            F.col("customer_id").cast("string").alias("customer_id"),
            F.trim(F.col("first_name")).alias("first_name"),
            F.trim(F.col("last_name")).alias("last_name"),
            F.date_format(F.col("date_of_birth"), "yyyy-MM-dd").alias("date_of_birth"),
            F.col("nationality"),
            F.col("kyc_status"),
            F.col("risk_rating"),
            F.col("segment"),
            F.lit("").alias("postcode"),
            F.col("nationality").alias("country"),
            F.date_format(F.col("onboarding_date"), "yyyy-MM-dd").alias("effective_from"),
            F.lit("").alias("effective_to"),
            F.lit("Y").alias("is_current"),
        )
        .orderBy("customer_id")
    )

    row_count = customers.count()
    staging_dir = f"{args.export_dir}/_staging_{file_stamp}"
    final_file = f"{args.export_dir}/customer_export_{file_stamp}.dat"

    (
        customers.coalesce(1)
        .write.format("csv")
        # BTEQ used .SET TITLEDASHES OFF and emitted no header row.
        .option("header", "false")
        .option("sep", args.delimiter)
        .option("nullValue", "")
        .mode("overwrite")
        .save(staging_dir)
    )

    part_file = next(f.path for f in dbutils.fs.ls(staging_dir) if f.name.startswith("part-"))
    dbutils.fs.mv(part_file, final_file)
    dbutils.fs.rm(staging_dir, recurse=True)

    print(f"Exported {row_count} customer records to {final_file}")


if __name__ == "__main__":
    main()
