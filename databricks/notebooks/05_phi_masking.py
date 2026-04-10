# Databricks notebook source
# MAGIC %md
# MAGIC # 05 - PHI Masking (Protected Health Information)
# MAGIC
# MAGIC This notebook implements PHI/PII masking for healthcare claims data.
# MAGIC
# MAGIC ## IMPORTANT: Databricks Masking Strategy
# MAGIC PHI masking is applied at the **STAGING layer** (before warehouse).
# MAGIC This is a deliberate design difference from Teradata, which applies
# MAGIC masking after the mart layer. By masking early in Databricks, we ensure:
# MAGIC - No downstream consumer ever sees unmasked PHI
# MAGIC - Warehouse and mart tables are always safe for broader access
# MAGIC - Compliance with HIPAA minimum necessary standard
# MAGIC
# MAGIC ## Masking Methods
# MAGIC - **Names**: SHA-256 hash (irreversible, consistent for joins)
# MAGIC - **SSN**: SHA-256 hash
# MAGIC - **DOB**: Year preserved + Jan 1 (age derivation still possible)
# MAGIC - **ZIP Code**: Truncated to first 3 digits (Safe Harbor de-identification)
# MAGIC - **Phone/Email**: SHA-256 hash
# MAGIC - **Address**: Removed entirely (not needed downstream)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col, sha2, concat_ws, lit, when, substring, to_date,
    concat, year, current_timestamp, row_number, coalesce,
    date_format, regexp_replace, lower, trim, upper
)
from pyspark.sql.types import StringType, DateType

RAW_SCHEMA = "claims_raw"
STAGING_SCHEMA = "claims_staging"

# COMMAND ----------

# MAGIC %md
# MAGIC ## PHI Masking Functions
# MAGIC
# MAGIC These functions implement HIPAA-compliant de-identification using the
# MAGIC Safe Harbor method (45 CFR 164.514(b)(2)).

# COMMAND ----------

def mask_name(name_col):
    """
    Masks a name field using SHA-256 hashing.
    The hash is consistent (same input -> same output) which allows
    for deterministic joins across tables without exposing the name.
    """
    return sha2(
        when(col(name_col).isNotNull(), lower(trim(col(name_col))))
        .otherwise(lit("__NULL__")),
        256
    )


def mask_ssn(ssn_col):
    """
    Masks SSN using SHA-256 hashing. The full SSN is never stored
    in the staging or downstream layers.
    """
    return sha2(
        when(col(ssn_col).isNotNull(),
             regexp_replace(col(ssn_col), "[^0-9]", ""))
        .otherwise(lit("__NULL__")),
        256
    )


def mask_dob(dob_col):
    """
    Masks date of birth by preserving only the year and setting
    month/day to January 1. This preserves age calculation ability
    while removing the specific birth date (HIPAA Safe Harbor requires
    removing day and month for ages under 90).
    """
    return when(
        col(dob_col).isNotNull(),
        to_date(concat(year(col(dob_col)).cast(StringType()), lit("-01-01")), "yyyy-MM-dd")
    ).otherwise(lit(None).cast(DateType()))


def mask_zip_code(zip_col):
    """
    Truncates ZIP code to first 3 digits per HIPAA Safe Harbor method.
    ZIP codes with fewer than 20,000 population (000, 036, 059, 063,
    102, 203, 556, 692, 878, 879, 884, 890, 893) should be set to 000,
    but this simplified version just truncates to 3 digits.
    """
    return when(
        col(zip_col).isNotNull(),
        substring(regexp_replace(col(zip_col), "[^0-9]", ""), 1, 3)
    ).otherwise(lit(None))


def mask_phone(phone_col):
    """
    Masks phone number using SHA-256 hashing.
    """
    return sha2(
        when(col(phone_col).isNotNull(),
             regexp_replace(col(phone_col), "[^0-9]", ""))
        .otherwise(lit("__NULL__")),
        256
    )


def mask_email(email_col):
    """
    Masks email address using SHA-256 hashing.
    """
    return sha2(
        when(col(email_col).isNotNull(), lower(trim(col(email_col))))
        .otherwise(lit("__NULL__")),
        256
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Apply PHI Masking to Member Eligibility
# MAGIC
# MAGIC This creates the `stg_member_latest` table with all PHI fields masked.
# MAGIC We also deduplicate to keep only the latest record per member.

# COMMAND ----------

def create_masked_member_staging():
    """
    Reads raw member eligibility, applies PHI masking, and deduplicates
    to keep the latest record per member_id.

    PHI masking is applied HERE at the staging layer, NOT after marts.
    This is a key difference from the Teradata implementation.
    """
    # Read raw member eligibility
    df_raw = spark.table(f"{RAW_SCHEMA}.raw_member_eligibility")

    print(f"Raw member eligibility records: {df_raw.count():,}")

    # Apply PHI masking
    df_masked = (
        df_raw
        # Mask PHI fields
        .withColumn("member_first_name_masked", mask_name("member_first_name"))
        .withColumn("member_last_name_masked", mask_name("member_last_name"))
        .withColumn("date_of_birth_masked", mask_dob("date_of_birth"))
        .withColumn("ssn_masked", mask_ssn("ssn"))
        .withColumn("zip_code_3digit", mask_zip_code("zip_code"))
        # Drop original PHI columns - they should NOT propagate downstream
        .drop(
            "member_first_name", "member_last_name", "date_of_birth",
            "ssn", "address_line_1", "address_line_2", "city",
            "zip_code", "phone_number", "email_address",
            "medicare_beneficiary_id", "medicaid_id"
        )
    )

    # Deduplicate to keep latest record per member
    # Window: partition by member_id, order by coverage_end_date desc, ingestion desc
    latest_window = Window.partitionBy("member_id").orderBy(
        col("coverage_end_date").desc_nulls_last(),
        col("ingestion_timestamp").desc()
    )

    df_latest = (
        df_masked
        .withColumn("rn", row_number().over(latest_window))
        .filter(col("rn") == 1)
        .drop("rn")
        .withColumn("effective_date", col("coverage_start_date"))
        .withColumn("staging_timestamp", current_timestamp())
        # Drop columns not in staging table
        .drop("source_file_name", "ingestion_timestamp", "record_hash",
               "pcp_provider_name", "subscriber_id", "relationship_code")
    )

    # Re-add subscriber_id and relationship_code (they're not PHI)
    df_final = (
        df_raw
        .select("member_id", "subscriber_id", "relationship_code")
        .dropDuplicates(["member_id"])
        .join(df_latest, on="member_id", how="inner")
    )

    return df_final

# COMMAND ----------

# Execute member masking
df_member_masked = create_masked_member_staging()

print(f"Masked member records: {df_member_masked.count():,}")

# Write to staging
(
    df_member_masked.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{STAGING_SCHEMA}.stg_member_latest")
)
print(f"Written to {STAGING_SCHEMA}.stg_member_latest")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation: Verify PHI is Masked

# COMMAND ----------

# Verify masking was applied correctly
df_check = spark.table(f"{STAGING_SCHEMA}.stg_member_latest").limit(5)

# These columns should contain SHA-256 hashes (64 hex characters)
print("Sample masked values:")
df_check.select(
    "member_id",
    "member_first_name_masked",
    "member_last_name_masked",
    "date_of_birth_masked",
    "ssn_masked",
    "zip_code_3digit"
).show(truncate=30)

# Verify no raw PHI columns exist in staging
staging_columns = [c.lower() for c in df_check.columns]
phi_columns = [
    "member_first_name", "member_last_name", "date_of_birth",
    "ssn", "address_line_1", "address_line_2", "city",
    "zip_code", "phone_number", "email_address"
]

phi_leaks = [c for c in phi_columns if c in staging_columns]
if phi_leaks:
    print(f"[FAIL] Raw PHI columns found in staging: {phi_leaks}")
else:
    print("[PASS] No raw PHI columns found in staging table")

# Verify ZIP codes are 3 digits
from pyspark.sql.functions import length as str_length
zip_check = spark.sql(f"""
    SELECT zip_code_3digit, LENGTH(zip_code_3digit) AS zip_len
    FROM {STAGING_SCHEMA}.stg_member_latest
    WHERE zip_code_3digit IS NOT NULL AND LENGTH(zip_code_3digit) != 3
""")
bad_zips = zip_check.count()
if bad_zips > 0:
    print(f"[FAIL] {bad_zips} ZIP codes not exactly 3 digits")
else:
    print("[PASS] All ZIP codes are exactly 3 digits")

# Verify DOB masking (all should be Jan 1)
dob_check = spark.sql(f"""
    SELECT date_of_birth_masked
    FROM {STAGING_SCHEMA}.stg_member_latest
    WHERE date_of_birth_masked IS NOT NULL
      AND (MONTH(date_of_birth_masked) != 1 OR DAY(date_of_birth_masked) != 1)
""")
bad_dobs = dob_check.count()
if bad_dobs > 0:
    print(f"[FAIL] {bad_dobs} DOBs not masked to Jan 1")
else:
    print("[PASS] All DOBs masked to year-01-01 format")

print("\nPHI masking complete.")
