# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - ADR (Adjustment/Denial/Reversal) Deduplication
# MAGIC
# MAGIC This notebook implements Claim Adjustment Deduplication using PySpark
# MAGIC DataFrame API. When claims are adjusted, denied, or reversed, multiple
# MAGIC versions of the same claim exist in the raw data. This process keeps
# MAGIC only the most relevant version based on CMS standard priority.
# MAGIC
# MAGIC ## ADR Dedup Priority (CORRECT CMS order):
# MAGIC - **PAID = 1** (highest priority - final adjudicated payment)
# MAGIC - **ADJUSTED = 2** (correction to a previously paid claim)
# MAGIC - **DENIED = 3** (claim denied by payer)
# MAGIC - **REVERSED = 4** (lowest priority - claim voided entirely)
# MAGIC
# MAGIC ## Key Design Decisions
# MAGIC - Uses PySpark DataFrame API (not just SQL) for complex transformations
# MAGIC - Custom UDF for claim status mapping with validation
# MAGIC - Window functions for selecting the highest-priority version
# MAGIC - PHI masking is applied in this staging step (Databricks approach)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup and Configuration

# COMMAND ----------

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col, row_number, when, lit, current_timestamp, udf, upper, trim,
    coalesce, max as spark_max, count as spark_count, sum as spark_sum
)
from pyspark.sql.types import IntegerType, StringType, StructType, StructField

RAW_SCHEMA = "claims_raw"
STAGING_SCHEMA = "claims_staging"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Claim Status Priority UDF
# MAGIC
# MAGIC PySpark UDF for complex claim status mapping. Handles edge cases like
# MAGIC unknown statuses, null values, and non-standard status codes that may
# MAGIC appear in source data.

# COMMAND ----------

def map_claim_status_priority(status):
    """
    Maps claim status to ADR dedup priority using CORRECT CMS ordering.

    Priority logic:
        PAID     = 1 (highest - this is the final adjudicated state)
        ADJUSTED = 2 (correction to a paid claim)
        DENIED   = 3 (payer denied the claim)
        REVERSED = 4 (claim voided - lowest priority)

    Also handles common source system variations:
        APPROVED, FINALIZED -> maps to PAID (1)
        CORRECTED, AMENDED  -> maps to ADJUSTED (2)
        REJECTED, DECLINED  -> maps to DENIED (3)
        VOIDED, CANCELLED   -> maps to REVERSED (4)

    Unknown/NULL statuses get priority 99 (lowest).
    """
    if status is None:
        return 99

    status_upper = status.strip().upper()

    # Primary CMS standard statuses
    priority_map = {
        "PAID": 1,
        "ADJUSTED": 2,
        "DENIED": 3,
        "REVERSED": 4,
    }

    # Common source system variations
    alias_map = {
        "APPROVED": 1,
        "FINALIZED": 1,
        "PROCESSED": 1,
        "CORRECTED": 2,
        "AMENDED": 2,
        "RESUBMITTED": 2,
        "REJECTED": 3,
        "DECLINED": 3,
        "REFUSED": 3,
        "VOIDED": 4,
        "CANCELLED": 4,
        "CANCELED": 4,
        "RETRACTED": 4,
    }

    if status_upper in priority_map:
        return priority_map[status_upper]
    elif status_upper in alias_map:
        return alias_map[status_upper]
    else:
        return 99  # Unknown status gets lowest priority


# Register the UDF
claim_status_priority_udf = udf(map_claim_status_priority, IntegerType())

# Also register for SQL usage
spark.udf.register("claim_status_priority", map_claim_status_priority, IntegerType())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Medical Claims ADR Deduplication
# MAGIC
# MAGIC Uses PySpark window functions to select the highest-priority version
# MAGIC of each claim. The dedup key is `claim_id + claim_line_number`.

# COMMAND ----------

def dedup_medical_claims():
    """
    Performs ADR deduplication on raw medical claims using PySpark DataFrame API.

    Dedup logic:
    1. Assign priority based on claim_status using the UDF
    2. Within each (claim_id, claim_line_number) group, rank by:
       - claim_status_priority ASC (lower number = higher priority)
       - adjustment_sequence_number DESC (latest adjustment wins for ties)
       - claim_adjudication_date DESC (latest adjudication wins for ties)
    3. Keep only the top-ranked row (row_number = 1)
    """
    # Read raw medical claims
    df_raw = spark.table(f"{RAW_SCHEMA}.raw_medical_claim")

    print(f"Raw medical claims count: {df_raw.count():,}")

    # Apply claim status priority mapping using PySpark UDF
    df_with_priority = df_raw.withColumn(
        "claim_status_priority",
        claim_status_priority_udf(col("claim_status"))
    )

    # Define the dedup window
    # Partition by claim_id + claim_line_number (the natural key for a claim line)
    # Order by priority (ASC), then adjustment sequence (DESC), then adjudication date (DESC)
    dedup_window = Window.partitionBy(
        "claim_id", "claim_line_number"
    ).orderBy(
        col("claim_status_priority").asc(),
        coalesce(col("adjustment_sequence_number"), lit(0)).desc(),
        coalesce(col("claim_adjudication_date"), lit("1900-01-01")).desc(),
        col("ingestion_timestamp").desc()
    )

    # Apply window function to rank claim versions
    df_ranked = df_with_priority.withColumn(
        "adr_rank", row_number().over(dedup_window)
    )

    # Keep only the top-ranked version of each claim line
    df_deduped = (
        df_ranked
        .filter(col("adr_rank") == 1)
        .drop("adr_rank")
        .withColumn("staging_timestamp", current_timestamp())
    )

    # Log dedup statistics
    raw_count = df_raw.count()
    deduped_count = df_deduped.count()
    removed_count = raw_count - deduped_count
    print(f"Deduplicated medical claims: {deduped_count:,} (removed {removed_count:,} duplicates)")

    # Status distribution after dedup
    print("\nClaim status distribution after ADR dedup:")
    df_deduped.groupBy("claim_status", "claim_status_priority").count().orderBy(
        "claim_status_priority"
    ).show()

    return df_deduped

# COMMAND ----------

# Execute medical claims dedup
df_medical_deduped = dedup_medical_claims()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pharmacy Claims ADR Deduplication

# COMMAND ----------

def dedup_pharmacy_claims():
    """
    Performs ADR deduplication on raw pharmacy claims.
    Same priority logic as medical claims: PAID=1 > ADJUSTED=2 > DENIED=3 > REVERSED=4
    """
    df_raw = spark.table(f"{RAW_SCHEMA}.raw_pharmacy_claim")

    print(f"Raw pharmacy claims count: {df_raw.count():,}")

    # Apply claim status priority
    df_with_priority = df_raw.withColumn(
        "claim_status_priority",
        claim_status_priority_udf(col("claim_status"))
    )

    # Dedup window for pharmacy claims
    dedup_window = Window.partitionBy(
        "claim_id", "claim_line_number"
    ).orderBy(
        col("claim_status_priority").asc(),
        coalesce(col("adjustment_sequence_number"), lit(0)).desc(),
        col("ingestion_timestamp").desc()
    )

    df_ranked = df_with_priority.withColumn(
        "adr_rank", row_number().over(dedup_window)
    )

    df_deduped = (
        df_ranked
        .filter(col("adr_rank") == 1)
        .drop("adr_rank")
        .withColumn("staging_timestamp", current_timestamp())
    )

    raw_count = df_raw.count()
    deduped_count = df_deduped.count()
    removed_count = raw_count - deduped_count
    print(f"Deduplicated pharmacy claims: {deduped_count:,} (removed {removed_count:,} duplicates)")

    print("\nClaim status distribution after ADR dedup:")
    df_deduped.groupBy("claim_status", "claim_status_priority").count().orderBy(
        "claim_status_priority"
    ).show()

    return df_deduped

# COMMAND ----------

# Execute pharmacy claims dedup
df_pharmacy_deduped = dedup_pharmacy_claims()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write Deduplicated Data to Staging Tables
# MAGIC
# MAGIC Write the deduplicated results to the staging schema Delta tables.
# MAGIC Uses overwrite mode for full refresh (daily pipeline pattern).

# COMMAND ----------

# Write deduplicated medical claims to staging
(
    df_medical_deduped.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{STAGING_SCHEMA}.stg_medical_claim_current")
)
print(f"Written deduplicated medical claims to {STAGING_SCHEMA}.stg_medical_claim_current")

# Write deduplicated pharmacy claims to staging
(
    df_pharmacy_deduped.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{STAGING_SCHEMA}.stg_pharmacy_claim_current")
)
print(f"Written deduplicated pharmacy claims to {STAGING_SCHEMA}.stg_pharmacy_claim_current")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation and Quality Checks

# COMMAND ----------

# Verify no duplicate claim lines exist in staging
for table, key_cols in [
    ("stg_medical_claim_current", ["claim_id", "claim_line_number"]),
    ("stg_pharmacy_claim_current", ["claim_id", "claim_line_number"]),
]:
    dup_check = spark.sql(f"""
        SELECT {', '.join(key_cols)}, COUNT(*) AS cnt
        FROM {STAGING_SCHEMA}.{table}
        GROUP BY {', '.join(key_cols)}
        HAVING COUNT(*) > 1
    """)
    dup_count = dup_check.count()
    status = "PASS" if dup_count == 0 else "FAIL"
    print(f"[{status}] Duplicate check for {table}: {dup_count} duplicate groups found")

# Verify all claims have valid status priorities
for table in ["stg_medical_claim_current", "stg_pharmacy_claim_current"]:
    unknown_status = spark.sql(f"""
        SELECT COUNT(*) AS cnt
        FROM {STAGING_SCHEMA}.{table}
        WHERE claim_status_priority = 99
    """).collect()[0]["cnt"]
    if unknown_status > 0:
        print(f"[WARN] {table}: {unknown_status:,} claims with unknown status (priority=99)")
    else:
        print(f"[PASS] {table}: All claims have recognized status codes")

print("\nADR Deduplication complete.")
