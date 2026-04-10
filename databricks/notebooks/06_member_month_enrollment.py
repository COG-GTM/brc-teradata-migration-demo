# Databricks notebook source
# MAGIC %md
# MAGIC # 06 - Member Month Enrollment Generation
# MAGIC
# MAGIC This notebook generates the `mart_member_months` table, which contains
# MAGIC one row per member per enrolled month. This is a fundamental building
# MAGIC block for healthcare analytics, enabling:
# MAGIC
# MAGIC - **PMPM (Per Member Per Month)** cost calculations
# MAGIC - Enrollment denominators for utilization rates
# MAGIC - Member month counts for actuarial analysis
# MAGIC - Continuous enrollment tracking
# MAGIC
# MAGIC ## Approach
# MAGIC Uses PySpark `date_add` and `months_between` to expand each member's
# MAGIC coverage period into individual monthly rows.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col, lit, when, explode, sequence, to_date, date_trunc,
    months_between, add_months, year, month, floor, concat,
    lpad, current_timestamp, datediff, row_number,
    coalesce, date_format
)
from pyspark.sql.types import IntegerType, StringType, DateType

STAGING_SCHEMA = "claims_staging"
WAREHOUSE_SCHEMA = "claims_warehouse"
MART_SCHEMA = "claims_mart"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Member Month Rows
# MAGIC
# MAGIC For each member's coverage period, generate one row per month they
# MAGIC were enrolled. Uses PySpark's `sequence` function to generate the
# MAGIC month series and `explode` to create individual rows.

# COMMAND ----------

def generate_member_months(df_members, df_eligibility):
    """
    Generates one row per member per enrolled month.

    Steps:
    1. For each eligibility record, compute the first and last enrolled month
    2. Use sequence() to generate an array of monthly dates
    3. Explode the array into individual rows
    4. Enrich with member demographics at each point in time
    5. Calculate derived fields (age band, etc.)

    Args:
        df_members: Staged member data (stg_member_latest)
        df_eligibility: Raw eligibility with all coverage periods

    Returns:
        DataFrame with one row per member per enrolled month
    """
    # Get all coverage periods (not just latest)
    df_coverage = spark.table(f"claims_raw.raw_member_eligibility").select(
        "member_id",
        "coverage_start_date",
        "coverage_end_date",
        "plan_id",
        "plan_type",
        "line_of_business",
        "group_id",
        "enrollment_status",
    ).filter(
        col("coverage_start_date").isNotNull() &
        col("enrollment_status").isin("ACTIVE", "COBRA")
    )

    # Cap the coverage end date to avoid generating future months
    df_coverage = df_coverage.withColumn(
        "coverage_end_date",
        when(col("coverage_end_date").isNull(), to_date(lit("2026-12-31")))
        .otherwise(col("coverage_end_date"))
    )

    # Truncate dates to first of month
    df_coverage = (
        df_coverage
        .withColumn("start_month", date_trunc("month", col("coverage_start_date")).cast(DateType()))
        .withColumn("end_month", date_trunc("month", col("coverage_end_date")).cast(DateType()))
    )

    # Generate sequence of months for each coverage period
    df_with_months = df_coverage.withColumn(
        "enrollment_months",
        sequence(col("start_month"), col("end_month"), lit(1).cast("interval 1 month"))
    )

    # Explode into individual month rows
    df_exploded = (
        df_with_months
        .select(
            col("member_id"),
            explode(col("enrollment_months")).alias("enrollment_month"),
            col("plan_id"),
            col("plan_type"),
            col("line_of_business"),
            col("group_id"),
            col("enrollment_status"),
        )
        .withColumn("enrollment_month", col("enrollment_month").cast(DateType()))
    )

    # Deduplicate: if a member has overlapping coverage periods, keep one row per month
    dedup_window = Window.partitionBy("member_id", "enrollment_month").orderBy(
        col("enrollment_status").asc(),  # ACTIVE before COBRA
        col("plan_id").asc()
    )

    df_deduped = (
        df_exploded
        .withColumn("rn", row_number().over(dedup_window))
        .filter(col("rn") == 1)
        .drop("rn")
    )

    # Join with member demographics from staging (masked PHI)
    df_enriched = (
        df_deduped.alias("mm")
        .join(
            df_members.alias("m"),
            on="member_id",
            how="inner"
        )
        .select(
            col("mm.member_id"),
            col("m.member_key"),
            col("mm.enrollment_month"),
            year(col("mm.enrollment_month")).alias("enrollment_year"),
            month(col("mm.enrollment_month")).alias("enrollment_month_number"),
            date_format(col("mm.enrollment_month"), "yyyy-MM").alias("enrollment_year_month"),
            col("mm.plan_id"),
            col("mm.plan_type"),
            col("mm.line_of_business"),
            col("mm.group_id"),
            col("m.gender"),
            col("m.state_code"),
            col("m.zip_code_3digit"),
            col("m.risk_score"),
            col("m.pcp_provider_id"),
            col("mm.enrollment_status"),
            col("m.date_of_birth_masked"),
        )
    )

    # Calculate age at each month
    df_with_age = df_enriched.withColumn(
        "age_at_month",
        floor(months_between(col("enrollment_month"), col("date_of_birth_masked")) / 12).cast(IntegerType())
    )

    # Assign age bands
    df_with_age_band = df_with_age.withColumn(
        "age_band",
        when(col("age_at_month") < 18, lit("0-17"))
        .when(col("age_at_month") < 26, lit("18-25"))
        .when(col("age_at_month") < 35, lit("26-34"))
        .when(col("age_at_month") < 45, lit("35-44"))
        .when(col("age_at_month") < 55, lit("45-54"))
        .when(col("age_at_month") < 65, lit("55-64"))
        .otherwise(lit("65+"))
    )

    # Add is_enrolled flag and timestamp
    df_final = (
        df_with_age_band
        .withColumn("is_enrolled", lit(True))
        .withColumn("created_timestamp", current_timestamp())
        .drop("date_of_birth_masked")
    )

    return df_final

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execute Member Month Generation

# COMMAND ----------

# Read staged member data
df_members = spark.table(f"{STAGING_SCHEMA}.stg_member_latest")

# Try to read dim_member for surrogate keys, fall back to staging if not populated
try:
    df_dim_member = spark.table(f"{WAREHOUSE_SCHEMA}.dim_member").filter(col("is_current") == True)
    df_member_keys = df_dim_member.select(
        col("member_id"),
        col("member_key"),
        col("gender"),
        col("state_code"),
        col("zip_code_3digit"),
        col("risk_score"),
        col("pcp_provider_id"),
        col("date_of_birth_masked"),
    )
    print("Using dim_member for surrogate keys")
except Exception:
    # If dim_member not yet populated, use staging data with null member_key
    df_member_keys = (
        df_members
        .withColumn("member_key", lit(None).cast("bigint"))
        .select(
            "member_id", "member_key", "gender", "state_code",
            "zip_code_3digit", "risk_score", "pcp_provider_id",
            "date_of_birth_masked"
        )
    )
    print("dim_member not available, using staging data (member_key will be null)")

# Generate member months
df_member_months = generate_member_months(df_member_keys, None)

total_member_months = df_member_months.count()
unique_members = df_member_months.select("member_id").distinct().count()
print(f"\nGenerated {total_member_months:,} member months for {unique_members:,} unique members")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Mart Table

# COMMAND ----------

(
    df_member_months.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{MART_SCHEMA}.mart_member_months")
)
print(f"Written to {MART_SCHEMA}.mart_member_months")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Member Month Statistics

# COMMAND ----------

# Enrollment summary by year
print("Enrollment by Year:")
(
    df_member_months
    .groupBy("enrollment_year")
    .agg(
        spark_count("*").alias("member_months"),
        countDistinct("member_id").alias("unique_members"),
    )
    .orderBy("enrollment_year")
    .show()
)

# Enrollment by line of business
print("\nEnrollment by Line of Business:")
(
    df_member_months
    .groupBy("line_of_business")
    .agg(
        spark_count("*").alias("member_months"),
        countDistinct("member_id").alias("unique_members"),
    )
    .orderBy(col("member_months").desc())
    .show()
)

# Age band distribution
print("\nAge Band Distribution:")
(
    df_member_months
    .groupBy("age_band")
    .agg(spark_count("*").alias("member_months"))
    .orderBy("age_band")
    .show()
)

print("\nMember month enrollment generation complete.")
