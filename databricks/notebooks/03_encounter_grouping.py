# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Encounter Grouping (Gap-and-Island Algorithm)
# MAGIC
# MAGIC This notebook implements **correct** encounter grouping using a proper
# MAGIC gap-and-island algorithm with PySpark window functions. Claims with
# MAGIC overlapping or contiguous date ranges are grouped into logical encounters.
# MAGIC
# MAGIC ## Why This Matters
# MAGIC The Teradata implementation uses a naive 30-day window grouping that
# MAGIC incorrectly groups claims that are close in time but not truly overlapping.
# MAGIC This Databricks implementation uses the correct approach:
# MAGIC
# MAGIC 1. Sort claims by patient and start date
# MAGIC 2. Use `LAG` to compare each claim's start date against the running
# MAGIC    maximum end date of prior claims
# MAGIC 3. When a claim starts AFTER the running max end date, it begins a new
# MAGIC    encounter (island)
# MAGIC 4. Assign encounter group IDs to each island
# MAGIC
# MAGIC This correctly handles:
# MAGIC - Truly overlapping date ranges
# MAGIC - Contiguous (adjacent) date ranges
# MAGIC - Gaps between service periods
# MAGIC - Multiple overlapping claims of varying lengths

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col, row_number, lag, lead, when, lit, sum as spark_sum,
    max as spark_max, min as spark_min, count as spark_count,
    collect_set, flatten, array_distinct, first, datediff,
    concat, lpad, monotonically_increasing_id, current_timestamp,
    coalesce, date_add
)
from pyspark.sql.types import IntegerType, StringType

STAGING_SCHEMA = "claims_staging"
WAREHOUSE_SCHEMA = "claims_warehouse"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gap-and-Island Algorithm Implementation
# MAGIC
# MAGIC The algorithm works in four steps:
# MAGIC 1. **Prepare**: Sort claims by patient and date, compute running max end date
# MAGIC 2. **Detect gaps**: Mark where a new island begins (start > running max end)
# MAGIC 3. **Assign groups**: Cumulative sum of gap markers = encounter group ID
# MAGIC 4. **Aggregate**: Roll up claims within each encounter group

# COMMAND ----------

def detect_encounter_boundaries(df_claims):
    """
    Step 1-2: Detect encounter boundaries using the gap-and-island approach.

    For each claim within a patient, we compute the running maximum end date
    from all prior claims. If the current claim's start date is AFTER that
    running max end date, it begins a new encounter.

    This correctly handles:
    - Claim A: Jan 1-5, Claim B: Jan 3-8 -> same encounter (overlapping)
    - Claim A: Jan 1-5, Claim B: Jan 6-10 -> same encounter (contiguous)
    - Claim A: Jan 1-5, Claim B: Jan 15-20 -> different encounters (gap)
    - Multiple overlapping claims of different lengths
    """
    # Window ordered by start date, then end date for tie-breaking
    patient_date_window = Window.partitionBy("patient_id").orderBy(
        col("claim_start_date").asc(),
        col("claim_end_date").asc(),
        col("claim_id").asc()
    )

    # Window for running max (all rows up to current)
    patient_running_window = (
        Window.partitionBy("patient_id")
        .orderBy(
            col("claim_start_date").asc(),
            col("claim_end_date").asc(),
            col("claim_id").asc()
        )
        .rowsBetween(Window.unboundedPreceding, -1)
    )

    # Step 1: Compute the running maximum end date from all PRIOR claims
    # Use -1 upper bound to exclude the current row
    df_with_running_max = df_claims.withColumn(
        "prior_max_end_date",
        spark_max(col("claim_end_date")).over(patient_running_window)
    )

    # Step 2: Mark encounter boundaries
    # A new encounter starts when:
    #   - There is no prior claim (first claim for patient), OR
    #   - Current start date is AFTER the running max end date of prior claims
    # We use > (not >=) because contiguous dates (end = next start) are the
    # same encounter (e.g., discharge Jan 5, new service Jan 6 is contiguous
    # if we use a 1-day tolerance)
    df_with_boundaries = df_with_running_max.withColumn(
        "is_new_encounter",
        when(
            col("prior_max_end_date").isNull(), lit(1)
        ).when(
            col("claim_start_date") > date_add(col("prior_max_end_date"), 1),
            lit(1)
        ).otherwise(lit(0))
    )

    return df_with_boundaries

# COMMAND ----------

def assign_encounter_groups(df_with_boundaries):
    """
    Step 3: Assign encounter group IDs using cumulative sum of boundary markers.

    Each time is_new_encounter = 1, the cumulative sum increments, creating
    a new group ID. All claims between two boundary markers share the same
    encounter group.
    """
    # Window for cumulative sum of encounter boundary markers
    patient_window = Window.partitionBy("patient_id").orderBy(
        col("claim_start_date").asc(),
        col("claim_end_date").asc(),
        col("claim_id").asc()
    )

    df_with_groups = df_with_boundaries.withColumn(
        "encounter_group_num",
        spark_sum(col("is_new_encounter")).over(patient_window)
    )

    # Generate a unique encounter ID combining patient_id and group number
    df_with_encounter_id = df_with_groups.withColumn(
        "encounter_id",
        concat(
            col("patient_id"),
            lit("-ENC-"),
            lpad(col("encounter_group_num").cast(StringType()), 6, "0")
        )
    )

    return df_with_encounter_id

# COMMAND ----------

def aggregate_encounters(df_with_encounter_id):
    """
    Step 4: Aggregate claims within each encounter group to produce
    encounter-level summary records.
    """
    # Get the first claim's details (by start date) for each encounter
    first_claim_window = Window.partitionBy(
        "patient_id", "encounter_id"
    ).orderBy(
        col("claim_start_date").asc(),
        col("claim_id").asc()
    )

    df_with_rank = df_with_encounter_id.withColumn(
        "claim_rank_in_encounter",
        row_number().over(first_claim_window)
    )

    # Aggregate to encounter level
    df_encounters = (
        df_with_encounter_id
        .groupBy("patient_id", "encounter_id")
        .agg(
            # Date range
            spark_min("claim_start_date").alias("encounter_start_date"),
            spark_max("claim_end_date").alias("encounter_end_date"),
            # Counts
            spark_count("*").alias("claim_line_count"),
            spark_count(col("claim_id")).alias("claim_count"),
            # Diagnosis codes - collect all unique codes across the encounter
            array_distinct(flatten(collect_set("diagnosis_codes"))).alias("diagnosis_codes_all"),
            # Financial aggregates
            spark_sum("billed_amount").alias("total_billed_amount"),
            spark_sum("allowed_amount").alias("total_allowed_amount"),
            spark_sum("paid_amount").alias("total_paid_amount"),
            spark_sum("member_liability_amount").alias("total_member_liability"),
            # Keep plan/LOB from first claim
            first("plan_id").alias("plan_id"),
            first("line_of_business").alias("line_of_business"),
            first("rendering_provider_npi").alias("primary_provider_npi"),
            first("facility_npi").alias("facility_npi"),
        )
    )

    # Add computed columns
    df_encounters = (
        df_encounters
        .withColumn(
            "length_of_encounter_days",
            datediff(col("encounter_end_date"), col("encounter_start_date")) + 1
        )
        .withColumn(
            "encounter_type",
            when(col("length_of_encounter_days") > 1, lit("INPATIENT"))
            .when(col("facility_npi").isNotNull(), lit("OUTPATIENT"))
            .otherwise(lit("OFFICE"))
        )
        .withColumn("grouping_method", lit("gap_and_island"))
        .withColumn("created_timestamp", current_timestamp())
    )

    # Get principal diagnosis from the first (earliest) claim
    df_first_claims = (
        df_with_rank
        .filter(col("claim_rank_in_encounter") == 1)
        .select(
            col("patient_id").alias("fc_patient_id"),
            col("encounter_id").alias("fc_encounter_id"),
            col("principal_diagnosis_code"),
            col("rendering_provider_npi").alias("fc_provider_npi"),
        )
    )

    df_encounters = (
        df_encounters
        .join(
            df_first_claims,
            (df_encounters.patient_id == df_first_claims.fc_patient_id) &
            (df_encounters.encounter_id == df_first_claims.fc_encounter_id),
            "left"
        )
        .drop("fc_patient_id", "fc_encounter_id", "fc_provider_npi")
    )

    return df_encounters

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execute Encounter Grouping Pipeline

# COMMAND ----------

# Read staged medical claims
df_claims = spark.table(f"{STAGING_SCHEMA}.stg_medical_claim_current")

# Filter to paid/adjusted claims only (denied/reversed claims are excluded from encounters)
df_claims_for_encounters = df_claims.filter(
    col("claim_status").isin("PAID", "ADJUSTED")
)

# Ensure dates are not null
df_claims_for_encounters = df_claims_for_encounters.filter(
    col("claim_start_date").isNotNull() & col("claim_end_date").isNotNull()
)

# Handle cases where end_date < start_date (data quality issue)
df_claims_for_encounters = df_claims_for_encounters.withColumn(
    "claim_end_date",
    when(col("claim_end_date") < col("claim_start_date"), col("claim_start_date"))
    .otherwise(col("claim_end_date"))
)

print(f"Claims eligible for encounter grouping: {df_claims_for_encounters.count():,}")

# COMMAND ----------

# Step 1-2: Detect encounter boundaries
df_with_boundaries = detect_encounter_boundaries(df_claims_for_encounters)

# Step 3: Assign encounter group IDs
df_with_groups = assign_encounter_groups(df_with_boundaries)

# Step 4: Aggregate into encounters
df_encounters = aggregate_encounters(df_with_groups)

print(f"Total encounters created: {df_encounters.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write Encounters to Warehouse

# COMMAND ----------

# Write encounter facts
(
    df_encounters.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{WAREHOUSE_SCHEMA}.fct_encounter")
)
print(f"Written encounters to {WAREHOUSE_SCHEMA}.fct_encounter")

# Also update medical claims with encounter_id for cross-referencing
df_claims_with_encounters = (
    df_with_groups
    .select("claim_id", "claim_line_number", "encounter_id")
)

# Store the claim-to-encounter mapping for use by fct_medical_claim
(
    df_claims_with_encounters.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(f"{STAGING_SCHEMA}.claim_encounter_mapping")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Encounter Statistics and Validation

# COMMAND ----------

# Encounter type distribution
print("Encounter type distribution:")
df_encounters.groupBy("encounter_type").agg(
    spark_count("*").alias("encounter_count"),
    spark_sum("total_paid_amount").alias("total_paid"),
).orderBy("encounter_type").show()

# Length of stay distribution for inpatient encounters
print("\nInpatient length of stay distribution:")
df_encounters.filter(col("encounter_type") == "INPATIENT").select(
    "length_of_encounter_days"
).describe().show()

# Claims per encounter distribution
print("\nClaims per encounter distribution:")
df_encounters.select("claim_line_count").describe().show()

# Validation: every claim should map to exactly one encounter
total_claims = df_claims_for_encounters.count()
mapped_claims = df_claims_with_encounters.count()
print(f"\nValidation: {mapped_claims:,} / {total_claims:,} claims mapped to encounters")

print("\nEncounter grouping complete.")
