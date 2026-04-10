# Databricks notebook source
# MAGIC %md
# MAGIC # 04 - Diagnosis Code Processing
# MAGIC
# MAGIC This notebook handles the processing of diagnosis codes stored as
# MAGIC `ARRAY<STRING>` in Databricks Delta Lake tables. This is a key structural
# MAGIC difference from the Teradata implementation which stores diagnosis codes
# MAGIC in 25 individual columns (icd_diagnosis_code_1..25).
# MAGIC
# MAGIC ## Key Operations
# MAGIC 1. **Explode** the `diagnosis_codes` array into individual rows
# MAGIC 2. **Map** ICD-10 codes to standard descriptions
# MAGIC 3. **Classify** codes into clinical categories (CCS/CCSR)
# MAGIC 4. **Identify** primary vs. secondary diagnoses
# MAGIC 5. Create a diagnosis-level fact table for analytics

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col, explode, explode_outer, posexplode, posexplode_outer,
    when, lit, upper, trim, regexp_replace, length, substring,
    count as spark_count, collect_list, array_distinct,
    row_number, current_timestamp, broadcast
)
from pyspark.sql.types import StringType, StructType, StructField

STAGING_SCHEMA = "claims_staging"
WAREHOUSE_SCHEMA = "claims_warehouse"

# COMMAND ----------

# MAGIC %md
# MAGIC ## ICD-10 Code Reference Data
# MAGIC
# MAGIC Create a reference mapping of ICD-10 codes to descriptions and clinical
# MAGIC categories. In production, this would be loaded from an external reference
# MAGIC table or CMS crosswalk file.

# COMMAND ----------

# ICD-10-CM category mappings (sample - production would use full CMS crosswalk)
# Major Diagnostic Categories (MDC) based on ICD-10 chapter
icd10_chapter_mapping = [
    ("A00-B99", "Infectious and Parasitic Diseases", "INFECTIOUS"),
    ("C00-D49", "Neoplasms", "NEOPLASM"),
    ("D50-D89", "Diseases of Blood and Blood-forming Organs", "BLOOD"),
    ("E00-E89", "Endocrine, Nutritional and Metabolic Diseases", "ENDOCRINE"),
    ("F01-F99", "Mental, Behavioral and Neurodevelopmental Disorders", "MENTAL"),
    ("G00-G99", "Diseases of the Nervous System", "NERVOUS"),
    ("H00-H59", "Diseases of the Eye", "EYE"),
    ("H60-H95", "Diseases of the Ear", "EAR"),
    ("I00-I99", "Diseases of the Circulatory System", "CIRCULATORY"),
    ("J00-J99", "Diseases of the Respiratory System", "RESPIRATORY"),
    ("K00-K95", "Diseases of the Digestive System", "DIGESTIVE"),
    ("L00-L99", "Diseases of the Skin", "SKIN"),
    ("M00-M99", "Diseases of the Musculoskeletal System", "MUSCULOSKELETAL"),
    ("N00-N99", "Diseases of the Genitourinary System", "GENITOURINARY"),
    ("O00-O9A", "Pregnancy, Childbirth and the Puerperium", "PREGNANCY"),
    ("P00-P96", "Conditions Originating in the Perinatal Period", "PERINATAL"),
    ("Q00-Q99", "Congenital Anomalies", "CONGENITAL"),
    ("R00-R99", "Symptoms and Signs", "SYMPTOMS"),
    ("S00-T88", "Injury, Poisoning and External Causes", "INJURY"),
    ("V00-Y99", "External Causes of Morbidity", "EXTERNAL"),
    ("Z00-Z99", "Factors Influencing Health Status", "HEALTH_STATUS"),
]

# Sample high-frequency ICD-10 codes with descriptions
sample_icd10_codes = [
    ("E11.9", "Type 2 diabetes mellitus without complications", "ENDOCRINE"),
    ("E11.65", "Type 2 diabetes mellitus with hyperglycemia", "ENDOCRINE"),
    ("I10", "Essential (primary) hypertension", "CIRCULATORY"),
    ("I25.10", "Atherosclerotic heart disease of native coronary artery", "CIRCULATORY"),
    ("J06.9", "Acute upper respiratory infection, unspecified", "RESPIRATORY"),
    ("J18.9", "Pneumonia, unspecified organism", "RESPIRATORY"),
    ("J44.1", "Chronic obstructive pulmonary disease with acute exacerbation", "RESPIRATORY"),
    ("K21.0", "Gastro-esophageal reflux disease with esophagitis", "DIGESTIVE"),
    ("M54.5", "Low back pain", "MUSCULOSKELETAL"),
    ("M17.11", "Primary osteoarthritis, right knee", "MUSCULOSKELETAL"),
    ("F32.9", "Major depressive disorder, single episode, unspecified", "MENTAL"),
    ("F41.1", "Generalized anxiety disorder", "MENTAL"),
    ("N18.3", "Chronic kidney disease, stage 3", "GENITOURINARY"),
    ("Z87.891", "Personal history of nicotine dependence", "HEALTH_STATUS"),
    ("Z79.4", "Long term (current) use of insulin", "HEALTH_STATUS"),
    ("R06.02", "Shortness of breath", "SYMPTOMS"),
    ("R10.9", "Unspecified abdominal pain", "SYMPTOMS"),
]

# Create reference DataFrames
df_icd10_ref = spark.createDataFrame(
    sample_icd10_codes,
    ["diagnosis_code", "diagnosis_description", "diagnosis_category"]
)

print(f"ICD-10 reference codes loaded: {df_icd10_ref.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Explode Diagnosis Codes Array
# MAGIC
# MAGIC Uses `posexplode` to convert the `ARRAY<STRING>` column into individual
# MAGIC rows while preserving the position (ordinal) of each code within the array.
# MAGIC Position 0 = primary/principal diagnosis.

# COMMAND ----------

def explode_diagnosis_codes(df_claims):
    """
    Explodes the diagnosis_codes ARRAY<STRING> column into individual rows.

    Uses posexplode() to preserve the ordinal position of each code:
    - Position 0: Primary/principal diagnosis
    - Position 1+: Secondary diagnoses

    This is the key transformation that bridges the structural gap between
    Databricks (ARRAY column) and Teradata (25 individual columns).
    """
    # Use posexplode to get both position and value
    df_exploded = df_claims.select(
        col("claim_id"),
        col("claim_line_number"),
        col("patient_id"),
        col("claim_type"),
        col("claim_status"),
        col("claim_start_date"),
        col("claim_end_date"),
        col("plan_id"),
        col("line_of_business"),
        col("paid_amount"),
        col("allowed_amount"),
        posexplode_outer(col("diagnosis_codes")).alias("diagnosis_position", "diagnosis_code_raw"),
    )

    # Clean and standardize diagnosis codes
    df_cleaned = (
        df_exploded
        .withColumn(
            "diagnosis_code",
            upper(trim(regexp_replace(col("diagnosis_code_raw"), "[^A-Za-z0-9.]", "")))
        )
        .filter(
            col("diagnosis_code").isNotNull() &
            (length(col("diagnosis_code")) >= 3)
        )
        .withColumn(
            "is_principal_diagnosis",
            when(col("diagnosis_position") == 0, lit(True)).otherwise(lit(False))
        )
        .withColumn(
            "diagnosis_rank",
            col("diagnosis_position") + 1  # 1-based rank
        )
        .drop("diagnosis_code_raw")
    )

    return df_cleaned

# COMMAND ----------

# Read staged medical claims
df_claims = spark.table(f"{STAGING_SCHEMA}.stg_medical_claim_current")

print(f"Total staged medical claims: {df_claims.count():,}")
print(f"Claims with diagnosis codes: {df_claims.filter(col('diagnosis_codes').isNotNull()).count():,}")

# Explode diagnosis codes
df_dx_exploded = explode_diagnosis_codes(df_claims)

print(f"Total diagnosis code rows after explode: {df_dx_exploded.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Map Diagnosis Codes to Descriptions and Categories

# COMMAND ----------

def classify_diagnosis_code(diagnosis_code):
    """
    Classifies an ICD-10 code into its chapter/category based on the
    first character of the code. This provides high-level clinical grouping.
    """
    if diagnosis_code is None or len(diagnosis_code) < 1:
        return "UNKNOWN"

    first_char = diagnosis_code[0].upper()

    chapter_map = {
        "A": "INFECTIOUS", "B": "INFECTIOUS",
        "C": "NEOPLASM", "D": "NEOPLASM",
        "E": "ENDOCRINE",
        "F": "MENTAL",
        "G": "NERVOUS",
        "H": "EYE_EAR",
        "I": "CIRCULATORY",
        "J": "RESPIRATORY",
        "K": "DIGESTIVE",
        "L": "SKIN",
        "M": "MUSCULOSKELETAL",
        "N": "GENITOURINARY",
        "O": "PREGNANCY",
        "P": "PERINATAL",
        "Q": "CONGENITAL",
        "R": "SYMPTOMS",
        "S": "INJURY", "T": "INJURY",
        "V": "EXTERNAL", "W": "EXTERNAL", "X": "EXTERNAL", "Y": "EXTERNAL",
        "Z": "HEALTH_STATUS",
    }

    return chapter_map.get(first_char, "UNKNOWN")


# Register UDF for classification
from pyspark.sql.functions import udf
classify_dx_udf = udf(classify_diagnosis_code, StringType())
spark.udf.register("classify_diagnosis_code", classify_diagnosis_code, StringType())

# COMMAND ----------

# Join with reference data for descriptions and add category classification
df_dx_enriched = (
    df_dx_exploded
    # Left join with ICD-10 reference for descriptions
    .join(
        broadcast(df_icd10_ref),
        on="diagnosis_code",
        how="left"
    )
    # Apply chapter-level classification for codes not in reference
    .withColumn(
        "diagnosis_category",
        when(col("diagnosis_category").isNotNull(), col("diagnosis_category"))
        .otherwise(classify_dx_udf(col("diagnosis_code")))
    )
    .withColumn(
        "diagnosis_description",
        when(col("diagnosis_description").isNotNull(), col("diagnosis_description"))
        .otherwise(lit("Description not available"))
    )
    # Add the 3-character category code (first 3 chars of ICD-10)
    .withColumn(
        "diagnosis_code_3char",
        substring(col("diagnosis_code"), 1, 3)
    )
    .withColumn("created_timestamp", current_timestamp())
)

print(f"Enriched diagnosis records: {df_dx_enriched.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write Diagnosis Fact Table

# COMMAND ----------

# Write the exploded and enriched diagnosis data
(
    df_dx_enriched.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{WAREHOUSE_SCHEMA}.fct_claim_diagnosis")
)
print(f"Written to {WAREHOUSE_SCHEMA}.fct_claim_diagnosis")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Diagnosis Analytics Summary

# COMMAND ----------

# Top diagnosis categories by claim count
print("Top 10 Diagnosis Categories by Claim Count:")
(
    df_dx_enriched
    .groupBy("diagnosis_category")
    .agg(
        spark_count("*").alias("diagnosis_count"),
        spark_count("claim_id").alias("claim_count"),
    )
    .orderBy(col("diagnosis_count").desc())
    .limit(10)
    .show(truncate=False)
)

# Top specific diagnosis codes
print("\nTop 15 Diagnosis Codes by Frequency:")
(
    df_dx_enriched
    .groupBy("diagnosis_code", "diagnosis_description")
    .agg(spark_count("*").alias("frequency"))
    .orderBy(col("frequency").desc())
    .limit(15)
    .show(truncate=False)
)

# Principal vs secondary diagnosis distribution
print("\nPrincipal vs Secondary Diagnosis Distribution:")
(
    df_dx_enriched
    .groupBy("is_principal_diagnosis")
    .agg(spark_count("*").alias("count"))
    .show()
)

# Average number of diagnosis codes per claim
print("\nAverage diagnosis codes per claim:")
(
    df_dx_enriched
    .groupBy("claim_id")
    .agg(spark_count("*").alias("dx_count"))
    .select("dx_count")
    .describe()
    .show()
)

print("\nDiagnosis code processing complete.")
