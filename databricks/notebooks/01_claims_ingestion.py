# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Claims Data Ingestion Pipeline
# MAGIC
# MAGIC This notebook implements the raw claims data ingestion pipeline using
# MAGIC Databricks Auto Loader (cloudFiles) for streaming file ingestion and
# MAGIC Delta Lake MERGE for deduplication.
# MAGIC
# MAGIC ## Data Sources
# MAGIC - Member eligibility files (CSV/Parquet)
# MAGIC - Medical claims files (CSV/Parquet)
# MAGIC - Pharmacy claims files (CSV/Parquet)
# MAGIC
# MAGIC ## Key Features
# MAGIC - Auto Loader for incremental file processing with schema evolution
# MAGIC - MERGE INTO for upsert/dedup on raw tables
# MAGIC - Ingestion metadata tracking (file name, timestamp)
# MAGIC - Schema enforcement with rescue column for malformed data

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, current_timestamp, input_file_name, lit, sha2, concat_ws,
    to_date, to_timestamp, trim, upper, when, array, split
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType,
    DateType, TimestampType, ArrayType
)

# Configuration - parameterize for different environments
CATALOG = "healthcare_claims"
RAW_SCHEMA = "claims_raw"

# Source file paths - configured via Databricks widgets or job parameters
dbutils.widgets.text("source_base_path", "s3://healthcare-claims-landing/", "Source Base Path")
dbutils.widgets.dropdown("file_format", "csv", ["csv", "parquet", "json"], "File Format")
dbutils.widgets.dropdown("environment", "dev", ["dev", "staging", "prod"], "Environment")

SOURCE_BASE_PATH = dbutils.widgets.get("source_base_path")
FILE_FORMAT = dbutils.widgets.get("file_format")
ENVIRONMENT = dbutils.widgets.get("environment")

# Auto Loader checkpoint locations
CHECKPOINT_BASE = f"dbfs:/checkpoints/{ENVIRONMENT}/claims_ingestion"

print(f"Environment: {ENVIRONMENT}")
print(f"Source path: {SOURCE_BASE_PATH}")
print(f"File format: {FILE_FORMAT}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Schema Definitions
# MAGIC
# MAGIC Define explicit schemas for each source file type. Auto Loader can infer
# MAGIC schemas, but explicit schemas provide better control and error handling.

# COMMAND ----------

# Member eligibility schema
member_eligibility_schema = StructType([
    StructField("member_id", StringType(), False),
    StructField("member_first_name", StringType(), True),
    StructField("member_last_name", StringType(), True),
    StructField("date_of_birth", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("ssn", StringType(), True),
    StructField("address_line_1", StringType(), True),
    StructField("address_line_2", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state_code", StringType(), True),
    StructField("zip_code", StringType(), True),
    StructField("phone_number", StringType(), True),
    StructField("email_address", StringType(), True),
    StructField("plan_id", StringType(), True),
    StructField("plan_name", StringType(), True),
    StructField("plan_type", StringType(), True),
    StructField("line_of_business", StringType(), True),
    StructField("group_id", StringType(), True),
    StructField("group_name", StringType(), True),
    StructField("coverage_start_date", StringType(), True),
    StructField("coverage_end_date", StringType(), True),
    StructField("enrollment_status", StringType(), True),
    StructField("pcp_provider_id", StringType(), True),
    StructField("pcp_provider_name", StringType(), True),
    StructField("subscriber_id", StringType(), True),
    StructField("relationship_code", StringType(), True),
    StructField("medicare_beneficiary_id", StringType(), True),
    StructField("medicaid_id", StringType(), True),
    StructField("risk_score", StringType(), True),
    StructField("source_system", StringType(), True),
])

# Medical claim schema - note diagnosis_codes as pipe-delimited string
# that will be converted to ARRAY<STRING> during ingestion
medical_claim_schema = StructType([
    StructField("claim_id", StringType(), False),
    StructField("claim_line_number", StringType(), True),
    StructField("patient_id", StringType(), False),
    StructField("subscriber_id", StringType(), True),
    StructField("claim_type", StringType(), True),
    StructField("claim_status", StringType(), True),
    StructField("claim_submission_date", StringType(), True),
    StructField("claim_adjudication_date", StringType(), True),
    StructField("claim_start_date", StringType(), True),
    StructField("claim_end_date", StringType(), True),
    StructField("admission_date", StringType(), True),
    StructField("discharge_date", StringType(), True),
    StructField("discharge_status_code", StringType(), True),
    StructField("place_of_service_code", StringType(), True),
    StructField("type_of_bill_code", StringType(), True),
    StructField("revenue_code", StringType(), True),
    StructField("diagnosis_codes", StringType(), True),  # Pipe-delimited string -> ARRAY
    StructField("diagnosis_code_type", StringType(), True),
    StructField("principal_diagnosis_code", StringType(), True),
    StructField("admitting_diagnosis_code", StringType(), True),
    StructField("procedure_code", StringType(), True),
    StructField("procedure_code_type", StringType(), True),
    StructField("procedure_modifier_1", StringType(), True),
    StructField("procedure_modifier_2", StringType(), True),
    StructField("procedure_modifier_3", StringType(), True),
    StructField("procedure_modifier_4", StringType(), True),
    StructField("drg_code", StringType(), True),
    StructField("ndc_code", StringType(), True),
    StructField("rendering_provider_npi", StringType(), True),
    StructField("rendering_provider_name", StringType(), True),
    StructField("rendering_provider_specialty", StringType(), True),
    StructField("billing_provider_npi", StringType(), True),
    StructField("billing_provider_name", StringType(), True),
    StructField("billing_provider_tax_id", StringType(), True),
    StructField("facility_npi", StringType(), True),
    StructField("facility_name", StringType(), True),
    StructField("referring_provider_npi", StringType(), True),
    StructField("billed_amount", StringType(), True),
    StructField("allowed_amount", StringType(), True),
    StructField("paid_amount", StringType(), True),
    StructField("member_liability_amount", StringType(), True),
    StructField("copay_amount", StringType(), True),
    StructField("coinsurance_amount", StringType(), True),
    StructField("deductible_amount", StringType(), True),
    StructField("cob_amount", StringType(), True),
    StructField("units_of_service", StringType(), True),
    StructField("days_of_service", StringType(), True),
    StructField("authorization_number", StringType(), True),
    StructField("referral_number", StringType(), True),
    StructField("original_claim_id", StringType(), True),
    StructField("adjustment_sequence_number", StringType(), True),
    StructField("plan_id", StringType(), True),
    StructField("line_of_business", StringType(), True),
    StructField("network_status", StringType(), True),
    StructField("benefit_code", StringType(), True),
    StructField("source_system", StringType(), True),
])

# Pharmacy claim schema
pharmacy_claim_schema = StructType([
    StructField("claim_id", StringType(), False),
    StructField("claim_line_number", StringType(), True),
    StructField("patient_id", StringType(), False),
    StructField("subscriber_id", StringType(), True),
    StructField("claim_status", StringType(), True),
    StructField("fill_date", StringType(), True),
    StructField("written_date", StringType(), True),
    StructField("ndc_code", StringType(), False),
    StructField("drug_name", StringType(), True),
    StructField("generic_indicator", StringType(), True),
    StructField("therapeutic_class_code", StringType(), True),
    StructField("therapeutic_class_name", StringType(), True),
    StructField("formulary_status", StringType(), True),
    StructField("quantity_dispensed", StringType(), True),
    StructField("days_supply", StringType(), True),
    StructField("refill_number", StringType(), True),
    StructField("daw_code", StringType(), True),
    StructField("compound_code", StringType(), True),
    StructField("prescriber_npi", StringType(), True),
    StructField("prescriber_name", StringType(), True),
    StructField("prescriber_specialty", StringType(), True),
    StructField("pharmacy_npi", StringType(), True),
    StructField("pharmacy_name", StringType(), True),
    StructField("pharmacy_type", StringType(), True),
    StructField("pharmacy_zip_code", StringType(), True),
    StructField("billed_amount", StringType(), True),
    StructField("allowed_amount", StringType(), True),
    StructField("paid_amount", StringType(), True),
    StructField("member_liability_amount", StringType(), True),
    StructField("copay_amount", StringType(), True),
    StructField("coinsurance_amount", StringType(), True),
    StructField("deductible_amount", StringType(), True),
    StructField("ingredient_cost", StringType(), True),
    StructField("dispensing_fee", StringType(), True),
    StructField("sales_tax", StringType(), True),
    StructField("original_claim_id", StringType(), True),
    StructField("adjustment_sequence_number", StringType(), True),
    StructField("plan_id", StringType(), True),
    StructField("line_of_business", StringType(), True),
    StructField("benefit_code", StringType(), True),
    StructField("prior_auth_required", StringType(), True),
    StructField("prior_auth_number", StringType(), True),
    StructField("source_system", StringType(), True),
])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Auto Loader Ingestion Functions
# MAGIC
# MAGIC Uses Databricks Auto Loader (cloudFiles) for efficient, incremental
# MAGIC file ingestion with automatic schema evolution support.

# COMMAND ----------

def create_autoloader_stream(source_path, schema, checkpoint_path, file_format="csv"):
    """
    Creates an Auto Loader streaming DataFrame for incremental file ingestion.

    Args:
        source_path: Path to source files (S3/ADLS/GCS)
        schema: StructType schema for the source files
        checkpoint_path: Path for Auto Loader checkpoint/state
        file_format: Source file format (csv, parquet, json)

    Returns:
        Streaming DataFrame with ingestion metadata columns
    """
    reader = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", file_format)
        .option("cloudFiles.schemaLocation", checkpoint_path)
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("cloudFiles.inferColumnTypes", "false")
        .schema(schema)
    )

    # Format-specific options
    if file_format == "csv":
        reader = (
            reader
            .option("header", "true")
            .option("delimiter", ",")
            .option("quote", '"')
            .option("escape", '"')
            .option("multiLine", "true")
            .option("dateFormat", "yyyy-MM-dd")
            .option("mode", "PERMISSIVE")
            .option("columnNameOfCorruptRecord", "_rescued_data")
        )

    df = reader.load(source_path)

    # Add ingestion metadata columns
    df = (
        df
        .withColumn("source_file_name", input_file_name())
        .withColumn("ingestion_timestamp", current_timestamp())
    )

    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ## Member Eligibility Ingestion

# COMMAND ----------

def ingest_member_eligibility():
    """
    Ingests member eligibility files using Auto Loader and writes to the raw
    Delta table using MERGE for deduplication based on member_id and
    coverage_start_date.
    """
    source_path = f"{SOURCE_BASE_PATH}member_eligibility/"
    checkpoint_path = f"{CHECKPOINT_BASE}/member_eligibility"
    target_table = f"{RAW_SCHEMA}.raw_member_eligibility"

    df_stream = create_autoloader_stream(
        source_path=source_path,
        schema=member_eligibility_schema,
        checkpoint_path=checkpoint_path,
        file_format=FILE_FORMAT,
    )

    # Apply type conversions and cleansing
    df_transformed = (
        df_stream
        .withColumn("date_of_birth", to_date(col("date_of_birth"), "yyyy-MM-dd"))
        .withColumn("coverage_start_date", to_date(col("coverage_start_date"), "yyyy-MM-dd"))
        .withColumn("coverage_end_date", to_date(col("coverage_end_date"), "yyyy-MM-dd"))
        .withColumn("risk_score", col("risk_score").cast(DoubleType()))
        .withColumn("gender", upper(trim(col("gender"))))
        .withColumn("state_code", upper(trim(col("state_code"))))
        .withColumn("enrollment_status", upper(trim(col("enrollment_status"))))
        .withColumn("record_hash", sha2(
            concat_ws("|",
                col("member_id"), col("plan_id"), col("coverage_start_date"),
                col("coverage_end_date"), col("enrollment_status")
            ), 256
        ))
    )

    # Write as streaming MERGE (foreachBatch pattern)
    def merge_member_eligibility(batch_df, batch_id):
        batch_df.createOrReplaceTempView("member_eligibility_updates")

        spark.sql(f"""
            MERGE INTO {target_table} AS target
            USING member_eligibility_updates AS source
            ON target.member_id = source.member_id
               AND target.coverage_start_date = source.coverage_start_date
               AND target.plan_id = source.plan_id
            WHEN MATCHED AND target.record_hash != source.record_hash THEN
                UPDATE SET *
            WHEN NOT MATCHED THEN
                INSERT *
        """)

    query = (
        df_transformed.writeStream
        .format("delta")
        .foreachBatch(merge_member_eligibility)
        .outputMode("update")
        .option("checkpointLocation", f"{checkpoint_path}/write")
        .trigger(availableNow=True)
        .start()
    )

    query.awaitTermination()
    print(f"Member eligibility ingestion complete for batch.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Medical Claims Ingestion
# MAGIC
# MAGIC Key transformation: The source `diagnosis_codes` column is a pipe-delimited
# MAGIC string that gets converted to `ARRAY<STRING>` for native Delta Lake storage.
# MAGIC This is a structural difference from Teradata's 25 individual columns.

# COMMAND ----------

def ingest_medical_claims():
    """
    Ingests medical claims files and writes to raw Delta table.
    Converts pipe-delimited diagnosis codes string to ARRAY<STRING>.
    Uses MERGE for dedup on claim_id + claim_line_number + adjustment_sequence_number.
    """
    source_path = f"{SOURCE_BASE_PATH}medical_claims/"
    checkpoint_path = f"{CHECKPOINT_BASE}/medical_claims"
    target_table = f"{RAW_SCHEMA}.raw_medical_claim"

    df_stream = create_autoloader_stream(
        source_path=source_path,
        schema=medical_claim_schema,
        checkpoint_path=checkpoint_path,
        file_format=FILE_FORMAT,
    )

    # Apply type conversions
    df_transformed = (
        df_stream
        # Convert pipe-delimited diagnosis codes to ARRAY<STRING>
        # This is the KEY structural difference from Teradata (25 individual columns)
        .withColumn("diagnosis_codes",
            when(col("diagnosis_codes").isNotNull(),
                 split(trim(col("diagnosis_codes")), "\\|"))
            .otherwise(array().cast(ArrayType(StringType())))
        )
        # Date conversions
        .withColumn("claim_submission_date", to_date(col("claim_submission_date"), "yyyy-MM-dd"))
        .withColumn("claim_adjudication_date", to_date(col("claim_adjudication_date"), "yyyy-MM-dd"))
        .withColumn("claim_start_date", to_date(col("claim_start_date"), "yyyy-MM-dd"))
        .withColumn("claim_end_date", to_date(col("claim_end_date"), "yyyy-MM-dd"))
        .withColumn("admission_date", to_date(col("admission_date"), "yyyy-MM-dd"))
        .withColumn("discharge_date", to_date(col("discharge_date"), "yyyy-MM-dd"))
        # Numeric conversions
        .withColumn("claim_line_number", col("claim_line_number").cast(IntegerType()))
        .withColumn("billed_amount", col("billed_amount").cast(DoubleType()))
        .withColumn("allowed_amount", col("allowed_amount").cast(DoubleType()))
        .withColumn("paid_amount", col("paid_amount").cast(DoubleType()))
        .withColumn("member_liability_amount", col("member_liability_amount").cast(DoubleType()))
        .withColumn("copay_amount", col("copay_amount").cast(DoubleType()))
        .withColumn("coinsurance_amount", col("coinsurance_amount").cast(DoubleType()))
        .withColumn("deductible_amount", col("deductible_amount").cast(DoubleType()))
        .withColumn("cob_amount", col("cob_amount").cast(DoubleType()))
        .withColumn("units_of_service", col("units_of_service").cast(DoubleType()))
        .withColumn("days_of_service", col("days_of_service").cast(IntegerType()))
        .withColumn("adjustment_sequence_number", col("adjustment_sequence_number").cast(IntegerType()))
        # Standardize string fields
        .withColumn("claim_status", upper(trim(col("claim_status"))))
        .withColumn("claim_type", upper(trim(col("claim_type"))))
        .withColumn("network_status", upper(trim(col("network_status"))))
        # Record hash for change detection
        .withColumn("record_hash", sha2(
            concat_ws("|",
                col("claim_id"), col("claim_line_number"),
                col("claim_status"), col("paid_amount")
            ), 256
        ))
    )

    def merge_medical_claims(batch_df, batch_id):
        batch_df.createOrReplaceTempView("medical_claim_updates")

        spark.sql(f"""
            MERGE INTO {target_table} AS target
            USING medical_claim_updates AS source
            ON target.claim_id = source.claim_id
               AND target.claim_line_number = source.claim_line_number
               AND COALESCE(target.adjustment_sequence_number, 0)
                   = COALESCE(source.adjustment_sequence_number, 0)
            WHEN MATCHED AND target.record_hash != source.record_hash THEN
                UPDATE SET *
            WHEN NOT MATCHED THEN
                INSERT *
        """)

    query = (
        df_transformed.writeStream
        .format("delta")
        .foreachBatch(merge_medical_claims)
        .outputMode("update")
        .option("checkpointLocation", f"{checkpoint_path}/write")
        .trigger(availableNow=True)
        .start()
    )

    query.awaitTermination()
    print("Medical claims ingestion complete for batch.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pharmacy Claims Ingestion

# COMMAND ----------

def ingest_pharmacy_claims():
    """
    Ingests pharmacy claims files and writes to raw Delta table.
    Uses MERGE for dedup on claim_id + claim_line_number + adjustment_sequence_number.
    """
    source_path = f"{SOURCE_BASE_PATH}pharmacy_claims/"
    checkpoint_path = f"{CHECKPOINT_BASE}/pharmacy_claims"
    target_table = f"{RAW_SCHEMA}.raw_pharmacy_claim"

    df_stream = create_autoloader_stream(
        source_path=source_path,
        schema=pharmacy_claim_schema,
        checkpoint_path=checkpoint_path,
        file_format=FILE_FORMAT,
    )

    df_transformed = (
        df_stream
        # Date conversions
        .withColumn("fill_date", to_date(col("fill_date"), "yyyy-MM-dd"))
        .withColumn("written_date", to_date(col("written_date"), "yyyy-MM-dd"))
        # Numeric conversions
        .withColumn("claim_line_number", col("claim_line_number").cast(IntegerType()))
        .withColumn("quantity_dispensed", col("quantity_dispensed").cast(DoubleType()))
        .withColumn("days_supply", col("days_supply").cast(IntegerType()))
        .withColumn("refill_number", col("refill_number").cast(IntegerType()))
        .withColumn("billed_amount", col("billed_amount").cast(DoubleType()))
        .withColumn("allowed_amount", col("allowed_amount").cast(DoubleType()))
        .withColumn("paid_amount", col("paid_amount").cast(DoubleType()))
        .withColumn("member_liability_amount", col("member_liability_amount").cast(DoubleType()))
        .withColumn("copay_amount", col("copay_amount").cast(DoubleType()))
        .withColumn("coinsurance_amount", col("coinsurance_amount").cast(DoubleType()))
        .withColumn("deductible_amount", col("deductible_amount").cast(DoubleType()))
        .withColumn("ingredient_cost", col("ingredient_cost").cast(DoubleType()))
        .withColumn("dispensing_fee", col("dispensing_fee").cast(DoubleType()))
        .withColumn("sales_tax", col("sales_tax").cast(DoubleType()))
        .withColumn("adjustment_sequence_number", col("adjustment_sequence_number").cast(IntegerType()))
        # Standardize
        .withColumn("claim_status", upper(trim(col("claim_status"))))
        .withColumn("generic_indicator", upper(trim(col("generic_indicator"))))
        .withColumn("pharmacy_type", upper(trim(col("pharmacy_type"))))
        # Record hash
        .withColumn("record_hash", sha2(
            concat_ws("|",
                col("claim_id"), col("claim_line_number"),
                col("claim_status"), col("paid_amount")
            ), 256
        ))
    )

    def merge_pharmacy_claims(batch_df, batch_id):
        batch_df.createOrReplaceTempView("pharmacy_claim_updates")

        spark.sql(f"""
            MERGE INTO {target_table} AS target
            USING pharmacy_claim_updates AS source
            ON target.claim_id = source.claim_id
               AND target.claim_line_number = source.claim_line_number
               AND COALESCE(target.adjustment_sequence_number, 0)
                   = COALESCE(source.adjustment_sequence_number, 0)
            WHEN MATCHED AND target.record_hash != source.record_hash THEN
                UPDATE SET *
            WHEN NOT MATCHED THEN
                INSERT *
        """)

    query = (
        df_transformed.writeStream
        .format("delta")
        .foreachBatch(merge_pharmacy_claims)
        .outputMode("update")
        .option("checkpointLocation", f"{checkpoint_path}/write")
        .trigger(availableNow=True)
        .start()
    )

    query.awaitTermination()
    print("Pharmacy claims ingestion complete for batch.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execute Ingestion Pipeline

# COMMAND ----------

# Run all three ingestion streams
print("=" * 60)
print("Starting Claims Ingestion Pipeline")
print("=" * 60)

print("\n[1/3] Ingesting member eligibility...")
ingest_member_eligibility()

print("\n[2/3] Ingesting medical claims...")
ingest_medical_claims()

print("\n[3/3] Ingesting pharmacy claims...")
ingest_pharmacy_claims()

print("\n" + "=" * 60)
print("Claims Ingestion Pipeline Complete")
print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Post-Ingestion Validation

# COMMAND ----------

# Validate record counts
for table_name in ["raw_member_eligibility", "raw_medical_claim", "raw_pharmacy_claim"]:
    count = spark.sql(f"SELECT COUNT(*) AS cnt FROM {RAW_SCHEMA}.{table_name}").collect()[0]["cnt"]
    print(f"  {RAW_SCHEMA}.{table_name}: {count:,} rows")

# Check for recent ingestion activity
print("\nLatest ingestion timestamps:")
for table_name in ["raw_member_eligibility", "raw_medical_claim", "raw_pharmacy_claim"]:
    latest = spark.sql(f"""
        SELECT MAX(ingestion_timestamp) AS latest_ts
        FROM {RAW_SCHEMA}.{table_name}
    """).collect()[0]["latest_ts"]
    print(f"  {table_name}: {latest}")
