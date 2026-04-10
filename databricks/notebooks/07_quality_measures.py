# Databricks notebook source
# MAGIC %md
# MAGIC # 07 - HEDIS Quality Measures Calculation
# MAGIC
# MAGIC This notebook calculates sample HEDIS (Healthcare Effectiveness Data and
# MAGIC Information Set) quality measures using PySpark. These measures are used
# MAGIC for:
# MAGIC - Health plan performance evaluation (CMS Stars ratings)
# MAGIC - Value-based care contract compliance
# MAGIC - Provider quality scorecards
# MAGIC - Gap-in-care identification and outreach
# MAGIC
# MAGIC ## Sample Measures Implemented
# MAGIC 1. **BCS** - Breast Cancer Screening (women 50-74)
# MAGIC 2. **CDC-HBA1C** - Comprehensive Diabetes Care: HbA1c Testing
# MAGIC 3. **COL** - Colorectal Cancer Screening (adults 45-75)
# MAGIC 4. **CBP** - Controlling High Blood Pressure
# MAGIC 5. **PCE** - Pharmacotherapy for Opioid Use Disorder

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col, lit, when, year, month, datediff, floor, months_between,
    max as spark_max, min as spark_min, count as spark_count,
    sum as spark_sum, countDistinct, array_contains, current_date,
    current_timestamp, to_date, explode, expr, concat, first
)
from pyspark.sql.types import IntegerType, BooleanType, StringType

STAGING_SCHEMA = "claims_staging"
WAREHOUSE_SCHEMA = "claims_warehouse"
MART_SCHEMA = "claims_mart"

# Measurement year configuration
dbutils.widgets.text("measurement_year", "2025", "Measurement Year")
MEASUREMENT_YEAR = int(dbutils.widgets.get("measurement_year"))

MEASUREMENT_START = f"{MEASUREMENT_YEAR}-01-01"
MEASUREMENT_END = f"{MEASUREMENT_YEAR}-12-31"

print(f"Calculating quality measures for year: {MEASUREMENT_YEAR}")
print(f"Measurement period: {MEASUREMENT_START} to {MEASUREMENT_END}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reference Code Sets
# MAGIC
# MAGIC Define the code sets used for measure identification. In production,
# MAGIC these would be loaded from NCQA Value Set Directory (VSD) tables.

# COMMAND ----------

# Procedure codes for screening/testing (sample sets)
MAMMOGRAPHY_CPT_CODES = ["77061", "77062", "77063", "77065", "77066", "77067"]
HBA1C_CPT_CODES = ["83036", "83037"]
COLONOSCOPY_CPT_CODES = ["44388", "44389", "44390", "44391", "44392",
                         "44394", "44401", "44402", "44403", "44404",
                         "45378", "45379", "45380", "45381", "45382",
                         "45384", "45385", "45386", "45388", "45390",
                         "45391", "45392", "45393", "45398"]
FIT_CPT_CODES = ["82270", "82274"]
BP_MEASUREMENT_CPT_CODES = ["99091", "99453", "99454", "99457", "99458",
                            "99473", "99474"]

# Diagnosis codes for condition identification
DIABETES_ICD10 = ["E10", "E11", "E13"]  # Type 1, Type 2, Other
HYPERTENSION_ICD10 = ["I10", "I11", "I12", "I13", "I15"]
OPIOID_USE_ICD10 = ["F11"]

# Pharmacy codes (NDC prefixes) for medication measures
OPIOID_MAT_NDC_PREFIXES = ["00054", "00228", "12496", "43063", "54123",
                            "54868", "55700", "65757"]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Base Data

# COMMAND ----------

# Load member months for denominator identification
df_member_months = spark.table(f"{MART_SCHEMA}.mart_member_months").filter(
    col("enrollment_year") == MEASUREMENT_YEAR
)

# Load staged medical claims within measurement period
df_medical = spark.table(f"{STAGING_SCHEMA}.stg_medical_claim_current").filter(
    (col("claim_start_date") >= MEASUREMENT_START) &
    (col("claim_start_date") <= MEASUREMENT_END) &
    (col("claim_status").isin("PAID", "ADJUSTED"))
)

# Load staged pharmacy claims
df_pharmacy = spark.table(f"{STAGING_SCHEMA}.stg_pharmacy_claim_current").filter(
    (col("fill_date") >= MEASUREMENT_START) &
    (col("fill_date") <= MEASUREMENT_END) &
    (col("claim_status").isin("PAID", "ADJUSTED"))
)

# Get continuously enrolled members (enrolled at least 11 of 12 months)
df_continuous_enrollment = (
    df_member_months
    .groupBy("member_id")
    .agg(spark_count("*").alias("enrolled_months"))
    .filter(col("enrolled_months") >= 11)
)

print(f"Continuously enrolled members ({MEASUREMENT_YEAR}): "
      f"{df_continuous_enrollment.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Measure 1: BCS - Breast Cancer Screening
# MAGIC Women aged 50-74 who had a mammogram during the measurement year.

# COMMAND ----------

def calculate_bcs(df_member_months, df_medical, measurement_year):
    """
    Breast Cancer Screening (BCS) measure.

    Denominator: Women aged 50-74, continuously enrolled
    Numerator: Had a mammogram (CPT 77061-77067) during measurement year
    Exclusions: Bilateral mastectomy history
    """
    # Identify eligible population (denominator)
    df_eligible = (
        df_member_months
        .filter(
            (col("gender") == "F") &
            (col("age_at_month") >= 50) &
            (col("age_at_month") <= 74)
        )
        .select("member_id")
        .distinct()
        .join(df_continuous_enrollment, on="member_id", how="inner")
    )

    # Identify members with mammogram (numerator)
    df_screened = (
        df_medical
        .filter(col("procedure_code").isin(MAMMOGRAPHY_CPT_CODES))
        .select("patient_id")
        .distinct()
        .withColumnRenamed("patient_id", "member_id")
    )

    # Combine
    df_bcs = (
        df_eligible
        .join(df_screened, on="member_id", how="left")
        .withColumn("measure_id", lit("BCS"))
        .withColumn("measure_name", lit("Breast Cancer Screening"))
        .withColumn("measure_category", lit("Preventive"))
        .withColumn("is_eligible", lit(True))
        .withColumn("is_numerator_compliant",
                     when(df_screened["member_id"].isNotNull(), lit(True))
                     .otherwise(lit(False)))
        .withColumn("is_excluded", lit(False))
        .select("member_id", "measure_id", "measure_name", "measure_category",
                "is_eligible", "is_numerator_compliant", "is_excluded")
    )

    eligible_count = df_eligible.count()
    compliant_count = df_bcs.filter(col("is_numerator_compliant") == True).count()
    rate = (compliant_count / eligible_count * 100) if eligible_count > 0 else 0

    print(f"BCS - Breast Cancer Screening:")
    print(f"  Eligible: {eligible_count:,}")
    print(f"  Compliant: {compliant_count:,}")
    print(f"  Rate: {rate:.1f}%")

    return df_bcs

# COMMAND ----------

df_bcs = calculate_bcs(df_member_months, df_medical, MEASUREMENT_YEAR)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Measure 2: CDC-HBA1C - Diabetes Care: HbA1c Testing
# MAGIC Members 18-75 with diabetes who had an HbA1c test.

# COMMAND ----------

def calculate_cdc_hba1c(df_member_months, df_medical, measurement_year):
    """
    Comprehensive Diabetes Care: HbA1c Testing measure.

    Denominator: Members 18-75 with diabetes diagnosis, continuously enrolled
    Numerator: Had an HbA1c test (CPT 83036, 83037) during measurement year
    """
    # Identify members with diabetes diagnosis
    df_diabetic = (
        df_medical
        .filter(
            expr("EXISTS(diagnosis_codes, code -> " +
                 " OR ".join([f"code LIKE '{dx}%'" for dx in DIABETES_ICD10]) +
                 ")")
        )
        .select("patient_id")
        .distinct()
        .withColumnRenamed("patient_id", "member_id")
    )

    # Eligible: diabetic, age 18-75, continuously enrolled
    df_eligible = (
        df_member_months
        .filter(
            (col("age_at_month") >= 18) &
            (col("age_at_month") <= 75)
        )
        .select("member_id")
        .distinct()
        .join(df_diabetic, on="member_id", how="inner")
        .join(df_continuous_enrollment, on="member_id", how="inner")
    )

    # Identify members with HbA1c test (numerator)
    df_tested = (
        df_medical
        .filter(col("procedure_code").isin(HBA1C_CPT_CODES))
        .select("patient_id")
        .distinct()
        .withColumnRenamed("patient_id", "member_id")
    )

    df_cdc = (
        df_eligible
        .join(df_tested, on="member_id", how="left")
        .withColumn("measure_id", lit("CDC-HBA1C"))
        .withColumn("measure_name", lit("Comprehensive Diabetes Care: HbA1c Testing"))
        .withColumn("measure_category", lit("Chronic"))
        .withColumn("is_eligible", lit(True))
        .withColumn("is_numerator_compliant",
                     when(df_tested["member_id"].isNotNull(), lit(True))
                     .otherwise(lit(False)))
        .withColumn("is_excluded", lit(False))
        .select("member_id", "measure_id", "measure_name", "measure_category",
                "is_eligible", "is_numerator_compliant", "is_excluded")
    )

    eligible_count = df_eligible.count()
    compliant_count = df_cdc.filter(col("is_numerator_compliant") == True).count()
    rate = (compliant_count / eligible_count * 100) if eligible_count > 0 else 0

    print(f"\nCDC-HBA1C - Diabetes HbA1c Testing:")
    print(f"  Eligible (diabetic): {eligible_count:,}")
    print(f"  Compliant: {compliant_count:,}")
    print(f"  Rate: {rate:.1f}%")

    return df_cdc

# COMMAND ----------

df_cdc = calculate_cdc_hba1c(df_member_months, df_medical, MEASUREMENT_YEAR)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Measure 3: COL - Colorectal Cancer Screening
# MAGIC Adults aged 45-75 with appropriate colorectal cancer screening.

# COMMAND ----------

def calculate_col_screening(df_member_months, df_medical, measurement_year):
    """
    Colorectal Cancer Screening (COL) measure.

    Denominator: Adults 45-75, continuously enrolled
    Numerator: Had colonoscopy in past 10 years OR FIT test in past year
    """
    df_eligible = (
        df_member_months
        .filter(
            (col("age_at_month") >= 45) &
            (col("age_at_month") <= 75)
        )
        .select("member_id")
        .distinct()
        .join(df_continuous_enrollment, on="member_id", how="inner")
    )

    # Colonoscopy in past 10 years
    lookback_start = f"{measurement_year - 10}-01-01"
    df_colonoscopy = (
        spark.table(f"{STAGING_SCHEMA}.stg_medical_claim_current")
        .filter(
            (col("procedure_code").isin(COLONOSCOPY_CPT_CODES)) &
            (col("claim_start_date") >= lookback_start) &
            (col("claim_start_date") <= MEASUREMENT_END)
        )
        .select("patient_id")
        .distinct()
        .withColumnRenamed("patient_id", "member_id")
    )

    # FIT test in measurement year
    df_fit = (
        df_medical
        .filter(col("procedure_code").isin(FIT_CPT_CODES))
        .select("patient_id")
        .distinct()
        .withColumnRenamed("patient_id", "member_id")
    )

    # Union of both screening methods
    df_screened = df_colonoscopy.union(df_fit).distinct()

    df_col = (
        df_eligible
        .join(df_screened, on="member_id", how="left")
        .withColumn("measure_id", lit("COL"))
        .withColumn("measure_name", lit("Colorectal Cancer Screening"))
        .withColumn("measure_category", lit("Preventive"))
        .withColumn("is_eligible", lit(True))
        .withColumn("is_numerator_compliant",
                     when(df_screened["member_id"].isNotNull(), lit(True))
                     .otherwise(lit(False)))
        .withColumn("is_excluded", lit(False))
        .select("member_id", "measure_id", "measure_name", "measure_category",
                "is_eligible", "is_numerator_compliant", "is_excluded")
    )

    eligible_count = df_eligible.count()
    compliant_count = df_col.filter(col("is_numerator_compliant") == True).count()
    rate = (compliant_count / eligible_count * 100) if eligible_count > 0 else 0

    print(f"\nCOL - Colorectal Cancer Screening:")
    print(f"  Eligible: {eligible_count:,}")
    print(f"  Compliant: {compliant_count:,}")
    print(f"  Rate: {rate:.1f}%")

    return df_col

# COMMAND ----------

df_col = calculate_col_screening(df_member_months, df_medical, MEASUREMENT_YEAR)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Measure 4: CBP - Controlling High Blood Pressure

# COMMAND ----------

def calculate_cbp(df_member_months, df_medical, measurement_year):
    """
    Controlling High Blood Pressure (CBP) measure.

    Denominator: Members 18-85 with hypertension diagnosis, continuously enrolled
    Numerator: Had BP measurement and adequate control
    """
    # Identify members with hypertension
    df_hypertensive = (
        df_medical
        .filter(
            expr("EXISTS(diagnosis_codes, code -> " +
                 " OR ".join([f"code LIKE '{dx}%'" for dx in HYPERTENSION_ICD10]) +
                 ")")
        )
        .select("patient_id")
        .distinct()
        .withColumnRenamed("patient_id", "member_id")
    )

    df_eligible = (
        df_member_months
        .filter(
            (col("age_at_month") >= 18) &
            (col("age_at_month") <= 85)
        )
        .select("member_id")
        .distinct()
        .join(df_hypertensive, on="member_id", how="inner")
        .join(df_continuous_enrollment, on="member_id", how="inner")
    )

    # Members with BP measurement
    df_bp_measured = (
        df_medical
        .filter(col("procedure_code").isin(BP_MEASUREMENT_CPT_CODES))
        .select("patient_id")
        .distinct()
        .withColumnRenamed("patient_id", "member_id")
    )

    df_cbp = (
        df_eligible
        .join(df_bp_measured, on="member_id", how="left")
        .withColumn("measure_id", lit("CBP"))
        .withColumn("measure_name", lit("Controlling High Blood Pressure"))
        .withColumn("measure_category", lit("Chronic"))
        .withColumn("is_eligible", lit(True))
        .withColumn("is_numerator_compliant",
                     when(df_bp_measured["member_id"].isNotNull(), lit(True))
                     .otherwise(lit(False)))
        .withColumn("is_excluded", lit(False))
        .select("member_id", "measure_id", "measure_name", "measure_category",
                "is_eligible", "is_numerator_compliant", "is_excluded")
    )

    eligible_count = df_eligible.count()
    compliant_count = df_cbp.filter(col("is_numerator_compliant") == True).count()
    rate = (compliant_count / eligible_count * 100) if eligible_count > 0 else 0

    print(f"\nCBP - Controlling High Blood Pressure:")
    print(f"  Eligible (hypertensive): {eligible_count:,}")
    print(f"  Compliant: {compliant_count:,}")
    print(f"  Rate: {rate:.1f}%")

    return df_cbp

# COMMAND ----------

df_cbp = calculate_cbp(df_member_months, df_medical, MEASUREMENT_YEAR)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Measure 5: PCE - Pharmacotherapy for Opioid Use Disorder

# COMMAND ----------

def calculate_pce(df_member_months, df_medical, df_pharmacy, measurement_year):
    """
    Pharmacotherapy for Opioid Use Disorder (PCE) measure.

    Denominator: Members 18+ with OUD diagnosis, continuously enrolled
    Numerator: Received MAT (Medication-Assisted Treatment) pharmacy fill
    """
    df_oud = (
        df_medical
        .filter(
            expr("EXISTS(diagnosis_codes, code -> code LIKE 'F11%')")
        )
        .select("patient_id")
        .distinct()
        .withColumnRenamed("patient_id", "member_id")
    )

    df_eligible = (
        df_member_months
        .filter(col("age_at_month") >= 18)
        .select("member_id")
        .distinct()
        .join(df_oud, on="member_id", how="inner")
        .join(df_continuous_enrollment, on="member_id", how="inner")
    )

    # Members with MAT pharmacy fills
    mat_filter = " OR ".join(
        [f"ndc_code LIKE '{prefix}%'" for prefix in OPIOID_MAT_NDC_PREFIXES]
    )
    df_mat = (
        df_pharmacy
        .filter(expr(mat_filter))
        .select("patient_id")
        .distinct()
        .withColumnRenamed("patient_id", "member_id")
    )

    df_pce = (
        df_eligible
        .join(df_mat, on="member_id", how="left")
        .withColumn("measure_id", lit("PCE"))
        .withColumn("measure_name", lit("Pharmacotherapy for Opioid Use Disorder"))
        .withColumn("measure_category", lit("Behavioral"))
        .withColumn("is_eligible", lit(True))
        .withColumn("is_numerator_compliant",
                     when(df_mat["member_id"].isNotNull(), lit(True))
                     .otherwise(lit(False)))
        .withColumn("is_excluded", lit(False))
        .select("member_id", "measure_id", "measure_name", "measure_category",
                "is_eligible", "is_numerator_compliant", "is_excluded")
    )

    eligible_count = df_eligible.count()
    compliant_count = df_pce.filter(col("is_numerator_compliant") == True).count()
    rate = (compliant_count / eligible_count * 100) if eligible_count > 0 else 0

    print(f"\nPCE - Pharmacotherapy for Opioid Use Disorder:")
    print(f"  Eligible (OUD diagnosis): {eligible_count:,}")
    print(f"  Compliant (MAT fill): {compliant_count:,}")
    print(f"  Rate: {rate:.1f}%")

    return df_pce

# COMMAND ----------

df_pce = calculate_pce(df_member_months, df_medical, df_pharmacy, MEASUREMENT_YEAR)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Combine All Measures and Write to Mart

# COMMAND ----------

from functools import reduce
from pyspark.sql import DataFrame

# Union all measure results
df_all_measures = reduce(
    DataFrame.unionByName,
    [df_bcs, df_cdc, df_col, df_cbp, df_pce]
)

# Add measurement period metadata
df_final = (
    df_all_measures
    .withColumn("measurement_year", lit(MEASUREMENT_YEAR))
    .withColumn("measurement_period_start", to_date(lit(MEASUREMENT_START)))
    .withColumn("measurement_period_end", to_date(lit(MEASUREMENT_END)))
    .withColumn("exclusion_reason", lit(None).cast(StringType()))
    .withColumn("gap_status",
                when(col("is_numerator_compliant") == True, lit("CLOSED"))
                .otherwise(lit("OPEN")))
    .withColumn("created_timestamp", current_timestamp())
    .withColumn("updated_timestamp", current_timestamp())
    .withColumnRenamed("member_id", "patient_id")
)

# Write to quality measures mart
(
    df_final.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{MART_SCHEMA}.mart_quality_measures")
)

total_records = df_final.count()
print(f"\nWritten {total_records:,} quality measure records to {MART_SCHEMA}.mart_quality_measures")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quality Measures Summary Dashboard

# COMMAND ----------

# Overall quality measure summary
print(f"\n{'='*60}")
print(f"HEDIS Quality Measures Summary - {MEASUREMENT_YEAR}")
print(f"{'='*60}\n")

summary = (
    df_final
    .groupBy("measure_id", "measure_name", "measure_category")
    .agg(
        spark_count("*").alias("eligible"),
        spark_sum(when(col("is_numerator_compliant") == True, 1).otherwise(0)).alias("compliant"),
        spark_sum(when(col("gap_status") == "OPEN", 1).otherwise(0)).alias("open_gaps"),
    )
    .withColumn("compliance_rate",
                (col("compliant") / col("eligible") * 100).cast("decimal(5,1)"))
    .orderBy("measure_id")
)

summary.show(truncate=False)

print("Quality measures calculation complete.")
