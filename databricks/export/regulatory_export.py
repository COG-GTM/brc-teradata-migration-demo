# =============================================================================
# regulatory_export.py
#
# Replaces:
#   teradata/bteq/monthly_regulatory_report.bteq  (STEP 3 validation/export SELECTs)
#   teradata/scheduled_jobs/monthly_regulatory_sequence.txt
#       -> BRCL_MONTH_005_EXPORT_REGULATORY (PRA/FCA submission files)
#       -> BRCL_MONTH_006_EXPORT_PNL        (finance P&L files)
#
# Mapping rationale:
#   The BTEQ script produced its regulatory and P&L extracts as terminal
#   SELECTs that the scheduler captured to files. Here each extract is an
#   explicit, ordered write to the export volume so the artefact is
#   reproducible and auditable. The calculations themselves are no longer
#   procedural: sp_regulatory_capital_calc and sp_monthly_pnl_rollup became the
#   dbt models fct_regulatory_capital and fct_monthly_pnl, so this task only
#   reads their output (BTEQ STEP 1 and STEP 2 are the dbt task in
#   databricks/jobs/monthly_regulatory_job.yml).
#
#   Reporting-date derivation translates
#   `CURRENT_DATE - EXTRACT(DAY FROM CURRENT_DATE) + 1` (first day of the
#   current month) to TRUNC(CURRENT_DATE(), 'MM'), overridable via
#   --reporting_date for reruns and backfills.
# =============================================================================

import argparse

from pyspark.dbutils import DBUtils
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Month-end regulatory and P&L extracts")
    parser.add_argument("--catalog", default="barclays")
    parser.add_argument("--schema_risk", default="risk")
    parser.add_argument("--schema_finance", default="finance")
    parser.add_argument("--export_dir", default="/Volumes/barclays/raw/exports/regulatory")
    parser.add_argument("--reporting_date", default="", help="yyyy-MM-dd; empty -> 1st of month")
    parser.add_argument("--delimiter", default="|")
    return parser.parse_args()


def write_extract(
    dbutils: DBUtils, df: DataFrame, name: str, export_dir: str, file_stamp: str, delimiter: str
) -> int:
    """Single-file, delimited extract with a header row for submission."""
    staging_dir = f"{export_dir}/_staging_{name}_{file_stamp}"
    final_file = f"{export_dir}/{name}_{file_stamp}.dat"
    count = df.count()
    (
        df.coalesce(1)
        .write.format("csv")
        .option("header", "true")
        .option("sep", delimiter)
        .option("nullValue", "")
        .mode("overwrite")
        .save(staging_dir)
    )
    part_file = next(f.path for f in dbutils.fs.ls(staging_dir) if f.name.startswith("part-"))
    dbutils.fs.mv(part_file, final_file)
    dbutils.fs.rm(staging_dir, recurse=True)
    print(f"  {name}: {count} rows -> {final_file}")
    return count


def main() -> None:
    args = parse_args()
    spark = SparkSession.builder.getOrCreate()
    dbutils = DBUtils(spark)

    reporting_date = (
        spark.sql(
            "SELECT COALESCE("
            f"TO_DATE('{args.reporting_date}', 'yyyy-MM-dd'), TRUNC(CURRENT_DATE(), 'MM')"
            ") AS d"
        )
        .first()["d"]
        .isoformat()
    )
    file_stamp = reporting_date.replace("-", "")
    print(f"Monthly regulatory export for reporting_date={reporting_date}")

    # --- BRCL_MONTH_005_EXPORT_REGULATORY ------------------------------------
    # BTEQ: asset_class, total_exposure, total_rwa, capital_required,
    #       capital_ratio FROM MART_REGULATORY_CAPITAL
    capital = (
        spark.table(f"{args.catalog}.{args.schema_risk}.fct_regulatory_capital")
        .where(F.col("reporting_date") == F.lit(reporting_date).cast("date"))
        .select(
            "reporting_date",
            "asset_class",
            "total_exposure",
            "total_rwa",
            "capital_required",
            "capital_ratio",
        )
        .orderBy("asset_class")
    )
    capital_rows = write_extract(
        dbutils, capital, "regulatory_capital", args.export_dir, file_stamp, args.delimiter
    )

    # --- BRCL_MONTH_006_EXPORT_PNL -------------------------------------------
    # BTEQ: business_line, gross_revenue, net_profit, cost_income_ratio
    #       FROM MART_MONTHLY_PNL
    pnl = (
        spark.table(f"{args.catalog}.{args.schema_finance}.fct_monthly_pnl")
        .where(F.col("reporting_month") == F.lit(reporting_date).cast("date"))
        .select(
            "reporting_month",
            "business_line",
            "gross_revenue",
            "net_profit",
            "cost_income_ratio",
        )
        .orderBy("business_line")
    )
    pnl_rows = write_extract(
        dbutils, pnl, "monthly_pnl", args.export_dir, file_stamp, args.delimiter
    )

    # BTEQ exited 12 when a submission extract came back empty; failing the
    # task here skips the downstream validation and notification tasks.
    if capital_rows == 0 or pnl_rows == 0:
        raise RuntimeError(
            f"Regulatory export empty for {reporting_date} "
            f"(capital={capital_rows}, pnl={pnl_rows}) - submission files not produced."
        )

    print(f"Exported {capital_rows} capital rows and {pnl_rows} P&L rows for {reporting_date}")


if __name__ == "__main__":
    main()
