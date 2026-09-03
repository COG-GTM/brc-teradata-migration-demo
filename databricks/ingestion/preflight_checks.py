# =============================================================================
# preflight_checks.py
#
# Replaces:
#   teradata/scheduled_jobs/daily_etl_sequence.txt  -> BRCL_DAILY_001_PREFLIGHT
#   teradata/scheduled_jobs/monthly_regulatory_sequence.txt -> BRCL_MONTH_001_PREFLIGHT
#   teradata/bteq/daily_batch_load.bteq             -> pre-flight block and its
#                                                      .LABEL NODATA branch
#
# Mapping rationale:
#   BTEQ signalled "no data" by branching to .LABEL NODATA and exiting 0, and
#   hard failures by .GOTO ERRORHANDLER / .QUIT 12. Databricks Workflows model
#   this with task outcome: a raised exception fails the task and, through the
#   job's dependency graph, skips downstream tasks exactly as the scheduler's
#   dependency column did. The no-data case exits successfully with a warning
#   so the run is not paged on, matching the legacy exit code 0.
#
#   File-arrival checks replace the scheduler's SOURCE_EXTRACT_COMPLETE
#   dependency, which in Teradata was an external file-watcher event.
# =============================================================================

import argparse
import sys
from datetime import date

from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession

# Feed name -> landing sub-directory / file prefix.
REQUIRED_FEEDS = {
    "market_data": "market_data",        # FastLoad feed (market_data_load.fl)
    "transactions": "transactions",      # TPT feed (transaction_load.tpt)
    "customers": "customers",            # reference extract
    "accounts": "accounts",              # reference extract
    "counterparties": "counterparties",  # reference extract
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Daily/monthly ETL pre-flight checks")
    parser.add_argument("--catalog", default="barclays")
    parser.add_argument("--schema_raw", default="raw")
    parser.add_argument("--landing_path", default="/Volumes/barclays/raw/landing")
    parser.add_argument("--business_date", default="")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    spark = SparkSession.builder.getOrCreate()
    dbutils = DBUtils(spark)
    business_date = args.business_date or date.today().isoformat()

    found = {}
    for feed, prefix in REQUIRED_FEEDS.items():
        directory = f"{args.landing_path}/{feed}"
        try:
            files = [f for f in dbutils.fs.ls(directory) if f.name.startswith(prefix)]
        except Exception:
            # A missing directory is equivalent to a missing source extract.
            files = []
        found[feed] = len(files)
        print(f"pre-flight: {feed}: {len(files)} file(s) in {directory}")

    missing = [feed for feed, count in found.items() if count == 0]

    # BTEQ treated an empty transaction feed as NODATA: warn, skip the batch,
    # exit 0. Anything else missing was a hard failure.
    if found["transactions"] == 0:
        print(f"WARNING: no transaction extract found for {business_date} - skipping daily batch")
        sys.exit(0)

    if missing:
        raise RuntimeError(
            f"Pre-flight failed for {business_date}: missing source extracts {missing}. "
            "Equivalent to BTEQ .GOTO ERRORHANDLER / .QUIT 12."
        )

    print(f"Pre-flight checks passed for business date {business_date}")


if __name__ == "__main__":
    main()
