# =============================================================================
# market_data_autoloader.py
#
# Replaces: teradata/fastload/market_data_load.fl
#
# Mapping rationale:
#   FastLoad bulk-loads a delimited file into an *empty* table using N sessions,
#   with CHECKPOINT restartability and ET/UV error tables. The Databricks
#   equivalent is Auto Loader (cloudFiles): the streaming checkpoint replaces
#   `CHECKPOINT 5000`, file discovery replaces the single `FILE=` handle so new
#   daily files are picked up without re-running the script, exactly-once file
#   processing replaces FastLoad's restart logic, and `_rescued_data` replaces
#   the MKTDATA_FL_ET / MKTDATA_FL_UV error tables. FastLoad's requirement that
#   the target be empty disappears — Delta append is transactional.
#
#   Field-level mapping (FastLoad DEFINE ... -> explicit schema below):
#     in_valuation_date VARCHAR(10) + CAST(... AS DATE FORMAT 'YYYY-MM-DD')
#       -> DATE column with dateFormat 'yyyy-MM-dd'
#     in_mid/bid/ask_price VARCHAR(20) + CAST(... AS DECIMAL(18,8))
#       -> DECIMAL(18,8) columns
#     SET RECORD VARTEXT '|' -> option("sep", ...); sample_data/market_data.csv
#       is comma-delimited with a header, so the delimiter is parameterised.
#     in_source_system is supplied by the extract job, not the file, so it is
#       added as a literal.
#
# Run as the `load_market_data` task of databricks/jobs/daily_etl_job.yml.
# =============================================================================

import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType,
    DecimalType,
    StringType,
    StructField,
    StructType,
)

# Explicit schema: one entry per FastLoad DEFINE field.
MARKET_DATA_SCHEMA = StructType(
    [
        StructField("instrument_id", StringType(), True),
        StructField("valuation_date", DateType(), True),
        StructField("instrument_type", StringType(), True),
        StructField("instrument_name", StringType(), True),
        StructField("currency", StringType(), True),
        StructField("mid_price", DecimalType(18, 8), True),
        StructField("bid_price", DecimalType(18, 8), True),
        StructField("ask_price", DecimalType(18, 8), True),
    ]
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Auto Loader ingest of the market data feed")
    parser.add_argument("--catalog", default="barclays")
    parser.add_argument("--schema_raw", default="raw")
    parser.add_argument("--landing_path", default="/Volumes/barclays/raw/landing/market_data")
    parser.add_argument("--checkpoint_path", default="/Volumes/barclays/raw/_checkpoints/market_data")
    # '|' for the legacy VARTEXT extracts, ',' for the sample_data CSVs.
    parser.add_argument("--delimiter", default=",")
    parser.add_argument("--source_system", default="MARKET_DATA_FEED")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    spark = SparkSession.builder.getOrCreate()
    target_table = f"{args.catalog}.{args.schema_raw}.market_data"

    stream = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", f"{args.checkpoint_path}/_schema")
        # Malformed rows land in _rescued_data instead of a FastLoad ET table.
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .option("rescuedDataColumn", "_rescued_data")
        .option("header", "true")
        .option("sep", args.delimiter)
        .option("dateFormat", "yyyy-MM-dd")
        .schema(MARKET_DATA_SCHEMA)
        .load(args.landing_path)
        .withColumn("source_system", F.lit(args.source_system))
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_ingested_at", F.current_timestamp())
        .select(
            "instrument_id",
            "valuation_date",
            "instrument_type",
            "instrument_name",
            "currency",
            "mid_price",
            "bid_price",
            "ask_price",
            "source_system",
            "_source_file",
            "_ingested_at",
            "_rescued_data",
        )
    )

    # availableNow gives batch semantics inside a scheduled job (the FastLoad
    # job ran once per day and exited) while keeping exactly-once guarantees.
    query = (
        stream.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", args.checkpoint_path)
        .option("mergeSchema", "false")
        .trigger(availableNow=True)
        .toTable(target_table)
    )
    query.awaitTermination()

    rows_loaded = query.lastProgress["numInputRows"] if query.lastProgress else 0
    print(f"market_data Auto Loader complete: {rows_loaded} rows appended to {target_table}")
    if rows_loaded == 0:
        # The FastLoad job logged an empty-input warning without failing the
        # scheduler; downstream tasks still run against existing history.
        print(f"WARNING: no new market data files found in {args.landing_path}")


if __name__ == "__main__":
    main()
