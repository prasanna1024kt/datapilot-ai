import argparse
import json
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col


def main():

    parser = argparse.ArgumentParser(
        description="Find invalid records in Bronze data"
    )

    parser.add_argument(
        "--dataset",
        required=True
    )

    parser.add_argument(
        "--column",
        required=True
    )

    parser.add_argument(
        "--minimum",
        required=True,
        type=float
    )

    parser.add_argument(
        "--maximum",
        required=True,
        type=float
    )

    parser.add_argument(
        "--output",
        required=True
    )

    args = parser.parse_args()

    # -----------------------------------------------------
    # Spark
    # -----------------------------------------------------

    spark = (
        SparkSession.builder
        .appName(
            f"DataPilot-InvalidRecords-{args.dataset}"
        )
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "com.amazonaws.auth.profile.ProfileCredentialsProvider"
        )
        .config(
            "spark.hadoop.fs.s3a.endpoint.region",
            "us-east-1"
        )
        .getOrCreate()
    )

    # -----------------------------------------------------
    # Bronze path
    # -----------------------------------------------------

    bronze_path = (
        "s3a://datapilot-ai-data-practice/"
        f"processed/bronze/{args.dataset}"
    )

    print(f"Reading Bronze data: {bronze_path}")

    df = spark.read.parquet(bronze_path)

    # -----------------------------------------------------
    # Validate column
    # -----------------------------------------------------

    if args.column not in df.columns:

        result = {
            "dataset": args.dataset,
            "layer": "bronze",
            "column": args.column,
            "status": "FAIL",
            "error": "COLUMN_NOT_FOUND",
            "available_columns": df.columns
        }

        Path(args.output).parent.mkdir(
            parents=True,
            exist_ok=True
        )

        with open(args.output, "w") as file:
            json.dump(
                result,
                file,
                indent=2,
                default=str
            )

        spark.stop()

        raise RuntimeError(
            f"Column not found: {args.column}"
        )

    # -----------------------------------------------------
    # Find invalid records
    # -----------------------------------------------------

    invalid_df = df.filter(
        (col(args.column) < args.minimum)
        |
        (col(args.column) > args.maximum)
    )

    invalid_count = invalid_df.count()

    # -----------------------------------------------------
    # Collect records
    # -----------------------------------------------------

    records = [
        row.asDict()
        for row in invalid_df.limit(20).collect()
    ]

    # -----------------------------------------------------
    # Result
    # -----------------------------------------------------

    result = {
        "dataset": args.dataset,
        "layer": "bronze",
        "column": args.column,
        "minimum": args.minimum,
        "maximum": args.maximum,
        "status": (
            "FAIL"
            if invalid_count > 0
            else "PASS"
        ),
        "invalid_count": invalid_count,
        "records_returned": len(records),
        "records": records
    }

    # -----------------------------------------------------
    # Save result
    # -----------------------------------------------------

    output_path = Path(args.output)

    output_path.parent.mkdir(
        parents=True,
        exist_ok=True
    )

    with open(output_path, "w") as file:

        json.dump(
            result,
            file,
            indent=2,
            default=str
        )

    print(
        f"Invalid records found: {invalid_count}"
    )

    print(
        f"Result written to: {args.output}"
    )

    spark.stop()


if __name__ == "__main__":
    main()
