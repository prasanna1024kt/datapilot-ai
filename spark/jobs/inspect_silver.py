import argparse
import json
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col


BUCKET = "datapilot-ai-data-practice"
REGION = "us-east-1"


def inspect_silver(spark,dataset,column,minimum,maximum):
    silver_path = (
        f"s3a://{BUCKET}/processed/silver/{dataset}"
    )

    df = spark.read.parquet(silver_path)

    if column not in df.columns:
        return {
            "dataset": dataset,
            "layer": "silver",
            "column": column,
            "path": silver_path,
            "status": "FAIL",
            "error": f"Column not found: {column}",
        }

    invalid_df = df.filter(
        (col(column) < minimum)
        | (col(column) > maximum)
    )

    invalid_count = invalid_df.count()

    sample_values = [
        row[column]
        for row in (
            invalid_df
            .select(column)
            .limit(10)
            .collect()
        )
    ]

    return {
        "dataset": dataset,
        "layer": "silver",
        "column": column,
        "path": silver_path,
        "status": (
            "PASS"
            if invalid_count == 0
            else "FAIL"
        ),
        "minimum": minimum,
        "maximum": maximum,
        "invalid_count": invalid_count,
        "sample_invalid_values": sample_values,
    }


def main():

    parser = argparse.ArgumentParser(
        description="DataPilot AI Bronze inspection job"
    )

    parser.add_argument(
        "--dataset",
        required=True,
    )

    parser.add_argument(
        "--column",
        required=True,
    )

    parser.add_argument(
        "--minimum",
        type=float,
        required=True,
    )

    parser.add_argument(
        "--maximum",
        type=float,
        required=True,
    )

    parser.add_argument(
        "--output",
        required=True,
    )

    args = parser.parse_args()

    spark = (
        SparkSession.builder
        .appName(
            f"DataPilot-Silver-Inspection-{args.dataset}"
        )
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "com.amazonaws.auth.profile.ProfileCredentialsProvider",
        )
        .config(
            "spark.hadoop.fs.s3a.endpoint.region",
            REGION,
        )
        .getOrCreate()
    )

    try:

        result = inspect_silver(
            spark=spark,
            dataset=args.dataset,
            column=args.column,
            minimum=args.minimum,
            maximum=args.maximum,
        )

        output_directory = os.path.dirname(
            args.output
        )

        if output_directory:
            os.makedirs(
                output_directory,
                exist_ok=True,
            )

        with open(
            args.output,
            "w",
            encoding="utf-8",
        ) as file:
            json.dump(
                result,
                file,
                indent=2,
                default=str,
            )

        print(
            json.dumps(
                result,
                indent=2,
                default=str,
            )
        )

        print(
            f"\nInspection result saved to: {args.output}"
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()