import argparse

from pyspark.sql import SparkSession

from framework.pipeline import DataPipeline


def parse_args():

    parser = argparse.ArgumentParser(
        description="DataPilot AI Pipeline Runner"
    )

    parser.add_argument(
        "--layer",
        required=True,
        choices=[
            "bronze",
            "silver",
            "gold"
        ]
    )

    parser.add_argument(
        "--table",
        required=True
    )

    parser.add_argument(
        "--config-root",
        default="/opt/datapilot/config"
    )

    return parser.parse_args()


def main():

    args = parse_args()

    spark = (
        SparkSession.builder
        .appName("DataPilot-S3-Parquet-Test")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider"
        )
        .config(
            "spark.hadoop.fs.s3a.endpoint.region",
            "us-east-1"
        )
        .getOrCreate()
)

    try:

        pipeline = DataPipeline(
            spark=spark,
            config_root=args.config_root
        )

        pipeline.run(
            layer=args.layer,
            table=args.table
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()