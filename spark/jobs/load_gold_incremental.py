import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp


BUCKET = "datapilot-ai-data-practice"
REGION = "us-east-1"


def main():

    parser = argparse.ArgumentParser(
        description="DataPilot AI incremental Gold loader"
    )

    parser.add_argument("--dataset", required=True)
    parser.add_argument("--source", required=True)
    parser.add_argument("--gold", required=True)

    parser.add_argument(
        "--keys",
        required=True,
        help="Comma-separated business keys"
    )

    args = parser.parse_args()

    spark = (
        SparkSession.builder
        .appName(
            f"DataPilot-Gold-Incremental-{args.dataset}"
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

        source_df = spark.read.parquet(args.source)

        keys = [
            key.strip()
            for key in args.keys.split(",")
        ]

        missing_keys = [
            key
            for key in keys
            if key not in source_df.columns
        ]

        if missing_keys:
            raise ValueError(
                f"Missing business keys: {missing_keys}"
            )

        source_df = (
            source_df
            .withColumn(
                "_gold_loaded_at",
                current_timestamp()
            )
        )

        print(
            f"Loading source: {args.source}"
        )

        print(
            f"Gold target: {args.gold}"
        )

        print(
            f"Business keys: {keys}"
        )

        # -------------------------------------------------
        # First load
        # -------------------------------------------------

        try:

            existing_df = spark.read.parquet(
                args.gold
            )

            print("Existing Gold dataset found.")

            # Remove existing records having the same
            # business keys as the incoming batch.

            join_condition = [
                existing_df[key] == source_df[key]
                for key in keys
            ]

            existing_without_batch = (
                existing_df.alias("existing")
                .join(
                    source_df.alias("incoming"),
                    on=join_condition,
                    how="left_anti"
                )
            )

            final_df = (
                existing_without_batch
                .unionByName(
                    source_df,
                    allowMissingColumns=True
                )
            )

            print(
                "Performing incremental Gold merge."
            )

        except Exception:

            print(
                "Gold dataset does not exist. "
                "Creating initial Gold dataset."
            )

            final_df = source_df

        # -------------------------------------------------
        # Write Gold
        # -------------------------------------------------

        (
            final_df
            .write
            .mode("overwrite")
            .parquet(args.gold)
        )

        print(
            f"Gold load completed for {args.dataset}"
        )

        print(
            f"Gold record count: {final_df.count()}"
        )

    finally:

        spark.stop()


if __name__ == "__main__":
    main()