import argparse

import yaml

from pyspark.sql import SparkSession

from framework.business_transformer import BusinessTransformer


def parse_args():

    parser = argparse.ArgumentParser(
        description="DataPilot AI Gold Layer Runner"
    )

    parser.add_argument(
        "--table",
        required=True,
        choices=[
            "olist_order_summary"
        ]
    )

    parser.add_argument(
        "--config-root",
        default="/opt/datapilot/config"
    )

    return parser.parse_args()


def load_variables(config_root):

    path = f"{config_root}/variables.yaml"

    with open(path, "r") as file:
        return yaml.safe_load(file)


def load_gold_config(config_root, table):

    path = f"{config_root}/gold/{table}.yaml"

    with open(path, "r") as file:
        config = yaml.safe_load(file)

    if not config:
        raise ValueError(
            f"Gold configuration is empty or invalid: {path}"
        )

    return config


def build_silver_path(variables, table):

    bucket = variables["storage"]["bucket"]
    silver_path = variables["storage"]["layers"]["silver"]

    return f"s3a://{bucket}/{silver_path}/{table}"


def build_gold_path(variables, table):

    bucket = variables["storage"]["bucket"]
    gold_path = variables["storage"]["layers"]["gold"]

    return f"s3a://{bucket}/{gold_path}/{table}"


def main():

    args = parse_args()

    spark = (
        SparkSession.builder
        .appName(f"DataPilot-Gold-{args.table}")
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

        variables = load_variables(
            args.config_root
        )

        gold_config = load_gold_config(
            args.config_root,
            args.table
        )

        tables = gold_config["source"]["tables"]

        print("")
        print("========================================")
        print(f"GOLD BUILD: {args.table}")
        print("========================================")

        # Read Silver datasets

        orders = spark.read.parquet(
            build_silver_path(
                variables,
                tables["orders"]
            )
        )

        order_items = spark.read.parquet(
            build_silver_path(
                variables,
                tables["order_items"]
            )
        )

        payments = spark.read.parquet(
            build_silver_path(
                variables,
                tables["payments"]
            )
        )

        customers = spark.read.parquet(
            build_silver_path(
                variables,
                tables["customers"]
            )
        )

        print("Silver datasets loaded successfully")

        # Business transformation

        transformer = BusinessTransformer(spark)

        result = transformer.build_order_summary(
            orders=orders,
            order_items=order_items,
            payments=payments,
            customers=customers,
        )

        target_path = build_gold_path(
            variables,
            args.table
        )

        print(f"Target: {target_path}")
        print("Writing Gold dataset...")

        (
            result.write
            .format("parquet")
            .mode("overwrite")
            .save(target_path)
        )

        print("")
        print("========================================")
        print("GOLD BUILD SUCCESS")
        print("========================================")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()