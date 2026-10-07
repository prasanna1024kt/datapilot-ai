import argparse

import yaml

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from framework.business_transformer import BusinessTransformer


ICEBERG_CATALOG = "glue_catalog"
ICEBERG_DATABASE = "datapilot"


def parse_args():

    parser = argparse.ArgumentParser(
        description="DataPilot AI Gold Summary Layer Runner"
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

    with open(path, "r", encoding="utf-8") as file:
        return yaml.safe_load(file)


def load_gold_config(config_root, table):

    path = f"{config_root}/gold/{table}.yaml"

    with open(path, "r", encoding="utf-8") as file:
        config = yaml.safe_load(file)

    if not config:
        raise ValueError(
            f"Gold configuration is empty or invalid: {path}"
        )

    return config


def build_iceberg_table_name(table):

    return f"{ICEBERG_CATALOG}.{ICEBERG_DATABASE}.{table}"


def build_iceberg_warehouse(variables):

    bucket = variables["storage"]["bucket"]
    gold_path = variables["storage"]["layers"]["gold"]

    return f"s3a://{bucket}/{gold_path}"


def configure_iceberg(spark, warehouse):

    return (
        spark
        .config(
            f"spark.sql.catalog.{ICEBERG_CATALOG}",
            "org.apache.iceberg.spark.SparkCatalog"
        )
        .config(
            f"spark.sql.catalog.{ICEBERG_CATALOG}.catalog-impl",
            "org.apache.iceberg.aws.glue.GlueCatalog"
        )
        .config(
            f"spark.sql.catalog.{ICEBERG_CATALOG}.warehouse",
            warehouse
        )
        .config(
            f"spark.sql.catalog.{ICEBERG_CATALOG}.io-impl",
            "org.apache.iceberg.aws.s3.S3FileIO"
        )
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider"
        )
        .config(
            "spark.hadoop.fs.s3a.endpoint.region",
            "us-east-1"
        )
    )


def table_exists(spark, table_name):

    try:
        spark.table(table_name)
        return True
    except Exception:
        return False


def validate_source_duplicates(df, key_columns):

    duplicate_count = (
        df.groupBy(*key_columns)
        .count()
        .filter(col("count") > 1)
        .count()
    )

    if duplicate_count > 0:
        raise RuntimeError(
            f"Source contains {duplicate_count} duplicate key groups "
            f"for keys: {key_columns}"
        )


def validate_result(df, key_columns):

    total_count = df.count()

    if total_count == 0:
        raise RuntimeError(
            "Gold summary transformation produced zero records."
        )

    duplicate_count = (
        df.groupBy(*key_columns)
        .count()
        .filter(col("count") > 1)
        .count()
    )

    if duplicate_count > 0:
        raise RuntimeError(
            f"Gold summary contains {duplicate_count} duplicate key groups "
            f"for keys: {key_columns}"
        )

    print("")
    print("Summary validation")
    print("------------------")
    print(f"Record count      : {total_count}")
    print(f"Duplicate groups  : {duplicate_count}")


def create_database(spark):

    spark.sql(
        f"CREATE DATABASE IF NOT EXISTS "
        f"{ICEBERG_CATALOG}.{ICEBERG_DATABASE}"
    )


def create_iceberg_table(spark, table_name, result):

    print("")
    print("Creating Iceberg summary table...")
    print(f"Table: {table_name}")

    (
        result.writeTo(table_name)
        .using("iceberg")
        .tableProperty("format-version", "2")
        .create()
    )

    print("Iceberg table created successfully")


def merge_iceberg_table(spark, table_name, result, key_columns):

    print("")
    print("Merging summary into Iceberg table...")
    print(f"Table: {table_name}")

    result.createOrReplaceTempView("gold_summary_source")

    merge_condition = " AND ".join(
        [
            f"target.{column} = source.{column}"
            for column in key_columns
        ]
    )

    spark.sql(
        f"""
        MERGE INTO {table_name} AS target
        USING gold_summary_source AS source
        ON {merge_condition}

        WHEN MATCHED THEN
          UPDATE SET *

        WHEN NOT MATCHED THEN
          INSERT *
        """
    )

    spark.catalog.dropTempView("gold_summary_source")

    print("Iceberg MERGE completed successfully")


def main():

    args = parse_args()

    # Load configuration first so warehouse can be configured
    variables = load_variables(args.config_root)

    gold_config = load_gold_config(
        args.config_root,
        args.table
    )

    warehouse = build_iceberg_warehouse(variables)

    spark_builder = (
        SparkSession.builder
        .appName(f"DataPilot-Gold-Summary-{args.table}")
    )

    spark_builder = configure_iceberg(
        spark_builder,
        warehouse
    )

    spark = spark_builder.getOrCreate()

    try:

        source_tables = gold_config["source"]["tables"]

        target_config = gold_config["target"]

        target_table = target_config["table"]

        target_table_name = build_iceberg_table_name(
            target_table
        )

        key_columns = ["order_id"]

        print("")
        print("========================================")
        print(f"GOLD SUMMARY BUILD: {args.table}")
        print("========================================")

        print("")
        print("Iceberg source tables")
        print("---------------------")

        print(
            f"Orders        : "
            f"{build_iceberg_table_name(source_tables['orders'])}"
        )

        print(
            f"Order Items   : "
            f"{build_iceberg_table_name(source_tables['order_items'])}"
        )

        print(
            f"Payments      : "
            f"{build_iceberg_table_name(source_tables['payments'])}"
        )

        print(
            f"Customers     : "
            f"{build_iceberg_table_name(source_tables['customers'])}"
        )

        print(
            f"Products      : "
            f"{build_iceberg_table_name(source_tables['products'])}"
        )

        # -------------------------------------------------
        # Create Glue database
        # -------------------------------------------------

        create_database(spark)

        # -------------------------------------------------
        # Read Gold Iceberg tables
        # -------------------------------------------------

        orders = spark.table(
            build_iceberg_table_name(
                source_tables["orders"]
            )
        )

        order_items = spark.table(
            build_iceberg_table_name(
                source_tables["order_items"]
            )
        )

        payments = spark.table(
            build_iceberg_table_name(
                source_tables["payments"]
            )
        )

        customers = spark.table(
            build_iceberg_table_name(
                source_tables["customers"]
            )
        )

        products = spark.table(
            build_iceberg_table_name(
                source_tables["products"]
            )
        )

        print("")
        print("Gold Iceberg datasets loaded successfully")

        print(f"Orders       : {orders.count()}")
        print(f"Order Items  : {order_items.count()}")
        print(f"Payments     : {payments.count()}")
        print(f"Customers    : {customers.count()}")
        print(f"Products     : {products.count()}")

        # -------------------------------------------------
        # Validate source keys
        # -------------------------------------------------

        validate_source_duplicates(
            orders,
            ["order_id"]
        )

        validate_source_duplicates(
            order_items,
            ["order_id", "order_item_id"]
        )

        validate_source_duplicates(
            payments,
            ["order_id", "payment_sequential"]
        )

        validate_source_duplicates(
            customers,
            ["customer_id"]
        )

        validate_source_duplicates(
            products,
            ["product_id"]
        )

        print("")
        print("Source duplicate validation passed")

        # -------------------------------------------------
        # Business transformation
        # -------------------------------------------------

        transformer = BusinessTransformer(spark)

        result = transformer.build_order_summary(
            orders=orders,
            order_items=order_items,
            payments=payments,
            customers=customers,
        )

        # Products is loaded because it is part of the summary
        # source configuration. The current BusinessTransformer
        # method does not accept products as an argument.

        # -------------------------------------------------
        # Result validation
        # -------------------------------------------------

        validate_result(
            result,
            key_columns
        )

        # -------------------------------------------------
        # Add Gold metadata
        # -------------------------------------------------

        from pyspark.sql.functions import current_timestamp, lit

        result = (
            result
            .withColumn(
                "gold_ingestion_timestamp",
                current_timestamp()
            )
            .withColumn(
                "gold_source_layer",
                lit("gold")
            )
        )

        # -------------------------------------------------
        # Iceberg CREATE / MERGE
        # -------------------------------------------------

        if not table_exists(
            spark,
            target_table_name
        ):

            create_iceberg_table(
                spark,
                target_table_name,
                result
            )

        else:

            merge_iceberg_table(
                spark,
                target_table_name,
                result,
                key_columns
            )

        # -------------------------------------------------
        # Final validation
        # -------------------------------------------------

        final_df = spark.table(
            target_table_name
        )

        final_count = final_df.count()

        if final_count == 0:
            raise RuntimeError(
                "Final Iceberg summary table contains zero records."
            )

        print("")
        print("========================================")
        print("GOLD SUMMARY BUILD SUCCESS")
        print("========================================")
        print(f"Target table : {target_table_name}")
        print(f"Record count : {final_count}")
        print("Storage      : Apache Iceberg")
        print("Source       : Gold Iceberg tables")
        print("Mode         : Incremental MERGE")
        print("========================================")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()