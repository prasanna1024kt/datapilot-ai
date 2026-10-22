from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count

from framework.config_loader import ConfigLoader

CONFIG_ROOT = "/opt/datapilot/config"
TABLE = "glue_catalog.datapilot.olist_order_payments"
BUSINESS_KEYS = ["order_id", "payment_sequential"]


def main():
    loader = ConfigLoader(CONFIG_ROOT)
    variables = loader.load_variables()

    region = variables["aws"]["region"]
    bucket = variables["storage"]["bucket"]
    gold_layer = variables["storage"]["layers"]["gold"]
    warehouse = f"s3a://{bucket}/{gold_layer}"

    spark = (
        SparkSession.builder
        .appName("DataPilotAI-InspectGoldPayments")
        .config(
            "spark.sql.catalog.glue_catalog",
            "org.apache.iceberg.spark.SparkCatalog",
        )
        .config(
            "spark.sql.catalog.glue_catalog.catalog-impl",
            "org.apache.iceberg.aws.glue.GlueCatalog",
        )
        .config(
            "spark.sql.catalog.glue_catalog.warehouse",
            warehouse,
        )
        .config(
            "spark.sql.catalog.glue_catalog.io-impl",
            "org.apache.iceberg.aws.s3.S3FileIO",
        )
        .config(
            "spark.sql.catalog.glue_catalog.client.region",
            region,
        )
        .getOrCreate()
    )

    try:
        print(f"Target table: {TABLE}")
        print(f"Catalog region: {region}")
        print("Operation: READ ONLY")

        if not spark.catalog.tableExists(TABLE):
            print("\nRESULT: Gold payments table does not exist.")
            print("No table was created. No writes were performed.")
            return

        gold = spark.table(TABLE)
        print("\n=== Gold payments schema ===")
        gold.printSchema()

        required = set(BUSINESS_KEYS) | {"payment_installments"}
        missing = sorted(required - set(gold.columns))
        if missing:
            raise RuntimeError(f"Missing required columns: {missing}")

        total = gold.count()
        invalid_condition = (
            col("payment_installments").isNull()
            | (col("payment_installments") < 1)
            | (col("payment_installments") > 100)
        )
        invalid = gold.filter(invalid_condition)
        invalid_count = invalid.count()

        null_keys = gold.filter(
            col("order_id").isNull()
            | col("payment_sequential").isNull()
        ).count()

        duplicate_count = (
            gold.groupBy(*BUSINESS_KEYS)
            .agg(count("*").alias("key_count"))
            .filter(col("key_count") > 1)
            .count()
        )

        print("\n=== READ-ONLY GOLD REPORT ===")
        print(f"Gold rows: {total}")
        print(f"Invalid payment rows: {invalid_count}")
        print(f"Rows with null business keys: {null_keys}")
        print(f"Duplicate business-key groups: {duplicate_count}")

        if invalid_count:
            print("\n=== Invalid Gold payment records ===")
            invalid.select(
                "order_id",
                "payment_sequential",
                "payment_installments",
                "payment_value",
            ).show(20, truncate=False)

        print("\nNo CREATE TABLE, MERGE, DELETE, or S3 writes were performed.")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
