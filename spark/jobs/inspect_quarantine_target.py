from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count

from framework.config_loader import ConfigLoader

CONFIG_ROOT = "/opt/datapilot/config"
TABLE = "glue_catalog.datapilot.olist_order_payments_quarantine"
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
        .appName("DataPilotAI-InspectQuarantineTarget")
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

        exists = spark.catalog.tableExists(TABLE)

        if not exists:
            print("\nRESULT: Quarantine table does not exist.")
            print("No table was created.")
            print("No S3 writes were performed.")
            return

        print("\nRESULT: Quarantine table exists.")
        target = spark.table(TABLE)

        print("\n=== Existing table schema ===")
        target.printSchema()

        missing = sorted(set(BUSINESS_KEYS) - set(target.columns))
        if missing:
            print(f"\nSAFETY FAILURE: Missing business-key columns: {missing}")
            return

        null_key_condition = (
            col("order_id").isNull()
            | col("payment_sequential").isNull()
        )
        null_key_count = target.filter(null_key_condition).count()

        duplicate_keys = (
            target.groupBy(*BUSINESS_KEYS)
            .agg(count("*").alias("key_count"))
            .filter(col("key_count") > 1)
        )
        duplicate_key_count = duplicate_keys.count()

        print(f"\nExisting quarantine rows: {target.count()}")
        print(f"Rows with null business keys: {null_key_count}")
        print(f"Duplicate business-key groups: {duplicate_key_count}")

        if null_key_count or duplicate_key_count:
            print("SAFETY FAILURE: Target requires investigation.")
        else:
            print("Target business-key integrity check passed.")

        print("\nNo CREATE TABLE, MERGE, or S3 writes were performed.")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()