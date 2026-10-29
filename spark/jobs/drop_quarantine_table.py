from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("DataPilotAI-Drop-Quarantine-Table")
    .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
    )
    .config(
        "spark.sql.catalog.glue_catalog",
        "org.apache.iceberg.spark.SparkCatalog"
    )
    .config(
        "spark.sql.catalog.glue_catalog.catalog-impl",
        "org.apache.iceberg.aws.glue.GlueCatalog"
    )
    .config(
        "spark.sql.catalog.glue_catalog.warehouse",
        "s3://datapilot-ai-data-practice/processed/quarantine"
    )
    .config(
        "spark.sql.catalog.glue_catalog.io-impl",
        "org.apache.iceberg.aws.s3.S3FileIO"
    )
    .config(
        "spark.sql.catalog.glue_catalog.client.region",
        "us-east-1"
    )
    .getOrCreate()
)

try:
    table = "glue_catalog.datapilot.olist_order_payments_quarantine"

    if spark.catalog.tableExists(table):
        spark.sql(f"DROP TABLE {table} PURGE")
        print(f"TABLE_DROPPED_AND_PURGED: {table}")
    else:
        print(f"TABLE_NOT_FOUND: {table}")
finally:
    spark.stop()
