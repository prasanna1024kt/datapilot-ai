from pyspark.sql import SparkSession

CATALOG = "glue_catalog"
DATABASE = "datapilot"
TABLE = "olist_order_payments"
WAREHOUSE = "s3a://datapilot-ai-data-practice/gold"

spark = (
    SparkSession.builder
    .appName("DataPilot-Drop-Iceberg-Table")
    .config(f"spark.sql.catalog.{CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
    .config(f"spark.sql.catalog.{CATALOG}.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
    .config(f"spark.sql.catalog.{CATALOG}.warehouse", WAREHOUSE)
    .config(f"spark.sql.catalog.{CATALOG}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    .config("spark.sql.shuffle.partitions", "4")
    .config("spark.default.parallelism", "4")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

target = f"{CATALOG}.{DATABASE}.{TABLE}"

print(f"Dropping: {target}")

spark.sql(f"DROP TABLE IF EXISTS {target} PURGE")

print("DROP completed successfully.")

spark.stop()
