from pyspark.sql import SparkSession


CATALOG = "glue_catalog"
DATABASE = "datapilot"
TABLE = "iceberg_smoke_test"

WAREHOUSE = "s3a://datapilot-ai-data-practice/gold"


spark = (
    SparkSession.builder
    .appName("DataPilot-Iceberg-Glue-Smoke-Test")

    # Iceberg Spark catalog
    .config(
        f"spark.sql.catalog.{CATALOG}",
        "org.apache.iceberg.spark.SparkCatalog",
    )
    .config(
        f"spark.sql.catalog.{CATALOG}.catalog-impl",
        "org.apache.iceberg.aws.glue.GlueCatalog",
    )
    .config(
        f"spark.sql.catalog.{CATALOG}.warehouse",
        WAREHOUSE,
    )

    # S3
    .config(
        f"spark.sql.catalog.{CATALOG}.io-impl",
        "org.apache.iceberg.aws.s3.S3FileIO",
    )

    .getOrCreate()
)


print("\n========================================")
print("ICEBERG + GLUE CATALOG TEST")
print("========================================")

print("Spark version:", spark.version)

# ---------------------------------------------------------
# Create database if necessary
# ---------------------------------------------------------

spark.sql(
    f"CREATE DATABASE IF NOT EXISTS {CATALOG}.{DATABASE}"
)

print(f"Database ready: {CATALOG}.{DATABASE}")


# ---------------------------------------------------------
# Create test data
# ---------------------------------------------------------

data = [
    (1, "order-001", 2, 100.50),
    (2, "order-002", 1, 250.00),
]

df = spark.createDataFrame(
    data,
    [
        "id",
        "order_id",
        "payment_installments",
        "payment_value",
    ],
)


# ---------------------------------------------------------
# Create Iceberg table
# ---------------------------------------------------------

spark.sql(
    f"""
    DROP TABLE IF EXISTS
    {CATALOG}.{DATABASE}.{TABLE}
    """
)

df.writeTo(
    f"{CATALOG}.{DATABASE}.{TABLE}"
).using("iceberg").create()


print(
    f"Created table: "
    f"{CATALOG}.{DATABASE}.{TABLE}"
)


# ---------------------------------------------------------
# Read table
# ---------------------------------------------------------

result = spark.table(
    f"{CATALOG}.{DATABASE}.{TABLE}"
)

result.show()


# ---------------------------------------------------------
# List tables
# ---------------------------------------------------------

print("Tables:")

spark.sql(
    f"SHOW TABLES IN {CATALOG}.{DATABASE}"
).show(truncate=False)


print("\n========================================")
print("ICEBERG + GLUE TEST PASSED")
print("========================================")

spark.stop()