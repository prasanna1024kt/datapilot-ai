from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("DataPilot-S3-Test")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

path = "s3a://datapilot-ai-data-practice/processed/silver/olist_order_payments"

print("Testing S3 access...")
print(f"Path: {path}")

df = spark.read.parquet(path)

print(f"Records: {df.count()}")

df.printSchema()

spark.stop()