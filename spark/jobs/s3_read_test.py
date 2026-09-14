from pyspark.sql import SparkSession

BUCKET = "s3a://datapilot-ai-data-practice"

INPUT_PATH = f"{BUCKET}/raw/olist_orders_dataset.csv"
OUTPUT_PATH = f"{BUCKET}/processed/test/olist_orders_dataset_sample"

spark = (
    SparkSession.builder
    .appName("DataPilot-S3-Test")
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
    print("=" * 80, flush=True)
    print("DATAPILOT AI - SPARK S3 TEST", flush=True)
    print("=" * 80, flush=True)

    print(f"Input: {INPUT_PATH}", flush=True)

    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(INPUT_PATH)
    )

    print("\nINPUT RECORD COUNT:", flush=True)
    print(df.count(), flush=True)

    print("\nSCHEMA:", flush=True)
    df.printSchema()

    print("\nSAMPLE DATA:", flush=True)
    df.show(10, truncate=False)

    print("\nWRITING PROCESSED DATA...", flush=True)

    (
        df.limit(100)
        .write
        .mode("overwrite")
        .option("header", "true")
        .csv(OUTPUT_PATH)
    )

    print("\nSUCCESS!", flush=True)
    print(f"Output: {OUTPUT_PATH}", flush=True)
    print("=" * 80, flush=True)

finally:
    spark.stop()