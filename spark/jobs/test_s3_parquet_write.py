from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("DataPilot-S3-Parquet-Test")
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
    print("STEP 1: Spark started", flush=True)

    df = spark.createDataFrame(
        [
            (1, "bronze-test"),
            (2, "datapilot"),
            (3, "spark-s3"),
        ],
        ["id", "name"]
    )

    print("STEP 2: DataFrame created", flush=True)

    target = (
        "s3a://datapilot-ai-data-practice/"
        "processed/bronze/test_write"
    )

    print(f"STEP 3: Writing to {target}", flush=True)

    df.write \
        .mode("overwrite") \
        .format("parquet") \
        .save(target)

    print("STEP 4: WRITE SUCCESSFUL", flush=True)

finally:
    spark.stop()