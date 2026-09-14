from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("DataPilot-S3A-Write-Test")
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
    jvm = spark._jvm
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()

    path = "s3a://datapilot-ai-data-practice/processed/test-hadoop/test.txt"

    print(f"Testing S3A write: {path}", flush=True)

    fs = jvm.org.apache.hadoop.fs.FileSystem.get(
        jvm.org.apache.hadoop.fs.Path(path).toUri(),
        hadoop_conf
    )

    output_path = jvm.org.apache.hadoop.fs.Path(path)

    print("Creating file...", flush=True)

    output_stream = fs.create(output_path, True)
    output_stream.write(bytearray(b"DataPilot S3A test"))
    output_stream.close()

    print("S3A WRITE SUCCESSFUL", flush=True)

finally:
    spark.stop()