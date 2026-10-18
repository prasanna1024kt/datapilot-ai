from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    TimestampType,
)

EXPECTED_TYPES = {
    "execution_id": "string",
    "dataset": "string",
    "action": "string",
    "status": "string",
    "execution_mode": "string",
    "source_count": "bigint",
    "invalid_count": "bigint",
    "started_at": "timestamp",
    "completed_at": "timestamp",
    "error_message": "string",
}

SCHEMA = StructType([
    StructField("execution_id", StringType(), False),
    StructField("dataset", StringType(), False),
    StructField("action", StringType(), False),
    StructField("status", StringType(), False),
    StructField("execution_mode", StringType(), False),
    StructField("source_count", LongType(), True),
    StructField("invalid_count", LongType(), True),
    StructField("started_at", TimestampType(), False),
    StructField("completed_at", TimestampType(), True),
    StructField("error_message", StringType(), True),
])


def main():
    spark = (
        SparkSession.builder
        .appName("DataPilotAI-ValidateExecutionHistorySchema")
        .getOrCreate()
    )

    try:
        df = spark.createDataFrame([], SCHEMA)

        actual_types = {
            field.name: field.dataType.simpleString()
            for field in df.schema.fields
        }

        print("=== Proposed execution-history schema ===")
        df.printSchema()

        if actual_types != EXPECTED_TYPES:
            raise ValueError(
                f"Schema mismatch.\nExpected: {EXPECTED_TYPES}\n"
                f"Actual: {actual_types}"
            )

        print("\nSchema validation: PASS")
        print("History table: glue_catalog.datapilot.remediation_execution_history")
        print("Iceberg table created: False")
        print("S3 writes performed: False")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()

