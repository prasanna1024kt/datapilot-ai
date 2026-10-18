from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DecimalType, TimestampType,
)

EXPECTED_TYPES = {
    "order_id": "string",
    "payment_sequential": "int",
    "payment_type": "string",
    "payment_installments": "int",
    "payment_value": "decimal(12,2)",
    "execution_id": "string",
    "quarantine_reason": "string",
    "rule_name": "string",
    "source_layer": "string",
    "quarantined_at": "timestamp",
}

SCHEMA = StructType([
    StructField("order_id", StringType(), True),
    StructField("payment_sequential", IntegerType(), True),
    StructField("payment_type", StringType(), True),
    StructField("payment_installments", IntegerType(), True),
    StructField("payment_value", DecimalType(12, 2), True),
    StructField("execution_id", StringType(), False),
    StructField("quarantine_reason", StringType(), False),
    StructField("rule_name", StringType(), False),
    StructField("source_layer", StringType(), False),
    StructField("quarantined_at", TimestampType(), False),
])

def main():
    spark = (
        SparkSession.builder
        .appName("DataPilotAI-ValidateQuarantineSchema")
        .getOrCreate()
    )

    try:
        empty_df = spark.createDataFrame([], SCHEMA)
        actual_types = {
            field.name: field.dataType.simpleString()
            for field in empty_df.schema.fields
        }

        print("=== Proposed quarantine schema ===")
        empty_df.printSchema()

        if actual_types != EXPECTED_TYPES:
            raise ValueError(
                f"Schema mismatch.\nExpected: {EXPECTED_TYPES}\n"
                f"Actual: {actual_types}"
            )

        print("\nSchema validation: PASS")
        print("Business keys: order_id, payment_sequential")
        print("Glue table created: False")
        print("S3 writes performed: False")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
