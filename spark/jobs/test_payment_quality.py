from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DecimalType,
)
from decimal import Decimal

from framework.payment_quality import split_valid_and_invalid_payments


def main():
    spark = (
        SparkSession.builder
        .appName("DataPilot-Payment-Quality-Test")
        .master("local[2]")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    schema = StructType([
        StructField("order_id", StringType(), True),
        StructField("payment_sequential", IntegerType(), True),
        StructField("payment_type", StringType(), True),
        StructField("payment_installments", IntegerType(), True),
        StructField("payment_value", DecimalType(12, 2), True),
    ])

    rows = [
        ("order-valid-min", 1, "credit_card", 1, Decimal("10.00")),
        ("order-valid-max", 1, "credit_card", 100, Decimal("20.00")),
        ("order-invalid-zero", 1, "credit_card", 0, Decimal("30.00")),
        ("order-invalid-high", 1, "credit_card", 101, Decimal("40.00")),
        ("order-invalid-null", 1, "credit_card", None, Decimal("50.00")),
    ]

    payments = spark.createDataFrame(rows, schema)

    valid, invalid = split_valid_and_invalid_payments(payments)

    valid_count = valid.count()
    invalid_count = invalid.count()

    assert valid_count == 2, f"Expected 2 valid rows, got {valid_count}"
    assert invalid_count == 3, f"Expected 3 invalid rows, got {invalid_count}"

    assert {
        row["order_id"] for row in invalid.select("order_id").collect()
    } == {
        "order-invalid-zero",
        "order-invalid-high",
        "order-invalid-null",
    }

    print("PASS: installment boundaries 1 and 100 are valid")
    print("PASS: zero, above-100, and NULL installments are invalid")
    print(f"PASS: valid={valid_count}, invalid={invalid_count}")

    spark.stop()


if __name__ == "__main__":
    main()
