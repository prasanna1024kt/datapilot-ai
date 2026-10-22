"""Read-only, in-memory regression tests for Gold payment filtering/verification."""
from datetime import datetime
from decimal import Decimal

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from framework.payment_quality import split_valid_and_invalid_payments
from gold_incremental_iceberg import verify_records_are_quarantined


SOURCE_SCHEMA = StructType([
    StructField("order_id", StringType(), False),
    StructField("payment_sequential", IntegerType(), False),
    StructField("payment_type", StringType(), True),
    StructField("payment_installments", IntegerType(), True),
    StructField("payment_value", DecimalType(12, 2), True),
])

QUARANTINE_SCHEMA = StructType(SOURCE_SCHEMA.fields + [
    StructField("execution_id", StringType(), False),
    StructField("quarantine_reason", StringType(), False),
    StructField("rule_name", StringType(), False),
    StructField("source_layer", StringType(), False),
    StructField("quarantined_at", TimestampType(), False),
])


def test_gold_payment_filter_and_quarantine_verification():
    spark = (
        SparkSession.builder
        .appName("DataPilot-Gold-Payment-Filter-Regression")
        .master("local[2]")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    try:
        source_rows = [
            ("valid-min", 1, "credit_card", 1, Decimal("10.00")),
            ("valid-max", 1, "credit_card", 100, Decimal("20.00")),
            ("invalid-zero", 1, "credit_card", 0, Decimal("30.00")),
            ("invalid-null", 1, "credit_card", None, Decimal("40.00")),
        ]
        source = spark.createDataFrame(source_rows, SOURCE_SCHEMA)
        valid, invalid = split_valid_and_invalid_payments(source)
        assert valid.count() == 2
        assert invalid.count() == 2
        assert {r.order_id for r in valid.select("order_id").collect()} == {"valid-min", "valid-max"}
        assert {r.order_id for r in invalid.select("order_id").collect()} == {"invalid-zero", "invalid-null"}
        print("PASS: Gold payment source split excludes invalid installment values")

        quarantine_rows = [
            ("invalid-zero", 1, "credit_card", 0, Decimal("30.00"), "exec-1", "installment range", "payment_installments_range", "silver", datetime(2026, 1, 1)),
            ("invalid-null", 1, "credit_card", None, Decimal("40.00"), "exec-1", "installment range", "payment_installments_range", "silver", datetime(2026, 1, 1)),
        ]
        quarantine = spark.createDataFrame(quarantine_rows, QUARANTINE_SCHEMA)
        verify_records_are_quarantined(
            invalid, quarantine,
            ["order_id", "payment_sequential"],
            ["order_id", "payment_sequential", "payment_type", "payment_installments", "payment_value"],
            "test invalid records",
        )
        print("PASS: matching quarantined records are accepted")

        incomplete = quarantine.filter("order_id <> 'invalid-null'")
        try:
            verify_records_are_quarantined(
                invalid, incomplete,
                ["order_id", "payment_sequential"],
                ["order_id", "payment_sequential", "payment_type", "payment_installments", "payment_value"],
                "test missing record",
            )
        except RuntimeError as exc:
            assert "Gold changes are blocked" in str(exc)
        else:
            raise AssertionError("Missing quarantine record was not rejected")
        print("PASS: missing quarantine records block Gold changes")

        mismatch_rows = [
            ("invalid-zero", 1, "credit_card", 0, Decimal("31.00"), "exec-1", "installment range", "payment_installments_range", "silver", datetime(2026, 1, 1)),
            quarantine_rows[1],
        ]
        mismatched = spark.createDataFrame(mismatch_rows, QUARANTINE_SCHEMA)
        try:
            verify_records_are_quarantined(
                invalid, mismatched,
                ["order_id", "payment_sequential"],
                ["order_id", "payment_sequential", "payment_type", "payment_installments", "payment_value"],
                "test mismatched record",
            )
        except RuntimeError as exc:
            assert "Gold changes are blocked" in str(exc)
        else:
            raise AssertionError("Mismatched quarantine record was not rejected")
        print("PASS: mismatched quarantine values block Gold changes")

        duplicated = spark.createDataFrame(quarantine_rows + [quarantine_rows[0]], QUARANTINE_SCHEMA)
        try:
            verify_records_are_quarantined(
                invalid, duplicated,
                ["order_id", "payment_sequential"],
                ["order_id", "payment_sequential", "payment_type", "payment_installments", "payment_value"],
                "test duplicate quarantine key",
            )
        except RuntimeError as exc:
            assert "duplicate business keys" in str(exc)
        else:
            raise AssertionError("Duplicate quarantine keys were not rejected")
        print("PASS: duplicate quarantine keys block Gold changes")
        print("PASS: all tests are in-memory; no Iceberg or S3 writes performed")
    finally:
        spark.stop()


if __name__ == "__main__":
    test_gold_payment_filter_and_quarantine_verification()
