"""Read-only tests for quarantine APPLY pre-write guards."""
from decimal import Decimal

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import DecimalType, IntegerType, StringType, StructField, StructType

from quarantine_records_candidate import (
    build_apply_failure_report,
    assert_existing_quarantine_rows_match,
    assert_expected_table_location,
    assert_iceberg_provider,
    assert_other_dq_rules_valid,
    normalize_s3_location,
)


SCHEMA = StructType([
    StructField("order_id", StringType(), False),
    StructField("payment_sequential", IntegerType(), False),
    StructField("payment_type", StringType(), True),
    StructField("payment_installments", IntegerType(), True),
    StructField("payment_value", DecimalType(12, 2), True),
])
COLUMNS = [field.name for field in SCHEMA.fields]


def test_s3a_and_s3_locations_normalize_to_same_location():
    assert normalize_s3_location(
        "s3a://datapilot-ai-data-practice/processed/quarantine/olist_order_payments/"
    ) == "s3://datapilot-ai-data-practice/processed/quarantine/olist_order_payments"


def test_wrong_registered_location_is_rejected():
    with pytest.raises(RuntimeError, match="location mismatch"):
        assert_expected_table_location(
            "s3://wrong-bucket/processed/quarantine/olist_order_payments",
            "s3a://datapilot-ai-data-practice/processed/quarantine/olist_order_payments",
        )


def test_existing_matching_quarantine_rows_are_accepted():
    spark = SparkSession.builder.master("local[2]").appName("quarantine-guard-test").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    try:
        rows = [("order-a", 1, "credit_card", 0, Decimal("10.00"))]
        invalid = spark.createDataFrame(rows, SCHEMA)
        target = spark.createDataFrame(rows, SCHEMA)
        assert_existing_quarantine_rows_match(invalid, target, COLUMNS)
    finally:
        spark.stop()


def test_existing_mismatched_quarantine_row_is_rejected_before_merge():
    spark = SparkSession.builder.master("local[2]").appName("quarantine-guard-test").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    try:
        invalid = spark.createDataFrame(
            [("order-a", 1, "credit_card", 0, Decimal("10.00"))], SCHEMA
        )
        target = spark.createDataFrame(
            [("order-a", 1, "credit_card", 0, Decimal("99.00"))], SCHEMA
        )
        with pytest.raises(RuntimeError, match="different source values"):
            assert_existing_quarantine_rows_match(invalid, target, COLUMNS)
    finally:
        spark.stop()



def test_iceberg_provider_is_required():
    assert_iceberg_provider("iceberg")
    with pytest.raises(RuntimeError, match="provider is not Iceberg"):
        assert_iceberg_provider("parquet")


def test_other_dq_rules_accept_valid_non_installment_fields():
    spark = SparkSession.builder.master("local[2]").appName("quarantine-dq-guard-test").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    try:
        df = spark.createDataFrame(
            [("order-a", 1, "credit_card", 0, Decimal("10.00"))], SCHEMA
        )
        assert_other_dq_rules_valid(df, 0, 100000)
    finally:
        spark.stop()


def test_null_payment_type_blocks_installment_only_quarantine():
    spark = SparkSession.builder.master("local[2]").appName("quarantine-dq-guard-test").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    try:
        df = spark.createDataFrame(
            [("order-a", 1, None, 0, Decimal("10.00"))], SCHEMA
        )
        with pytest.raises(RuntimeError, match="payment_type null check"):
            assert_other_dq_rules_valid(df, 0, 100000)
    finally:
        spark.stop()


def test_null_payment_value_blocks_installment_only_quarantine():
    spark = SparkSession.builder.master("local[2]").appName("quarantine-dq-guard-test").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    try:
        df = spark.createDataFrame(
            [("order-a", 1, "credit_card", 0, None)], SCHEMA
        )
        with pytest.raises(RuntimeError, match="payment_value null check"):
            assert_other_dq_rules_valid(df, 0, 100000)
    finally:
        spark.stop()


def test_out_of_range_payment_value_blocks_installment_only_quarantine():
    spark = SparkSession.builder.master("local[2]").appName("quarantine-dq-guard-test").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    try:
        df = spark.createDataFrame(
            [("order-a", 1, "credit_card", 0, Decimal("100001.00"))], SCHEMA
        )
        with pytest.raises(RuntimeError, match="payment_value range check"):
            assert_other_dq_rules_valid(df, 0, 100000)
    finally:
        spark.stop()


def _base_failure_report_kwargs():
    return {
        "dataset": "olist_order_payments",
        "execution_id": "recovery-test-001",
        "source_path": "s3a://example/processed/silver/olist_order_payments",
        "target_table": "`glue_catalog`.`datapilot`.`olist_order_payments_quarantine`",
        "quarantine_path": "s3a://example/processed/quarantine/olist_order_payments",
        "source_count": 103886,
        "invalid_count": 2,
        "expected_count": 2,
        "persistent_change_started": False,
        "merge_completed": False,
        "verification_completed": False,
        "error": RuntimeError("simulated failure"),
    }


def test_failure_before_any_persistent_change_is_reported_as_no_write():
    report = build_apply_failure_report(**_base_failure_report_kwargs())
    assert report["status"] == "FAILED_BEFORE_WRITE"
    assert report["s3_write_performed"] is False
    assert report["source_modified"] is False


def test_failure_after_persistent_change_starts_is_reported_unknown():
    kwargs = _base_failure_report_kwargs()
    kwargs["persistent_change_started"] = True
    report = build_apply_failure_report(**kwargs)
    assert report["status"] == "WRITE_OUTCOME_UNKNOWN"
    assert report["s3_write_performed"] is None
    assert report["source_modified"] is False
    assert "Do not assume rollback" in report["recovery_instruction"]


def test_failure_after_merge_but_before_verification_reports_commit():
    kwargs = _base_failure_report_kwargs()
    kwargs["persistent_change_started"] = True
    kwargs["merge_completed"] = True
    report = build_apply_failure_report(**kwargs)
    assert report["status"] == "COMMIT_SUCCEEDED_VERIFICATION_FAILED"
    assert report["s3_write_performed"] is True
    assert report["verification_completed"] is False


def test_audit_failure_after_verification_reports_committed_verified():
    kwargs = _base_failure_report_kwargs()
    kwargs["persistent_change_started"] = True
    kwargs["merge_completed"] = True
    kwargs["verification_completed"] = True
    report = build_apply_failure_report(**kwargs)
    assert report["status"] == "COMMITTED_VERIFIED_AUDIT_FAILED"
    assert report["s3_write_performed"] is True
    assert report["verification_completed"] is True
