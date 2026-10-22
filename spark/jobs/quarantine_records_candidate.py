"""
DataPilot AI quarantine writer for Olist payment records.

Safety:
- DRY_RUN is the default and performs no table/S3 writes.
- APPLY requires both --mode APPLY and --confirm-write.
- This job writes only to the quarantine Iceberg table. It does not modify Silver or Gold.
- Gold cleanup/ingestion remains a separate, later step.
"""
import argparse
import fcntl
import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path

import yaml
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    current_timestamp,
    lit,
)


SUPPORTED_DATASET = "olist_order_payments"
CHECK_COLUMN = "payment_installments"
BUSINESS_KEYS = ["order_id", "payment_sequential"]
METADATA_TYPES = {
    "execution_id": "string",
    "quarantine_reason": "string",
    "rule_name": "string",
    "source_layer": "string",
    "quarantined_at": "timestamp",
}


def parse_args():
    parser = argparse.ArgumentParser(
        description="DataPilot AI Iceberg payment quarantine writer"
    )
    parser.add_argument("--dataset", required=True)
    parser.add_argument("--execution-id", required=True)
    parser.add_argument("--config-root", default="/opt/datapilot/config")
    parser.add_argument("--expected-count", type=int)
    parser.add_argument(
        "--mode",
        choices=("DRY_RUN", "APPLY"),
        default="DRY_RUN",
        help="DRY_RUN is the safe default; APPLY writes to Iceberg.",
    )
    parser.add_argument(
        "--confirm-write",
        action="store_true",
        help="Required together with --mode APPLY to enable persistent writes.",
    )
    parser.add_argument(
        "--approval-file",
        default="/opt/datapilot/results/approvals/olist_order_payments.json",
    )
    parser.add_argument(
        "--plan-file",
        default="/opt/datapilot/results/remediation/olist_order_payments.json",
    )
    return parser.parse_args()


def load_yaml(path):
    with open(path, "r", encoding="utf-8") as file:
        return yaml.safe_load(file)


def validate_identifier(value, label):
    if not isinstance(value, str) or not re.fullmatch(
        r"[A-Za-z_][A-Za-z0-9_]*", value
    ):
        raise ValueError(f"Invalid {label}: {value!r}")
    return value


def quote_table(catalog, database, table):
    return ".".join(
        f"`{validate_identifier(part, 'table identifier')}`"
        for part in (catalog, database, table)
    )


def write_audit(report, audit_path):
    """Create an audit file without replacing an existing one."""
    audit_path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = audit_path.with_name(f"{audit_path.name}.{os.getpid()}.tmp")
    try:
        with temp_path.open("x", encoding="utf-8") as file:
            json.dump(report, file, indent=2)
            file.write("\n")
            file.flush()
            os.fsync(file.fileno())
        os.link(temp_path, audit_path)
    finally:
        temp_path.unlink(missing_ok=True)


def build_apply_failure_report(
    *,
    dataset,
    execution_id,
    source_path,
    target_table,
    quarantine_path,
    source_count,
    invalid_count,
    expected_count,
    persistent_change_started,
    merge_completed,
    verification_completed,
    error,
):
    """Classify APPLY failures without falsely claiming that no write occurred."""
    if not persistent_change_started:
        status = "FAILED_BEFORE_WRITE"
        s3_write_performed = False
    elif merge_completed and verification_completed:
        status = "COMMITTED_VERIFIED_AUDIT_FAILED"
        s3_write_performed = True
    elif merge_completed:
        status = "COMMIT_SUCCEEDED_VERIFICATION_FAILED"
        s3_write_performed = True
    else:
        # Database/table creation or MERGE may have reached the catalog/storage.
        status = "WRITE_OUTCOME_UNKNOWN"
        s3_write_performed = None

    return {
        "dataset": dataset,
        "mode": "APPLY",
        "status": status,
        "execution_id": execution_id,
        "source_path": source_path,
        "target_table": target_table,
        "quarantine_path": quarantine_path,
        "source_count": source_count,
        "invalid_count": invalid_count,
        "expected_count": expected_count,
        "s3_write_performed": s3_write_performed,
        "source_modified": False,
        "persistent_change_started": persistent_change_started,
        "merge_completed": merge_completed,
        "verification_completed": verification_completed,
        "error_type": type(error).__name__,
        "error_message": str(error)[:1000],
        "checked_at": datetime.now(timezone.utc).isoformat(),
        "recovery_instruction": (
            "Do not assume rollback. Inspect the Iceberg table and audit state; "
            "retry only with a new execution ID after reconciling existing business keys."
        ),
    }


def create_spark(region, warehouse):
    return (
        SparkSession.builder
        .appName("DataPilotAI-Payment-Quarantine")
        .config(
            "spark.sql.catalog.glue_catalog",
            "org.apache.iceberg.spark.SparkCatalog",
        )
        .config(
            "spark.sql.catalog.glue_catalog.catalog-impl",
            "org.apache.iceberg.aws.glue.GlueCatalog",
        )
        .config("spark.sql.catalog.glue_catalog.warehouse", warehouse)
        .config(
            "spark.sql.catalog.glue_catalog.io-impl",
            "org.apache.iceberg.aws.s3.S3FileIO",
        )
        .config("spark.sql.catalog.glue_catalog.client.region", region)
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def assert_source_keys_valid(df):
    required = set(BUSINESS_KEYS) | {
        "payment_type",
        "payment_installments",
        "payment_value",
    }
    missing = sorted(required - set(df.columns))
    if missing:
        raise RuntimeError(f"Silver source is missing required columns: {missing}")

    if df.filter(
        col("order_id").isNull() | col("payment_sequential").isNull()
    ).limit(1).count():
        raise RuntimeError("Silver source contains NULL business keys.")

    duplicate_keys = (
        df.groupBy(*BUSINESS_KEYS)
        .count()
        .filter(col("count") > 1)
        .limit(1)
        .count()
    )
    if duplicate_keys:
        raise RuntimeError("Silver source contains duplicate business keys.")


def normalize_s3_location(value):
    """Normalize S3 URI schemes for comparing Glue/Iceberg table locations."""
    if not isinstance(value, str) or not value.strip():
        raise RuntimeError(f"Invalid Iceberg table location: {value!r}")
    value = value.strip().rstrip("/")
    if value.startswith("s3a://"):
        value = "s3://" + value[len("s3a://"):]
    return value


def get_table_detail(spark, target, detail_name):
    """Read one registered table detail without modifying the table."""
    rows = spark.sql(f"DESCRIBE TABLE EXTENDED {target}").collect()
    wanted = detail_name.strip().lower()
    for row in rows:
        if str(row[0]).strip().lower() == wanted:
            return str(row[1]).strip()
    raise RuntimeError(
        f"Could not determine registered {detail_name} for {target}."
    )


def get_table_location(spark, target):
    """Read the catalog's registered table location without modifying it."""
    return get_table_detail(spark, target, "Location")


def assert_iceberg_provider(provider):
    if not isinstance(provider, str) or provider.strip().lower() != "iceberg":
        raise RuntimeError(
            "Quarantine target provider is not Iceberg; refusing to write. "
            f"Registered provider: {provider!r}."
        )


def assert_other_dq_rules_valid(df, payment_value_min, payment_value_max):
    """Fail closed if non-installment DQ rules already fail in the source."""
    invalid_payment_type = df.filter(col("payment_type").isNull()).limit(1).count()
    if invalid_payment_type:
        raise RuntimeError(
            "Silver source violates the payment_type null check; "
            "refusing installment-only quarantine."
        )

    null_payment_value = df.filter(col("payment_value").isNull()).limit(1).count()
    if null_payment_value:
        raise RuntimeError(
            "Silver source violates the payment_value null check; "
            "refusing installment-only quarantine."
        )

    invalid_payment_value = df.filter(
        (col("payment_value") < payment_value_min)
        | (col("payment_value") > payment_value_max)
    ).limit(1).count()
    if invalid_payment_value:
        raise RuntimeError(
            "Silver source violates the payment_value range check; "
            "refusing installment-only quarantine."
        )


def assert_expected_table_location(actual_location, expected_location):
    actual = normalize_s3_location(actual_location)
    expected = normalize_s3_location(expected_location)
    if actual != expected:
        raise RuntimeError(
            "Quarantine table location mismatch; refusing to write. "
            f"Expected {expected!r}, found {actual!r}."
        )


def assert_existing_quarantine_rows_match(invalid, target_df, original_columns):
    """Fail before MERGE if a quarantined business key has different source values."""
    expected = invalid.select(*original_columns).alias("expected")
    actual = target_df.select(*original_columns).alias("actual")
    join_condition = (
        (col("expected.order_id") == col("actual.order_id"))
        & (col("expected.payment_sequential") == col("actual.payment_sequential"))
    )
    existing = expected.join(actual, join_condition, "inner")
    mismatch_conditions = [
        ~col(f"expected.`{name}`").eqNullSafe(col(f"actual.`{name}`"))
        for name in original_columns
    ]
    if mismatch_conditions:
        mismatch = mismatch_conditions[0]
        for condition in mismatch_conditions[1:]:
            mismatch = mismatch | condition
        if existing.filter(mismatch).limit(1).count():
            raise RuntimeError(
                "Existing quarantine record has the same business key but "
                "different source values; refusing to merge any records."
            )


def validate_target_schema(spark, target, source_columns):
    target_df = spark.table(target)
    expected_columns = {field.name for field in source_columns} | set(METADATA_TYPES)
    actual_columns = set(target_df.columns)
    if actual_columns != expected_columns:
        raise RuntimeError(
            "Quarantine table schema mismatch. "
            f"Expected columns {sorted(expected_columns)}, "
            f"found {sorted(actual_columns)}."
        )

    source_schema = {field.name: field.dataType.simpleString()
                     for field in source_columns}
    target_schema = {field.name: field.dataType.simpleString()
                     for field in target_df.schema.fields}
    for name, expected_type in source_schema.items():
        if target_schema.get(name) != expected_type:
            raise RuntimeError(
                f"Quarantine column type mismatch for {name}: "
                f"expected {expected_type}, found {target_schema.get(name)}."
            )
    for name, expected_type in METADATA_TYPES.items():
        if target_schema.get(name) != expected_type:
            raise RuntimeError(
                f"Quarantine metadata type mismatch for {name}: "
                f"expected {expected_type}, found {target_schema.get(name)}."
            )


def main():
    args = parse_args()

    if args.dataset != SUPPORTED_DATASET:
        raise ValueError(f"Only {SUPPORTED_DATASET} is supported.")
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,99}", args.execution_id):
        raise ValueError("Invalid execution ID.")
    if args.expected_count is not None and args.expected_count < 0:
        raise ValueError("--expected-count cannot be negative.")
    if args.mode == "APPLY" and not args.confirm_write:
        raise ValueError(
            "APPLY mode requires --confirm-write. No writes were performed."
        )
    if args.mode == "DRY_RUN" and args.confirm_write:
        raise ValueError("--confirm-write is only valid with --mode APPLY.")

    if args.mode == "APPLY":
        approval = load_yaml(args.approval_file) if args.approval_file.endswith((".yaml", ".yml")) else json.loads(Path(args.approval_file).read_text(encoding="utf-8"))
        plan = load_yaml(args.plan_file) if args.plan_file.endswith((".yaml", ".yml")) else json.loads(Path(args.plan_file).read_text(encoding="utf-8"))
        if (
            approval.get("dataset") != args.dataset
            or approval.get("action") != "QUARANTINE_RECORDS"
            or approval.get("status") != "APPROVED"
            or approval.get("execution_authorized") is not True
        ):
            raise RuntimeError("Current approval is missing, mismatched, or not authorized; no writes performed.")
        if (
            plan.get("dataset") != args.dataset
            or plan.get("action") != "QUARANTINE_RECORDS"
            or plan.get("requires_approval") is not True
            or plan.get("status") != "PENDING_APPROVAL"
            or plan.get("execution_mode") != "DRY_RUN"
        ):
            raise RuntimeError("Remediation plan is missing or does not match the approved dry-run plan; no writes performed.")

    config_root = Path(args.config_root)
    variables = load_yaml(config_root / "variables.yaml")
    dq_config = load_yaml(config_root / "dq" / f"{args.dataset}.yaml")
    quarantine_config = load_yaml(
        config_root / "quarantine" / f"{args.dataset}.yaml"
    )

    storage = variables["storage"]
    bucket = storage["bucket"]
    layers = storage["layers"]
    region = variables.get("aws", {}).get("region", "us-east-1")

    quarantine_settings = quarantine_config["quarantine"]
    if quarantine_config.get("dataset") != args.dataset:
        raise RuntimeError("Quarantine config dataset mismatch.")
    if quarantine_settings.get("enabled") is not True:
        raise RuntimeError("Quarantine is disabled in configuration.")
    if quarantine_settings.get("catalog") != "glue_catalog":
        raise RuntimeError("Unexpected quarantine catalog.")
    if quarantine_settings.get("database") != "datapilot":
        raise RuntimeError("Unexpected quarantine database.")
    if quarantine_settings.get("table") != "olist_order_payments_quarantine":
        raise RuntimeError("Unexpected quarantine table.")
    if quarantine_settings.get("format") != "iceberg":
        raise RuntimeError("Only Iceberg quarantine is supported.")
    if quarantine_settings.get("keys") != BUSINESS_KEYS:
        raise RuntimeError("Unexpected quarantine business keys.")

    checks = dq_config.get("checks", {})
    range_checks = checks.get("range_checks", {})
    rule = range_checks.get(CHECK_COLUMN)
    if not rule or range_checks.get("enabled") is not True:
        raise RuntimeError("Enabled payment_installments range rule is required.")
    minimum, maximum = rule["min"], rule["max"]
    if minimum != 1 or maximum != 100:
        raise RuntimeError("Expected approved installment rule range 1–100.")

    payment_value_rule = range_checks.get("payment_value")
    if not payment_value_rule:
        raise RuntimeError("Enabled payment_value range rule is required.")
    if payment_value_rule.get("min") != 0 or payment_value_rule.get("max") != 100000:
        raise RuntimeError("Expected approved payment_value range 0–100000.")

    null_checks = checks.get("null_checks", {})
    required_null_checks = {"order_id", "payment_type", "payment_value"}
    if (
        null_checks.get("enabled") is not True
        or not required_null_checks.issubset(set(null_checks.get("columns", [])))
    ):
        raise RuntimeError("Expected enabled null checks for order_id, payment_type, and payment_value.")

    source_path = f"s3a://{bucket}/{layers['silver']}/{args.dataset}"
    quarantine_path = (
        f"s3a://{bucket}/{layers['quarantine']}/{args.dataset}"
    )
    warehouse = f"s3a://{bucket}/{layers['gold']}"
    target = quote_table(
        quarantine_settings["catalog"],
        quarantine_settings["database"],
        quarantine_settings["table"],
    )

    audit_suffix = "apply" if args.mode == "APPLY" else "dry_run"
    audit_path = (
        Path("/opt/spark-apps/results/quarantine")
        / f"{args.dataset}_{args.execution_id}_{audit_suffix}.json"
    )
    if audit_path.exists():
        raise RuntimeError(f"Audit already exists: {audit_path}")

    # Serialize APPLY attempts sharing this Docker-mounted project directory.
    # This is a single-host lock, not a distributed lock across separate hosts.
    lock_handle = None
    if args.mode == "APPLY":
        lock_path = audit_path.parent / f".{args.dataset}_apply.lock"
        lock_path.parent.mkdir(parents=True, exist_ok=True)
        lock_handle = lock_path.open("a+", encoding="utf-8")
        try:
            fcntl.flock(lock_handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            lock_handle.close()
            raise RuntimeError(
                "Another APPLY run holds the local quarantine writer lock; "
                "no new write was started."
            ) from exc

    persistent_change_started = False
    merge_completed = False
    verification_completed = False
    source_count = None
    invalid_count = None

    spark = create_spark(region, warehouse)
    try:
        spark.sparkContext.setLogLevel("WARN")
        print(f"Mode: {args.mode}")
        print(f"Dataset: {args.dataset}")
        print(f"Silver source: {source_path}")
        print(f"Quarantine table: {target}")
        print(f"Quarantine location if created: {quarantine_path}")

        source = spark.read.parquet(source_path)
        assert_source_keys_valid(source)
        assert_other_dq_rules_valid(
            source,
            payment_value_min=payment_value_rule["min"],
            payment_value_max=payment_value_rule["max"],
        )

        invalid_condition = (
            col(CHECK_COLUMN).isNull()
            | (col(CHECK_COLUMN) < minimum)
            | (col(CHECK_COLUMN) > maximum)
        )
        invalid = source.filter(invalid_condition).persist()
        try:
            source_count = source.count()
            invalid_count = invalid.count()
            print(f"Silver rows: {source_count}")
            print(f"Invalid rows: {invalid_count}")

            if invalid_count == 0:
                raise RuntimeError("No invalid records found; refusing to proceed.")
            if args.expected_count is not None and invalid_count != args.expected_count:
                raise RuntimeError(
                    f"Expected {args.expected_count} invalid rows, found {invalid_count}."
                )

            # Ensure each expected source row is identifiable and metadata is explicit.
            reason = (
                f"{CHECK_COLUMN} must be between {minimum} and {maximum}, inclusive"
            )
            incoming = (
                invalid
                .withColumn("execution_id", lit(args.execution_id))
                .withColumn("quarantine_reason", lit(reason))
                .withColumn("rule_name", lit("range_check:payment_installments"))
                .withColumn("source_layer", lit("silver"))
                .withColumn("quarantined_at", current_timestamp())
            )

            if args.mode == "DRY_RUN":
                report = {
                    "dataset": args.dataset,
                    "mode": "DRY_RUN",
                    "status": "VALIDATED",
                    "execution_id": args.execution_id,
                    "source_path": source_path,
                    "target_table": target,
                    "quarantine_path_if_created": quarantine_path,
                    "source_count": source_count,
                    "invalid_count": invalid_count,
                    "expected_count": args.expected_count,
                    "s3_write_performed": False,
                    "source_modified": False,
                    "checked_at": datetime.now(timezone.utc).isoformat(),
                }
                write_audit(report, audit_path)
                print(json.dumps(report, indent=2))
                return

            # APPLY begins here. The source is never written to. From this point,
            # catalog/table metadata or Iceberg data may be changed, so failures
            # must be reported conservatively.
            persistent_change_started = True
            spark.sql(f"CREATE DATABASE IF NOT EXISTS `glue_catalog`.`datapilot`")

            if not spark.catalog.tableExists(target):
                # Create an empty Iceberg table with explicit quarantine location.
                schema_sql = []
                for field in incoming.schema.fields:
                    name = validate_identifier(field.name, "column name")
                    schema_sql.append(
                        f"`{name}` {field.dataType.simpleString()}"
                    )
                spark.sql(
                    f"CREATE TABLE {target} ({', '.join(schema_sql)}) "
                    f"USING iceberg LOCATION '{quarantine_path}' "
                    "TBLPROPERTIES ('format-version'='2')"
                )

            # Refuse to write if a pre-existing Glue table points at an unexpected
            # S3 prefix, even when its schema happens to match.
            assert_iceberg_provider(get_table_detail(spark, target, "Provider"))
            assert_expected_table_location(
                get_table_location(spark, target), quarantine_path
            )
            validate_target_schema(spark, target, incoming.schema.fields)

            target_df = spark.table(target)
            if target_df.filter(
                col("order_id").isNull() | col("payment_sequential").isNull()
            ).limit(1).count():
                raise RuntimeError("Existing quarantine target has NULL business keys.")

            duplicate_target = (
                target_df.groupBy(*BUSINESS_KEYS)
                .count()
                .filter(col("count") > 1)
                .limit(1)
                .count()
            )
            if duplicate_target:
                raise RuntimeError("Existing quarantine target has duplicate business keys.")

            # Check key collisions before MERGE. Otherwise a conflicting existing
            # row could cause a post-commit verification failure after other rows
            # in the same batch have already been inserted.
            assert_existing_quarantine_rows_match(
                invalid, target_df, source.columns
            )

            incoming.createOrReplaceTempView("payment_quarantine_incoming")
            merge_condition = (
                "target.`order_id` = source.`order_id` AND "
                "target.`payment_sequential` = source.`payment_sequential`"
            )
            spark.sql(
                f"""
                MERGE INTO {target} AS target
                USING payment_quarantine_incoming AS source
                ON {merge_condition}
                WHEN NOT MATCHED THEN INSERT *
                """
            )
            merge_completed = True

            # Verify each invalid key and all original source values after commit.
            committed = spark.table(target)
            original_columns = source.columns
            expected = invalid.select(*original_columns).alias("expected")
            actual = committed.select(*original_columns).alias("actual")
            join_condition = (
                (col("expected.order_id") == col("actual.order_id"))
                & (
                    col("expected.payment_sequential")
                    == col("actual.payment_sequential")
                )
            )
            joined = expected.join(actual, join_condition, "left")
            missing = joined.filter(col("actual.order_id").isNull()).limit(1).count()
            if missing:
                raise RuntimeError("Quarantine verification failed: source keys missing.")

            mismatches = []
            for name in original_columns:
                mismatches.append(
                    ~col(f"expected.`{name}`").eqNullSafe(col(f"actual.`{name}`"))
                )
            mismatch_condition = mismatches[0]
            for condition in mismatches[1:]:
                mismatch_condition = mismatch_condition | condition
            if joined.filter(mismatch_condition).limit(1).count():
                raise RuntimeError(
                    "Quarantine verification failed: original source values differ."
                )

            duplicate_expected = (
                committed.join(
                    invalid.select(*BUSINESS_KEYS).dropDuplicates(),
                    BUSINESS_KEYS,
                    "inner",
                )
                .groupBy(*BUSINESS_KEYS)
                .count()
                .filter(col("count") != 1)
                .limit(1)
                .count()
            )
            if duplicate_expected:
                raise RuntimeError(
                    "Quarantine verification failed: expected keys are not unique."
                )
            verification_completed = True

            report = {
                "dataset": args.dataset,
                "mode": "APPLY",
                "status": "COMMITTED_AND_VERIFIED",
                "execution_id": args.execution_id,
                "source_path": source_path,
                "target_table": target,
                "quarantine_path": quarantine_path,
                "source_count": source_count,
                "invalid_count": invalid_count,
                "expected_count": args.expected_count,
                "s3_write_performed": True,
                "source_modified": False,
                "verified_original_columns": original_columns,
                "checked_at": datetime.now(timezone.utc).isoformat(),
            }
            write_audit(report, audit_path)
            print(json.dumps(report, indent=2))
        except Exception as apply_error:
            if args.mode == "APPLY" and not audit_path.exists():
                failure_report = build_apply_failure_report(
                    dataset=args.dataset,
                    execution_id=args.execution_id,
                    source_path=source_path,
                    target_table=target,
                    quarantine_path=quarantine_path,
                    source_count=source_count,
                    invalid_count=invalid_count,
                    expected_count=args.expected_count,
                    persistent_change_started=persistent_change_started,
                    merge_completed=merge_completed,
                    verification_completed=verification_completed,
                    error=apply_error,
                )
                try:
                    write_audit(failure_report, audit_path)
                    print(json.dumps(failure_report, indent=2))
                except Exception as audit_error:
                    print(
                        "CRITICAL: remediation failed and failure audit could not "
                        f"be persisted: {type(audit_error).__name__}: {audit_error}. "
                        f"Original failure: {type(apply_error).__name__}: {apply_error}. "
                        "Treat write outcome as unknown if persistent changes had started."
                    )
            raise
        finally:
            invalid.unpersist()
    finally:
        try:
            spark.stop()
        finally:
            if lock_handle is not None:
                try:
                    fcntl.flock(lock_handle.fileno(), fcntl.LOCK_UN)
                finally:
                    lock_handle.close()


if __name__ == "__main__":
    main()
