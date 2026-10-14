
import argparse
import json
import re
import os
from datetime import datetime, timezone
from pathlib import Path

import yaml
from pyspark.sql import SparkSession
from pyspark.sql.functions import col


SUPPORTED_DATASET = "olist_order_payments"
CHECK_COLUMN = "payment_installments"


def load_yaml(path):
    with open(path, "r", encoding="utf-8") as file:
        return yaml.safe_load(file)


def assert_s3_run_path_absent(spark, run_path):
    """Fail closed if the planned S3 quarantine run path already exists."""
    jvm = spark.sparkContext._jvm
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()

    path = jvm.org.apache.hadoop.fs.Path(run_path)
    filesystem = path.getFileSystem(hadoop_conf)

    if filesystem.exists(path):
        raise RuntimeError(
            f"Quarantine run path already exists; refusing reuse: {run_path}"
        )


def write_audit(report, audit_path):
    """Write an audit atomically without replacing an existing audit."""
    audit_path.parent.mkdir(parents=True, exist_ok=True)

    # Exclusive creation prevents silently reusing an existing temp file.
    temp_path = audit_path.with_name(
        f"{audit_path.name}.{os.getpid()}.tmp"
    )

    try:
        with temp_path.open("x", encoding="utf-8") as file:
            json.dump(report, file, indent=2)
            file.write("\n")
            file.flush()
            os.fsync(file.fileno())

        # Hard-link creation fails if audit_path already exists.
        # Both paths are in the same directory/filesystem.
        os.link(temp_path, audit_path)

    finally:
        temp_path.unlink(missing_ok=True)



def main():
    parser = argparse.ArgumentParser(
        description="DataPilot AI quarantine validator (dry-run only)"
    )
    parser.add_argument("--dataset", required=True)
    parser.add_argument("--execution-id", required=True)
    parser.add_argument(
        "--config-root", default="/opt/datapilot/config"
    )
    parser.add_argument("--expected-count", type=int)
    args = parser.parse_args()

    if args.dataset != SUPPORTED_DATASET:
        raise ValueError(
            f"Only {SUPPORTED_DATASET} is supported currently."
        )

    if not re.fullmatch(
        r"[A-Za-z0-9][A-Za-z0-9_-]{0,99}", args.execution_id
    ):
        raise ValueError(
            "Invalid execution ID. Use letters, numbers, hyphens "
            "or underscores; start with a letter or number."
        )

    if args.expected_count is not None and args.expected_count < 0:
        raise ValueError("--expected-count cannot be negative.")

    config_root = Path(args.config_root)
    variables = load_yaml(config_root / "variables.yaml")
    dq_config = load_yaml(
        config_root / "dq" / f"{args.dataset}.yaml"
    )

    storage = variables["storage"]
    bucket = storage["bucket"]
    layers = storage["layers"]
    region = variables.get("aws", {}).get("region", "us-east-1")

    if not layers.get("silver") or not layers.get("quarantine"):
        raise ValueError(
            "Configure storage.layers.silver and "
            "storage.layers.quarantine in variables.yaml."
        )

    checks = dq_config["checks"]
    range_checks = checks["range_checks"]
    rule = range_checks.get(CHECK_COLUMN)

    if not range_checks.get("enabled", False) or not rule:
        raise ValueError(
            f"No enabled range rule found for {CHECK_COLUMN}."
        )

    minimum = rule["min"]
    maximum = rule["max"]

    if minimum > maximum:
        raise ValueError("DQ minimum cannot exceed maximum.")

    source_path = (
        f"s3a://{bucket}/{layers['silver']}/{args.dataset}"
    )
    quarantine_base = (
        f"s3a://{bucket}/{layers['quarantine']}/{args.dataset}"
    )
    planned_run_path = (
        f"{quarantine_base}/runs/{args.execution_id}"
    )

    audit_path = (
        Path("/opt/spark-apps/results/quarantine")
        / f"{args.dataset}_{args.execution_id}_dry_run.json"
    )
    if audit_path.exists():
        raise RuntimeError(
        f"Execution ID already has a dry-run audit; "
        f"use a new execution ID: {args.execution_id}"
        )

    spark = (
        SparkSession.builder
        .appName(f"DataPilot-Quarantine-DryRun-{args.dataset}")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider",
        )
        .config("spark.hadoop.fs.s3a.endpoint.region", region)
        .getOrCreate()
    )
    print("Quarantine run-path collision check: PASS")

    try:
        spark.sparkContext.setLogLevel("WARN")
        assert_s3_run_path_absent(spark, planned_run_path)

        print("Mode: DRY_RUN — no S3 writes")
        print(f"Dataset: {args.dataset}")
        print(f"Execution ID: {args.execution_id}")
        print(f"Silver source: {source_path}")
        print(f"Planned quarantine run: {planned_run_path}")
        print(
            f"DQ rule: {minimum} <= {CHECK_COLUMN} <= {maximum}"
        )

        df = spark.read.parquet(source_path)

        if CHECK_COLUMN not in df.columns:
            raise ValueError(
                f"Column {CHECK_COLUMN} not found in Silver data."
            )

        # Null values are also invalid for a numeric range rule.
        invalid_df = df.filter(
            col(CHECK_COLUMN).isNull()
            | (col(CHECK_COLUMN) < minimum)
            | (col(CHECK_COLUMN) > maximum)
        )

        source_count = df.count()
        invalid_count = invalid_df.count()

        print(f"Silver row count: {source_count}")
        print(f"Invalid row count: {invalid_count}")

        if invalid_count == 0:
            raise RuntimeError(
                "No invalid records found; no quarantine is planned."
            )

        if (
            args.expected_count is not None
            and invalid_count != args.expected_count
        ):
            raise RuntimeError(
                f"Expected {args.expected_count} invalid rows, "
                f"but found {invalid_count}. No S3 writes performed."
            )

        report = {
            "dataset": args.dataset,
            "mode": "DRY_RUN",
            "status": "VALIDATED",
            "execution_id": args.execution_id,
            "source_path": source_path,
            "quarantine_base_path": quarantine_base,
            "planned_quarantine_run_path": planned_run_path,
            "rule": {
                "column": CHECK_COLUMN,
                "minimum": minimum,
                "maximum": maximum,
                "nulls_invalid": True,
            },
            "source_count": source_count,
            "invalid_count": invalid_count,
            "expected_count": args.expected_count,
            "s3_write_performed": False,
            "source_modified": False,
            "checked_at": datetime.now(timezone.utc).isoformat(),
        }

        write_audit(report, audit_path)

        print(f"Dry-run audit: {audit_path}")
        print(json.dumps(report, indent=2))

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
