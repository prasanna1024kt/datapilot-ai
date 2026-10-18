import argparse
import json
import re
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count

from framework.config_loader import ConfigLoader
from framework.path_builder import PathBuilder

CONFIG_ROOT = "/opt/datapilot/config"
DATASET = "olist_order_payments"

SOURCE_COLUMNS = [
    "order_id",
    "payment_sequential",
    "payment_type",
    "payment_installments",
    "payment_value",
]

BUSINESS_KEYS = ["order_id", "payment_sequential"]


def find_installment_range(value):
    """Find the configured payment_installments minimum and maximum."""
    if isinstance(value, dict):
        rule = value.get("payment_installments")

        if isinstance(rule, dict):
            minimum = rule.get("minimum", rule.get("min"))
            maximum = rule.get("maximum", rule.get("max"))

            if minimum is not None and maximum is not None:
                return minimum, maximum

        for nested in value.values():
            result = find_installment_range(nested)
            if result is not None:
                return result

    elif isinstance(value, list):
        for nested in value:
            result = find_installment_range(nested)
            if result is not None:
                return result

    return None


def main():
    parser = argparse.ArgumentParser(
        description="Plan quarantine remediation without writing to S3."
    )
    parser.add_argument("--execution-id", required=True)
    parser.add_argument("--expected-count", type=int, default=2)
    args = parser.parse_args()

    if not re.fullmatch(
        r"[A-Za-z0-9][A-Za-z0-9_-]{0,99}", args.execution_id
    ):
        raise ValueError("Invalid execution ID.")

    if args.expected_count < 0:
        raise ValueError("Expected count cannot be negative.")

    output_path = Path(
        "/opt/spark-apps/results/quarantine"
    ) / f"{DATASET}_{args.execution_id}_merge_plan.json"

    # Fail before starting Spark if this run's audit already exists.
    if output_path.exists():
        raise FileExistsError(
            f"Audit already exists; choose a new execution ID: {output_path}"
        )

    loader = ConfigLoader(CONFIG_ROOT)
    variables = loader.load_variables()
    dq_config = loader.load_layer_config("dq", DATASET)
    quarantine_config = loader.load_layer_config("quarantine", DATASET)

    quarantine = quarantine_config["quarantine"]
    if not quarantine["enabled"]:
        raise ValueError("Quarantine is disabled.")

    configured_keys = quarantine["keys"]
    if configured_keys != BUSINESS_KEYS:
        raise ValueError(
            f"Unexpected business keys: {configured_keys}"
        )

    rule_range = find_installment_range(dq_config)
    if rule_range is None:
        raise ValueError(
            "Could not find payment_installments min/max in DQ config."
        )

    minimum, maximum = rule_range

    path_builder = PathBuilder(variables)
    source_path = path_builder.build_layer_path("silver", DATASET)

    spark = (
        SparkSession.builder
        .appName("DataPilotAI-QuarantineMergePlan")
        .getOrCreate()
    )

    try:
        print(f"Reading Silver: {source_path}")
        df = spark.read.parquet(source_path)

        missing = sorted(set(SOURCE_COLUMNS) - set(df.columns))
        if missing:
            raise ValueError(f"Missing required columns: {missing}")

        source_count = df.count()

        # Reject null business keys.
        null_key_condition = (
            col("order_id").isNull()
            | col("payment_sequential").isNull()
        )
        null_key_count = df.filter(null_key_condition).count()

        if null_key_count:
            raise ValueError(
                f"Found {null_key_count} rows with null business keys."
            )

        # Reject duplicate business keys in the source.
        duplicate_keys = (
            df.groupBy(*BUSINESS_KEYS)
            .agg(count("*").alias("key_count"))
            .filter(col("key_count") > 1)
        )
        duplicate_key_count = duplicate_keys.count()

        if duplicate_key_count:
            raise ValueError(
                f"Found {duplicate_key_count} duplicate business keys."
            )

        installment = col("payment_installments")
        invalid_condition = (
            installment.isNull()
            | (installment < minimum)
            | (installment > maximum)
        )

        invalid_df = df.filter(invalid_condition).select(*SOURCE_COLUMNS)
        invalid_count = invalid_df.count()

        if invalid_count != args.expected_count:
            raise ValueError(
                f"Expected {args.expected_count} invalid rows, "
                f"found {invalid_count}."
            )

        planned_at = datetime.now(timezone.utc).isoformat()

        records = []
        for row in invalid_df.collect():
            record = row.asDict(recursive=True)
            record.update({
                "execution_id": args.execution_id,
                "quarantine_reason": (
                    f"payment_installments must be between "
                    f"{minimum} and {maximum}, inclusive"
                ),
                "rule_name": "payment_installments_range",
                "source_layer": "silver",
                "planned_at": planned_at,
            })
            records.append(record)

        report = {
            "dataset": DATASET,
            "mode": "DRY_RUN",
            "status": "VALIDATED",
            "execution_id": args.execution_id,
            "source_path": source_path,
            "target_table": (
                f"{quarantine['catalog']}."
                f"{quarantine['database']}."
                f"{quarantine['table']}"
            ),
            "source_count": source_count,
            "invalid_count": invalid_count,
            "expected_count": args.expected_count,
            "business_keys": BUSINESS_KEYS,
            "rule": {
                "column": "payment_installments",
                "minimum": minimum,
                "maximum": maximum,
                "nulls_invalid": True,
            },
            "source_duplicate_keys": duplicate_key_count,
            "null_business_key_rows": null_key_count,
            "target_comparison": "NOT_PERFORMED",
            "planned_action": (
                "UPSERT_BY_BUSINESS_KEY"
                if records else "NO_OP"
            ),
            "proposed_records": records,
            "iceberg_table_created": False,
            "iceberg_merge_executed": False,
            "s3_write_performed": False,
            "source_modified": False,
        }

        # Exclusive creation prevents overwriting an existing audit.
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with output_path.open("x", encoding="utf-8") as file:
            json.dump(report, file, indent=2, default=str)
            file.write("\n")

        print("\n=== DRY-RUN MERGE PLAN ===")
        print(json.dumps(report, indent=2, default=str))
        print(f"\nLocal audit written: {output_path}")
        print("No Iceberg table created. No S3 writes performed.")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
