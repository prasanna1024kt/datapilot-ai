import argparse
import json
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when
from framework.config_loader import ConfigLoader
from framework.path_builder import PathBuilder


def parse_args():

    parser = argparse.ArgumentParser(
        description="DataPilot AI Approved Data Remediation"
    )

    parser.add_argument("--dataset", required=True)

    parser.add_argument("--approval-root", default="/opt/datapilot/results/approvals")

    parser.add_argument("--config-root", default="/opt/datapilot/config")

    return parser.parse_args()


def load_approval(approval_root, dataset):

    path = (Path(approval_root)
        / f"{dataset}.json"
    )

    if not path.exists():raise FileNotFoundError(f"Approval not found: {path}")

    with path.open("r", encoding="utf-8") as file:

        approval = json.load(file)

    return approval


def validate_approval(approval, dataset):

    if approval.get("dataset") != dataset:

        raise ValueError(
            "Approval dataset does not match "
            f"requested dataset: {dataset}"
        )

    if approval.get("status") != "APPROVED":

        raise RuntimeError("Remediation is not approved.")

    if approval.get("execution_authorized") is not True:

        raise RuntimeError("Remediation execution is not authorized.")

    if approval.get("action") != "SOURCE_CORRECTION":

        raise ValueError("Only SOURCE_CORRECTION is supported.")

    corrections = approval.get("corrections")

    if not corrections:

        raise ValueError("No corrections found in approval.")

    return corrections


def build_spark():

    return (
        SparkSession.builder
        .appName("DataPilot-Remediation")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider",
        )
        .config(
            "spark.hadoop.fs.s3a.endpoint.region",
            "us-east-1",
        )
        .getOrCreate()
    )


def validate_correction_columns(df, corrections):

    available_columns = set(df.columns)

    for correction in corrections:

        column = correction["column"]

        if column not in available_columns:

            raise ValueError(
                f"Correction column does not exist: {column}"
            )

        for key in correction["keys"]:

            if key not in available_columns:

                raise ValueError(
                    f"Correction key does not exist: {key}"
                )


def apply_corrections(df, corrections):

    result = df

    for correction in corrections:

        keys = correction["keys"]
        column = correction["column"]
        new_value = correction["new_value"]

        conditions = None

        for key, value in keys.items():

            condition = (
                col(key) == lit(value)
            )

            if conditions is None:

                conditions = condition

            else:

                conditions = (
                    conditions & condition
                )

        result = result.withColumn(
            column, when(
                conditions,
                lit(new_value),
            ).otherwise(
                col(column)
            )
        )

    return result


def main():

    args = parse_args()

    spark = build_spark()

    try:

        # --------------------------------------------------
        # Load approval
        # --------------------------------------------------

        approval = load_approval(
            args.approval_root,
            args.dataset,
        )

        corrections = validate_approval(
            approval,
            args.dataset,
        )

        print("")
        print("========================================")
        print("DATAPILOT REMEDIATION")
        print("========================================")
        print(f"Dataset       : {args.dataset}")
        print(f"Action        : {approval['action']}")
        print(f"Approved by   : {approval['approved_by']}")
        print(
            f"Corrections   : {len(corrections)}"
        )

        # --------------------------------------------------
        # Load configuration
        # --------------------------------------------------

        config_loader = ConfigLoader(args.config_root)

        variables = (config_loader.load_variables())

        path_builder = PathBuilder(variables)

        # --------------------------------------------------
        # Bronze source
        # --------------------------------------------------

        bronze_path = (
            path_builder.build_layer_path(
                "bronze",
                args.dataset,
            )
        )

        print(f"Bronze source : {bronze_path}")

        # --------------------------------------------------
        # Read Bronze
        # --------------------------------------------------

        df = spark.read.parquet(bronze_path)

        original_count = df.count()

        print(f"Original rows: {original_count}")

        # --------------------------------------------------
        # Validate correction definitions
        # --------------------------------------------------

        validate_correction_columns(df, corrections)

        # --------------------------------------------------
        # Apply approved corrections
        # --------------------------------------------------

        corrected_df = apply_corrections(df, corrections)

        # --------------------------------------------------
        # Validate row count
        # --------------------------------------------------

        corrected_count = (corrected_df.count())

        if corrected_count != original_count:

            raise RuntimeError(
                "Remediation changed row count. "
                f"Before={original_count}, "
                f"After={corrected_count}"
            )

        # --------------------------------------------------
        # Validate affected records
        # --------------------------------------------------

        for correction in corrections:

            keys = correction["keys"]
            column = correction["column"]
            expected_value = correction["new_value"]

            conditions = None

            for key, value in keys.items():

                condition = (col(key) == lit(value))

                if conditions is None:
                    conditions = condition
                else:
                    conditions = (
                        conditions & condition
                    )

            validation_count = (
                corrected_df
                .filter(conditions)
                .filter(
                    col(column) == lit(expected_value)
                )
                .count()
            )

            if validation_count != 1:

                raise RuntimeError(
                    "Correction validation failed for "
                    f"{args.dataset}.{column}. "
                    f"Expected exactly 1 corrected record, "
                    f"found {validation_count}."
                )

        print(
            "Correction validation passed."
        )

        # --------------------------------------------------
        # Write corrected Bronze
        # --------------------------------------------------

        print(
            "Writing corrected Bronze..."
        )

        (
            corrected_df.write
            .format("parquet")
            .mode("overwrite")
            .save(bronze_path)
        )

        print(
            "Corrected Bronze written successfully."
        )

        print("")
        print("========================================")
        print("REMEDIATION SUCCESS")
        print("========================================")
        print(f"Dataset       : {args.dataset}")
        print(f"Rows          : {corrected_count}")
        print("Action        : SOURCE_CORRECTION")
        print("========================================")

    finally:

        spark.stop()


if __name__ == "__main__":
    main()