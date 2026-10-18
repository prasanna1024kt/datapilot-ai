import json
from pathlib import Path

import yaml
from pyspark.sql import SparkSession
from pyspark.sql.functions import col


CONFIG_ROOT = "/opt/datapilot/config"
DATASET = "olist_order_payments"
CHECK_COLUMN = "payment_installments"
OUTPUT = "/opt/spark-apps/results/quarantine/invalid_silver_records.json"


def main():
    with open(
        f"{CONFIG_ROOT}/variables.yaml", "r", encoding="utf-8"
    ) as file:
        variables = yaml.safe_load(file)

    with open(
        f"{CONFIG_ROOT}/dq/{DATASET}.yaml", "r", encoding="utf-8"
    ) as file:
        dq_config = yaml.safe_load(file)

    storage = variables["storage"]
    bucket = storage["bucket"]
    silver_layer = storage["layers"]["silver"]

    rule = dq_config["checks"]["range_checks"][CHECK_COLUMN]
    minimum = rule["min"]
    maximum = rule["max"]

    source_path = f"s3a://{bucket}/{silver_layer}/{DATASET}"

    spark = (
        SparkSession.builder
        .appName(f"DataPilot-Inspect-Silver-{DATASET}")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider",
        )
        .config(
            "spark.hadoop.fs.s3a.endpoint.region",
            variables.get("aws", {}).get("region", "us-east-1"),
        )
        .getOrCreate()
    )

    try:
        spark.sparkContext.setLogLevel("WARN")
        print(f"Reading Silver source: {source_path}")

        df = spark.read.parquet(source_path)

        required = [
            "order_id",
            "payment_sequential",
            "payment_type",
            "payment_installments",
            "payment_value",
        ]
        missing = sorted(set(required) - set(df.columns))
        if missing:
            raise ValueError(f"Missing required columns: {missing}")

        invalid = df.filter(
            col(CHECK_COLUMN).isNull()
            | (col(CHECK_COLUMN) < minimum)
            | (col(CHECK_COLUMN) > maximum)
        )

        invalid_count = invalid.count()

        records = [
            row.asDict(recursive=True)
            for row in invalid.select(*required).limit(21).collect()
        ]

        if invalid_count != 2 or len(records) != 2:
            raise RuntimeError(
                f"Expected exactly 2 invalid Silver records; "
                f"found {invalid_count}. No output report written."
            )

        keys = [
            (row["order_id"], row["payment_sequential"])
            for row in records
        ]
        if len(set(keys)) != len(keys):
            raise RuntimeError("Duplicate composite keys found in invalid records.")

        report = {
            "dataset": DATASET,
            "layer": "silver",
            "source_path": source_path,
            "rule": {
                "column": CHECK_COLUMN,
                "minimum": minimum,
                "maximum": maximum,
                "nulls_invalid": True,
            },
            "invalid_count": invalid_count,
            "records": records,
        }

        output_path = Path(OUTPUT)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with output_path.open("x", encoding="utf-8") as file:
            json.dump(report, file, indent=2, default=str)
            file.write("\n")

        print(json.dumps(report, indent=2, default=str))
        print(f"Diagnostic saved to: {OUTPUT}")
        print("No S3 writes performed.")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
