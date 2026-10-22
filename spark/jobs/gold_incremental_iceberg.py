
import argparse
from functools import reduce
from operator import or_
from pathlib import Path

import yaml
from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, current_timestamp, lit

from framework.payment_quality import split_valid_and_invalid_payments

PAYMENT_DATASET = "olist_order_payments"
PAYMENT_SOURCE_COLUMNS = [
    "order_id",
    "payment_sequential",
    "payment_type",
    "payment_installments",
    "payment_value",
]
QUARANTINE_METADATA_COLUMNS = [
    "execution_id",
    "quarantine_reason",
    "rule_name",
    "source_layer",
    "quarantined_at",
]


def parse_args():
    parser = argparse.ArgumentParser(
        description="DataPilot AI Generic Gold Incremental Iceberg Loader"
    )
    parser.add_argument(
        "--dataset",
        required=True,
        help="Dataset configured in gold_tables.yaml",
    )
    parser.add_argument(
        "--config",
        default="/opt/datapilot/config/gold_tables.yaml",
        help="Gold configuration YAML",
    )
    return parser.parse_args()


def load_config(config_path):
    with open(config_path, "r", encoding="utf-8") as file:
        config = yaml.safe_load(file)
    if not isinstance(config, dict):
        raise RuntimeError(f"Configuration must be a YAML mapping: {config_path}")
    return config


def create_spark(catalog, warehouse):
    return (
        SparkSession.builder.appName("DataPilot-Gold-Incremental-Iceberg")
        .config(f"spark.sql.catalog.{catalog}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{catalog}.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
        .config(f"spark.sql.catalog.{catalog}.warehouse", warehouse)
        .config(f"spark.sql.catalog.{catalog}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config(f"spark.sql.catalog.{catalog}.s3.multipart.num-threads", "4")
        .config(f"spark.sql.catalog.{catalog}.s3.multipart.part-size-bytes", "67108864")
        .config(f"spark.sql.catalog.{catalog}.s3.multipart.threshold", "1.5")
        .config(f"spark.sql.catalog.{catalog}.http-client.apache.connection-timeout-ms", "120000")
        .config(f"spark.sql.catalog.{catalog}.http-client.apache.socket-timeout-ms", "300000")
        .config(f"spark.sql.catalog.{catalog}.http-client.apache.connection-acquisition-timeout-ms", "120000")
        .config(f"spark.sql.catalog.{catalog}.http-client.apache.max-connections", "20")
        .config(f"spark.sql.catalog.{catalog}.http-client.apache.tcp-keep-alive-enabled", "true")
        .config(f"spark.sql.catalog.{catalog}.s3.retry.num-retries", "8")
        .config(f"spark.sql.catalog.{catalog}.s3.retry.min-wait-ms", "2000")
        .config(f"spark.sql.catalog.{catalog}.s3.retry.max-wait-ms", "30000")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.default.parallelism", "4")
        .getOrCreate()
    )


def table_exists(spark, catalog, database, table):
    return spark.catalog.tableExists(f"{catalog}.{database}.{table}")


def build_merge_condition(keys):
    return " AND ".join(f"target.`{key}` = source.`{key}`" for key in keys)


def validate_keys(df: DataFrame, keys):
    missing = [key for key in keys if key not in df.columns]
    if missing:
        raise RuntimeError(f"Configured business keys are missing from source dataset: {missing}")

    null_condition = reduce(or_, [col(key).isNull() for key in keys])
    if df.filter(null_condition).limit(1).count() > 0:
        raise RuntimeError(f"Source contains NULL business keys: {keys}")


def validate_source_duplicates(df: DataFrame, keys):
    duplicate_exists = (
        df.groupBy(*keys)
        .count()
        .filter(col("count") > 1)
        .limit(1)
        .count()
        > 0
    )
    if duplicate_exists:
        raise RuntimeError(f"Source validation failed. Duplicate business keys detected: {keys}")


def validate_quarantine_config(config_path: Path, dataset: str, expected_keys):
    quarantine_path = config_path.parent / "quarantine" / f"{dataset}.yaml"
    if not quarantine_path.is_file():
        raise RuntimeError(f"Quarantine configuration not found: {quarantine_path}")

    config = load_config(quarantine_path)
    settings = config.get("quarantine")
    if config.get("dataset") != dataset or not isinstance(settings, dict):
        raise RuntimeError("Quarantine configuration dataset/settings are invalid.")

    required = {
        "enabled": True,
        "catalog": "glue_catalog",
        "database": "datapilot",
        "table": "olist_order_payments_quarantine",
        "format": "iceberg",
        "source_layer": "silver",
        "target_layer": "quarantine",
        "keys": list(expected_keys),
        "validate_source_duplicates": True,
    }
    for key, expected in required.items():
        if settings.get(key) != expected:
            raise RuntimeError(
                f"Invalid quarantine configuration for {key!r}: "
                f"expected {expected!r}, got {settings.get(key)!r}."
            )
    return settings


def verify_records_are_quarantined(
    records: DataFrame,
    quarantine: DataFrame,
    keys,
    source_columns,
    label: str,
):
    """Require each input record to match one quarantine row by key and values."""
    missing = sorted(set(source_columns) - set(quarantine.columns))
    if missing:
        raise RuntimeError(f"Quarantine table is missing source columns: {missing}")

    source_types = {field.name: field.dataType for field in records.schema.fields}
    quarantine_types = {field.name: field.dataType for field in quarantine.schema.fields}
    incompatible = [
        name for name in source_columns
        if source_types[name] != quarantine_types[name]
    ]
    if incompatible:
        raise RuntimeError(
            f"Quarantine source-column types do not match the incoming records: {incompatible}"
        )

    null_quarantine_key_condition = reduce(or_, [col(key).isNull() for key in keys])
    if quarantine.filter(null_quarantine_key_condition).limit(1).count():
        raise RuntimeError("Quarantine table contains NULL business keys; refusing Gold changes.")

    quarantine_key_duplicates = (
        quarantine.groupBy(*keys)
        .count()
        .filter(col("count") > 1)
        .limit(1)
        .count()
    )
    if quarantine_key_duplicates:
        raise RuntimeError("Quarantine table contains duplicate business keys; refusing Gold changes.")

    source_alias = records.select(*source_columns).alias("source_record")
    quarantine_alias = quarantine.select(*source_columns).alias("quarantine_record")
    join_condition = reduce(
        lambda left, right: left & right,
        [col(f"source_record.`{key}`") == col(f"quarantine_record.`{key}`") for key in keys],
    )
    joined = source_alias.join(quarantine_alias, join_condition, "left")

    # A null on the joined quarantine key identifies a missing record. The
    # null-safe comparisons below validate every source field when it exists.
    missing_key_condition = col(f"quarantine_record.`{keys[0]}`").isNull()
    value_mismatch = reduce(
        or_,
        [
            ~col(f"source_record.`{column}`").eqNullSafe(
                col(f"quarantine_record.`{column}`")
            )
            for column in source_columns
        ],
    )
    bad_count = joined.filter(missing_key_condition | value_mismatch).count()
    if bad_count:
        raise RuntimeError(
            f"{label}: {bad_count} record(s) are missing from quarantine or differ from quarantined values. "
            "Gold changes are blocked."
        )


def count_invalid_payments(df: DataFrame) -> int:
    _, invalid = split_valid_and_invalid_payments(df)
    return invalid.count()


def main():
    args = parse_args()
    config_path = Path(args.config).resolve()
    config = load_config(config_path)
    gold_config = config["gold"]

    catalog = gold_config["catalog"]
    database = gold_config["database"]
    warehouse = gold_config["warehouse"]
    tables = gold_config["tables"]

    if args.dataset not in tables:
        raise RuntimeError(f"Dataset '{args.dataset}' is not configured in {args.config}")

    dataset_config = tables[args.dataset]
    if not dataset_config.get("enabled", False):
        raise RuntimeError(f"Dataset '{args.dataset}' is disabled.")

    source_path = dataset_config["source_path"]
    target_table = dataset_config["target_table"]
    keys = dataset_config["keys"]
    target = f"{catalog}.{database}.{target_table}"

    spark = create_spark(catalog, warehouse)
    spark.sparkContext.setLogLevel("WARN")
    print("\n========================================")
    print("DATAPILOT AI GOLD INCREMENTAL ICEBERG")
    print("========================================")
    print(f"Dataset       : {args.dataset}")
    print(f"Source        : {source_path}")
    print(f"Target        : {target}")
    print(f"Business Keys : {keys}")

    source_df = None
    invalid_source = None
    quarantine_df = None
    existing_invalid_gold = None
    existing_invalid_count = 0
    try:
        # This creates catalog metadata if missing, as the original loader did.
        spark.sql(f"CREATE DATABASE IF NOT EXISTS `{catalog}`.`{database}`")
        print(f"\nGlue database ready: {catalog}.{database}")

        source_df = spark.read.parquet(source_path).persist(StorageLevel.MEMORY_AND_DISK)
        source_count = source_df.count()
        print(f"Silver record count: {source_count}")
        source_df.printSchema()

        validate_keys(source_df, keys)
        validate_source_duplicates(source_df, keys)
        print("Source business-key validation: PASS")

        target_exists = table_exists(spark, catalog, database, target_table)
        gold_source = source_df
        quarantine_table = None

        if args.dataset == PAYMENT_DATASET:
            missing_payment_columns = sorted(set(PAYMENT_SOURCE_COLUMNS) - set(source_df.columns))
            if missing_payment_columns:
                raise RuntimeError(f"Payment source is missing required columns: {missing_payment_columns}")

            valid_source, invalid_source = split_valid_and_invalid_payments(source_df)
            invalid_source = invalid_source.persist(StorageLevel.MEMORY_AND_DISK)
            invalid_source_count = invalid_source.count()
            valid_source_count = valid_source.count()
            print(f"Valid payment records  : {valid_source_count}")
            print(f"Invalid payment records: {invalid_source_count}")

            quarantine_settings = validate_quarantine_config(config_path, args.dataset, keys)
            quarantine_table = (
                f"{quarantine_settings['catalog']}."
                f"{quarantine_settings['database']}."
                f"{quarantine_settings['table']}"
            )

            existing_invalid_gold = None
            if target_exists:
                existing_gold = spark.table(target)
                missing_target_columns = sorted(set(PAYMENT_SOURCE_COLUMNS) - set(existing_gold.columns))
                if missing_target_columns:
                    raise RuntimeError(f"Existing Gold payments table is missing columns: {missing_target_columns}")
                existing_invalid_gold = existing_gold.filter(
                    col("payment_installments").isNull()
                    | (col("payment_installments") < 1)
                    | (col("payment_installments") > 100)
                ).select(*PAYMENT_SOURCE_COLUMNS).persist(StorageLevel.MEMORY_AND_DISK)

            needs_quarantine_verification = invalid_source_count > 0
            existing_invalid_count = (
                existing_invalid_gold.count() if existing_invalid_gold is not None else 0
            )
            needs_quarantine_verification = needs_quarantine_verification or existing_invalid_count > 0

            if needs_quarantine_verification:
                if not spark.catalog.tableExists(quarantine_table):
                    raise RuntimeError(
                        f"Quarantine table {quarantine_table} does not exist, but invalid payment records "
                        "exist. Gold ingestion is blocked; quarantine and verify the records first."
                    )
                quarantine_df = spark.table(quarantine_table).persist(StorageLevel.MEMORY_AND_DISK)
                missing_quarantine_metadata = sorted(
                    set(QUARANTINE_METADATA_COLUMNS) - set(quarantine_df.columns)
                )
                if missing_quarantine_metadata:
                    raise RuntimeError(
                        f"Quarantine table is missing metadata columns: {missing_quarantine_metadata}"
                    )

                if invalid_source_count:
                    verify_records_are_quarantined(
                        invalid_source,
                        quarantine_df,
                        keys,
                        PAYMENT_SOURCE_COLUMNS,
                        "Invalid Silver payment verification",
                    )
                if existing_invalid_gold is not None and existing_invalid_count:
                    verify_records_are_quarantined(
                        existing_invalid_gold,
                        quarantine_df,
                        keys,
                        PAYMENT_SOURCE_COLUMNS,
                        "Existing invalid Gold payment verification",
                    )
                print("Quarantine verification: PASS")
            elif target_exists:
                existing_gold = spark.table(target)
                # Even with no invalid Silver rows, verify the target has no invalid records.
                remaining_invalid = existing_gold.filter(
                    col("payment_installments").isNull()
                    | (col("payment_installments") < 1)
                    | (col("payment_installments") > 100)
                ).limit(1).count()
                if remaining_invalid:
                    raise RuntimeError("Gold contains invalid payment rows; quarantine verification is required.")

            gold_source = valid_source

        gold_df = (
            gold_source
            .withColumn("gold_ingestion_timestamp", current_timestamp())
            .withColumn("gold_source_layer", lit("silver"))
        )

        if not target_exists:
            print("\nGold table does not exist; creating Iceberg table from valid source records.")
            (
                gold_df.writeTo(target)
                .using("iceberg")
                .tableProperty("format-version", "2")
                .create()
            )
            print(f"Created Gold Iceberg table: {target}")
        else:
            print("\nGold table already exists; performing incremental MERGE.")
            gold_df.createOrReplaceTempView("gold_incremental_source")
            merge_condition = build_merge_condition(keys)
            print(f"MERGE condition: {merge_condition}")
            spark.sql(
                f"""
                MERGE INTO {target} AS target
                USING gold_incremental_source AS source
                ON {merge_condition}
                WHEN MATCHED THEN UPDATE SET *
                WHEN NOT MATCHED THEN INSERT *
                """
            )
            print("Incremental MERGE completed.")

        # Delete only after the invalid rows are confirmed in quarantine.
        if args.dataset == PAYMENT_DATASET and target_exists and existing_invalid_count > 0:
            spark.sql(
                f"""
                DELETE FROM {target}
                WHERE payment_installments IS NULL
                   OR payment_installments < 1
                   OR payment_installments > 100
                """
            )
            print("Removed invalid payment rows from Gold after quarantine verification.")

        gold_count = spark.table(target).count()
        duplicate_key_count = (
            spark.table(target)
            .groupBy(*keys)
            .count()
            .filter(col("count") > 1)
            .count()
        )
        if duplicate_key_count:
            raise RuntimeError(f"Gold validation failed: {duplicate_key_count} duplicate business-key groups.")

        if args.dataset == PAYMENT_DATASET:
            invalid_gold_count = count_invalid_payments(spark.table(target))
            if invalid_gold_count:
                raise RuntimeError(f"Gold payment validation failed: {invalid_gold_count} invalid rows remain.")

        print("\n========================================")
        print("GOLD ICEBERG LOAD SUCCESSFUL")
        print("========================================")
        print(f"Dataset : {args.dataset}")
        print(f"Table   : {target}")
        print(f"Records : {gold_count}")
        print("DQ      : PASS")
        print("========================================")

    finally:
        if source_df is not None:
            source_df.unpersist()
        if invalid_source is not None:
            invalid_source.unpersist()
        if quarantine_df is not None:
            quarantine_df.unpersist()
        if existing_invalid_gold is not None:
            existing_invalid_gold.unpersist()
        spark.stop()


if __name__ == "__main__":
    main()
