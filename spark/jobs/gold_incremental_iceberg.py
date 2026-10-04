import argparse
import yaml

from pyspark import StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit


# =========================================================
# Arguments
# =========================================================

def parse_args():

    parser = argparse.ArgumentParser(
        description="DataPilot AI Generic Gold Incremental Iceberg Loader"
    )

    parser.add_argument(
        "--dataset",
        required=True,
        help="Dataset configured in gold_tables.yaml"
    )

    parser.add_argument(
        "--config",
        default="/opt/datapilot/config/gold_tables.yaml",
        help="Gold configuration YAML"
    )

    return parser.parse_args()


# =========================================================
# Configuration
# =========================================================

def load_config(config_path):

    with open(config_path, "r") as f:
        return yaml.safe_load(f)


# =========================================================
# Spark Session
# =========================================================

def create_spark(catalog, warehouse):

    return (
        SparkSession.builder.appName("DataPilot-Gold-Incremental-Iceberg")

        # =====================================================
        # Iceberg Catalog
        # =====================================================

        .config(f"spark.sql.catalog.{catalog}","org.apache.iceberg.spark.SparkCatalog")

        .config(f"spark.sql.catalog.{catalog}.catalog-impl","org.apache.iceberg.aws.glue.GlueCatalog")

        .config(f"spark.sql.catalog.{catalog}.warehouse",warehouse)

        .config( f"spark.sql.catalog.{catalog}.io-impl","org.apache.iceberg.aws.s3.S3FileIO")

        # =====================================================
        # Iceberg S3 multipart upload
        # =====================================================

        .config(f"spark.sql.catalog.{catalog}.s3.multipart.num-threads","4")

        .config(f"spark.sql.catalog.{catalog}.s3.multipart.part-size-bytes", "67108864")

        .config(f"spark.sql.catalog.{catalog}.s3.multipart.threshold", "1.5")

        # =====================================================
        # Iceberg AWS HTTP client
        # =====================================================

        .config(f"spark.sql.catalog.{catalog}.http-client.apache.connection-timeout-ms","120000")

        .config(f"spark.sql.catalog.{catalog}.http-client.apache.socket-timeout-ms","300000")

        .config(f"spark.sql.catalog.{catalog}.http-client.apache.connection-acquisition-timeout-ms","120000")

        .config(f"spark.sql.catalog.{catalog}.http-client.apache.max-connections","20")

        .config(f"spark.sql.catalog.{catalog}.http-client.apache.tcp-keep-alive-enabled","true")

        # =====================================================
        # Iceberg S3 retries
        # =====================================================

        .config(f"spark.sql.catalog.{catalog}.s3.retry.num-retries","8")

        .config(f"spark.sql.catalog.{catalog}.s3.retry.min-wait-ms","2000")

        .config(f"spark.sql.catalog.{catalog}.s3.retry.max-wait-ms","30000")

        # =====================================================
        # Spark
        # =====================================================

        .config("spark.sql.shuffle.partitions","4")

        .config("spark.default.parallelism", "4")

        .getOrCreate()
    )

def table_exists(spark, catalog, database, table):

    return spark.catalog.tableExists(
        f"{catalog}.{database}.{table}"
    )


def build_merge_condition(keys):

    return " AND ".join(
        f"target.`{key}` = source.`{key}`"
        for key in keys
    )


def validate_keys(df, keys):

    # -----------------------------------------------------
    # Check configured columns exist
    # -----------------------------------------------------

    missing = [
        key
        for key in keys
        if key not in df.columns
    ]

    if missing:

        raise RuntimeError(
            "Configured business keys are missing "
            f"from source dataset: {missing}"
        )

    # -----------------------------------------------------
    # NULL business key validation
    # -----------------------------------------------------

    null_condition = " OR ".join(
        f"`{key}` IS NULL"
        for key in keys
    )

    null_key_count = (
        df
        .filter(null_condition)
        .limit(1)
        .count()
    )

    if null_key_count > 0:

        raise RuntimeError(
            f"Source contains NULL business keys: {keys}"
        )


def validate_source_duplicates(df, keys):

    duplicate_count = (
        df
        .groupBy(*keys)
        .count()
        .filter("count > 1")
        .limit(1)
        .count()
    )

    if duplicate_count > 0:

        raise RuntimeError(
            "Source validation failed. "
            f"Duplicate business keys detected: {keys}"
        )


# =========================================================
# Main
# =========================================================

def main():

    args = parse_args()

    config = load_config(args.config)

    gold_config = config["gold"]

    catalog = gold_config["catalog"]
    database = gold_config["database"]
    warehouse = gold_config["warehouse"]

    tables = gold_config["tables"]

    # -----------------------------------------------------
    # Dataset validation
    # -----------------------------------------------------

    if args.dataset not in tables:

        raise RuntimeError(
            f"Dataset '{args.dataset}' is not configured "
            f"in {args.config}"
        )

    dataset_config = tables[args.dataset]

    if not dataset_config.get("enabled", False):

        raise RuntimeError(
            f"Dataset '{args.dataset}' is disabled."
        )

    source_path = dataset_config["source_path"]
    target_table = dataset_config["target_table"]
    keys = dataset_config["keys"]

    target = (
        f"{catalog}.{database}.{target_table}"
    )

    # -----------------------------------------------------
    # Spark
    # -----------------------------------------------------

    spark = create_spark(
        catalog,
        warehouse
    )

    spark.sparkContext.setLogLevel("WARN")

    print("\n========================================")
    print("DATAPILOT AI GOLD INCREMENTAL ICEBERG")
    print("========================================")

    print(f"Dataset       : {args.dataset}")
    print(f"Source        : {source_path}")
    print(f"Target        : {target}")
    print(f"Business Keys : {keys}")

    silver_df = None

    try:

        # -------------------------------------------------
        # Glue Database
        # -------------------------------------------------

        spark.sql(
            f"""
            CREATE DATABASE IF NOT EXISTS
            {catalog}.{database}
            """
        )

        print(
            f"\nGlue database ready: "
            f"{catalog}.{database}"
        )

        # -------------------------------------------------
        # Read Silver
        # -------------------------------------------------

        print("\nReading Silver data...")

        silver_df = (
            spark.read
            .parquet(source_path)
            .persist(StorageLevel.MEMORY_AND_DISK)
        )

        source_count = silver_df.count()

        print(
            f"Silver record count: "
            f"{source_count}"
        )

        silver_df.printSchema()

        # -------------------------------------------------
        # Validate keys
        # -------------------------------------------------

        print("\nValidating business keys...")

        validate_keys(
            silver_df,
            keys
        )

        print("Business key validation: PASS")

        # -------------------------------------------------
        # Source duplicate validation
        # -------------------------------------------------

        print(
            "\nChecking source duplicate "
            "business keys..."
        )

        validate_source_duplicates(
            silver_df,
            keys
        )

        print(
            "Source duplicate validation: PASS"
        )

        # -------------------------------------------------
        # Audit columns
        # -------------------------------------------------

        gold_df = (
            silver_df
            .withColumn(
                "gold_ingestion_timestamp",
                current_timestamp()
            )
            .withColumn(
                "gold_source_layer",
                lit("silver")
            )
        )

        # -------------------------------------------------
        # Create Gold table
        # -------------------------------------------------

        if not table_exists(
            spark,
            catalog,
            database,
            target_table
        ):

            print(
                "\nGold table does not exist."
            )

            print(
                "Creating Iceberg Gold table..."
            )

            (
                gold_df
                .writeTo(target)
                .using("iceberg")
                .tableProperty(
                    "format-version",
                    "2"
                )
                .create()
            )

            print(
                f"Created Gold Iceberg table: "
                f"{target}"
            )

        # -------------------------------------------------
        # Incremental MERGE
        # -------------------------------------------------

        else:

            print(
                "\nGold table already exists."
            )

            print(
                "Performing incremental MERGE..."
            )

            gold_df.createOrReplaceTempView(
                "gold_incremental_source"
            )

            merge_condition = (
                build_merge_condition(keys)
            )

            print(
                f"MERGE condition: "
                f"{merge_condition}"
            )

            spark.sql(
                f"""
                MERGE INTO {target} AS target

                USING gold_incremental_source AS source

                ON {merge_condition}

                WHEN MATCHED THEN
                    UPDATE SET *

                WHEN NOT MATCHED THEN
                    INSERT *
                """
            )

            print(
                "Incremental MERGE completed."
            )

        # -------------------------------------------------
        # Gold validation
        # -------------------------------------------------

        print(
            "\nValidating Gold table..."
        )

        gold_count = spark.sql(
            f"""
            SELECT COUNT(*)
            FROM {target}
            """
        ).collect()[0][0]

        duplicate_key_count = spark.sql(
            f"""
            SELECT COUNT(*)
            FROM (
                SELECT
                    {", ".join(
                        f"`{key}`"
                        for key in keys
                    )},
                    COUNT(*) AS record_count

                FROM {target}

                GROUP BY
                    {", ".join(
                        f"`{key}`"
                        for key in keys
                    )}

                HAVING COUNT(*) > 1
            )
            """
        ).collect()[0][0]

        print(
            f"Gold record count    : "
            f"{gold_count}"
        )

        print(
            f"Duplicate key groups : "
            f"{duplicate_key_count}"
        )

        if duplicate_key_count != 0:

            raise RuntimeError(
                "Gold validation failed. "
                f"Found {duplicate_key_count} "
                "duplicate business-key groups."
            )

        # -------------------------------------------------
        # Success
        # -------------------------------------------------

        print(
            "\n========================================"
        )

        print(
            "GOLD ICEBERG LOAD SUCCESSFUL"
        )

        print(
            "========================================"
        )

        print(
            f"Dataset : {args.dataset}"
        )

        print(
            f"Table   : {target}"
        )

        print(
            f"Records : {gold_count}"
        )

        print(
            "DQ      : PASS"
        )

        print(
            "========================================"
        )

    finally:

        # -------------------------------------------------
        # Release cached Silver data
        # -------------------------------------------------

        if silver_df is not None:
            silver_df.unpersist()

        spark.stop()


# =========================================================
# Entry Point
# =========================================================

if __name__ == "__main__":
    main()