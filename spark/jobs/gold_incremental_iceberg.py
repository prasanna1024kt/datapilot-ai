from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit


# =========================================================
# Configuration
# =========================================================

CATALOG = "glue_catalog"
DATABASE = "datapilot"
TABLE = "olist_order_payments"

SOURCE_PATH = (
    "s3a://datapilot-ai-data-practice/"
    "processed/silver/olist_order_payments"
)

WAREHOUSE = (
    "s3a://datapilot-ai-data-practice/"
    "gold"
)


# =========================================================
# Spark Session
# =========================================================

spark = (
    SparkSession.builder
    .appName("DataPilot-Gold-Incremental-Iceberg")

    # -----------------------------------------------------
    # Iceberg
    # -----------------------------------------------------

    .config(
        f"spark.sql.catalog.{CATALOG}",
        "org.apache.iceberg.spark.SparkCatalog"
    )
    .config(
        f"spark.sql.catalog.{CATALOG}.catalog-impl",
        "org.apache.iceberg.aws.glue.GlueCatalog"
    )
    .config(
        f"spark.sql.catalog.{CATALOG}.warehouse",
        WAREHOUSE
    )
    .config(
        f"spark.sql.catalog.{CATALOG}.io-impl",
        "org.apache.iceberg.aws.s3.S3FileIO"
    )

    .getOrCreate()
)


spark.sparkContext.setLogLevel("WARN")


# =========================================================
# Helper
# =========================================================

def table_exists(table_name):

    return spark.catalog.tableExists(
        f"{CATALOG}.{DATABASE}.{table_name}"
    )


# =========================================================
# Main
# =========================================================

def main():

    print("\n========================================")
    print("DATAPILOT AI GOLD INCREMENTAL ICEBERG")
    print("========================================")

    print(f"Source : {SOURCE_PATH}")
    print(
        f"Target : "
        f"{CATALOG}.{DATABASE}.{TABLE}"
    )

    # -----------------------------------------------------
    # Create Glue database
    # -----------------------------------------------------

    spark.sql(
        f"""
        CREATE DATABASE IF NOT EXISTS
        {CATALOG}.{DATABASE}
        """
    )

    print(
        f"Glue database ready: "
        f"{CATALOG}.{DATABASE}"
    )

    # -----------------------------------------------------
    # Read Silver
    # -----------------------------------------------------

    print("\nReading Silver data...")

    silver_df = (
        spark.read
        .parquet(SOURCE_PATH)
    )

    print(
        f"Silver record count: "
        f"{silver_df.count()}"
    )

    silver_df.printSchema()

    # -----------------------------------------------------
    # Defensive DQ filter
    #
    # Gold should NEVER receive invalid
    # payment_installments values.
    # -----------------------------------------------------

    valid_df = silver_df.filter(
        (silver_df.payment_installments >= 1)
        & (silver_df.payment_installments <= 100)
    )

    invalid_count = (
        silver_df.count()
        - valid_df.count()
    )

    print(
        f"Invalid records excluded from Gold: "
        f"{invalid_count}"
    )

    # -----------------------------------------------------
    # Stop if no valid records
    # -----------------------------------------------------

    if valid_df.limit(1).count() == 0:

        print(
            "No valid records available "
            "for Gold ingestion."
        )

        spark.stop()
        return

    # -----------------------------------------------------
    # Add audit columns
    # -----------------------------------------------------

    gold_df = (
        valid_df
        .withColumn(
            "gold_ingestion_timestamp",
            current_timestamp()
        )
        .withColumn(
            "gold_source_layer",
            lit("silver")
        )
    )

    # -----------------------------------------------------
    # Create Gold table if it doesn't exist
    # -----------------------------------------------------

    target = (
        f"{CATALOG}.{DATABASE}.{TABLE}"
    )

    if not table_exists(TABLE):

        print("\nGold table does not exist.")
        print("Creating Iceberg Gold table...")

        (
            gold_df.writeTo(target)
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

    else:

        print("\nGold table already exists.")
        print("Performing incremental MERGE...")

        # -------------------------------------------------
        # Temporary view for MERGE
        # -------------------------------------------------

        gold_df.createOrReplaceTempView(
            "gold_incremental_source"
        )

        # -------------------------------------------------
        # Incremental MERGE
        #
        # Business key:
        # order_id + payment_sequential
        # -------------------------------------------------

        spark.sql(
            f"""
            MERGE INTO {target} AS target
            USING gold_incremental_source AS source

            ON target.order_id =
                   source.order_id

            AND target.payment_sequential =
                   source.payment_sequential

            WHEN MATCHED THEN UPDATE SET *

            WHEN NOT MATCHED THEN INSERT *
            """
        )

        print(
            "Incremental MERGE completed."
        )

    # -----------------------------------------------------
    # Gold validation
    # -----------------------------------------------------

    print("\nValidating Gold table...")

    gold_count = spark.sql(
        f"""
        SELECT COUNT(*)
        FROM {target}
        """
    ).collect()[0][0]

    invalid_gold_count = spark.sql(
        f"""
        SELECT COUNT(*)
        FROM {target}
        WHERE payment_installments < 1
           OR payment_installments > 100
        """
    ).collect()[0][0]

    print(
        f"Gold record count : {gold_count}"
    )

    print(
        f"Invalid Gold records : "
        f"{invalid_gold_count}"
    )

    # -----------------------------------------------------
    # Final result
    # -----------------------------------------------------

    if invalid_gold_count != 0:

        raise RuntimeError(
            "Gold validation failed. "
            f"Found {invalid_gold_count} "
            "invalid payment_installments records."
        )

    print("\n========================================")
    print("GOLD ICEBERG LOAD SUCCESSFUL")
    print("========================================")

    print(
        f"Table: {target}"
    )

    print(
        f"Records: {gold_count}"
    )

    print(
        "DQ Status: PASS"
    )

    print("========================================")

    spark.stop()


# =========================================================
# Entry Point
# =========================================================

if __name__ == "__main__":
    main()