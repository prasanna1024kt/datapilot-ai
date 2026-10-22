from pyspark.sql import SparkSession
from framework.payment_quality import split_valid_and_invalid_payments

SOURCE = (
    "s3a://datapilot-ai-data-practice/"
    "processed/silver/olist_order_payments"
)


def main():
    spark = (
        SparkSession.builder
        .appName("DataPilot-Inspect-Payment-Quality")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        payments = spark.read.parquet(SOURCE)
        valid, invalid = split_valid_and_invalid_payments(payments)

        source_count = payments.count()
        valid_count = valid.count()
        invalid_count = invalid.count()

        print("\n=== READ-ONLY PAYMENT DQ REPORT ===")
        print(f"Source rows : {source_count}")
        print(f"Valid rows  : {valid_count}")
        print(f"Invalid rows: {invalid_count}")

        invalid.select(
            "order_id",
            "payment_sequential",
            "payment_installments",
            "payment_value",
        ).show(20, truncate=False)

        if source_count != valid_count + invalid_count:
            raise RuntimeError("DQ split count reconciliation failed.")

        if invalid_count != 2:
            raise RuntimeError(
                f"Expected 2 invalid rows based on prior inspection; "
                f"found {invalid_count}. Stop before Gold changes."
            )

        print("PASS: source count reconciles; no data was written.")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
