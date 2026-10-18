from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, when
from framework.config_loader import ConfigLoader
from framework.path_builder import PathBuilder

CONFIG_ROOT = "/opt/datapilot/config"
DATASET = "olist_order_payments"

def main():
    loader = ConfigLoader(CONFIG_ROOT)
    variables = loader.load_variables()
    path_builder = PathBuilder(variables)
    source_path = path_builder.build_layer_path("silver", DATASET)

    spark = (
        SparkSession.builder
        .appName("DataPilotAI-InspectSilverSchema")
        .getOrCreate()
    )

    try:
        print(f"Silver source: {source_path}")
        df = spark.read.parquet(source_path)

        print("\n=== Silver schema ===")
        df.printSchema()

        print("\n=== Business key null counts ===")
        df.select(
            sum(when(col("order_id").isNull(), 1).otherwise(0))
            .alias("null_order_id"),
            sum(when(col("payment_sequential").isNull(), 1).otherwise(0))
            .alias("null_payment_sequential")
        ).show(truncate=False)

        print("Read-only inspection completed. No writes performed.")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
