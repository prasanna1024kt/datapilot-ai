from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    count,
    sum,
)


class BusinessTransformer:

    def __init__(self, spark):
        self.spark = spark

    def build_order_summary(
        self,
        orders: DataFrame,
        order_items: DataFrame,
        payments: DataFrame,
        customers: DataFrame,
    ) -> DataFrame:

        # Aggregate order items to order level
        item_summary = (
            order_items
            .groupBy("order_id")
            .agg(
                count("*").alias("product_count"),
                sum("price").alias("total_price"),
                sum("freight_value").alias("total_freight"),
            )
        )

        # Aggregate payments to order level
        payment_summary = (
            payments
            .groupBy("order_id")
            .agg(
                sum("payment_value").alias("total_payment"),
            )
        )

        # Select required customer attributes
        customer_dim = customers.select(
            "customer_id",
            "customer_city",
            "customer_state",
        )

        # Build order-level business dataset
        result = (
            orders
            .join(
                item_summary,
                on="order_id",
                how="left",
            )
            .join(
                payment_summary,
                on="order_id",
                how="left",
            )
            .join(
                customer_dim,
                on="customer_id",
                how="left",
            )
        )

        # Select Gold columns
        result = result.select(
            "order_id",
            "customer_id",
            "order_status",
            "order_purchase_timestamp",
            "order_delivered_customer_date",
            "customer_city",
            "customer_state",
            "product_count",
            "total_price",
            "total_freight",
            "total_payment",
        )

        # Handle missing aggregates
        result = result.fillna(
            {
                "product_count": 0,
                "total_price": 0,
                "total_freight": 0,
                "total_payment": 0,
            }
        )

        return result