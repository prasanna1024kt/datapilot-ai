"""Data-quality rules for Olist payment records."""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col


def split_valid_and_invalid_payments(
    payments: DataFrame,
) -> tuple[DataFrame, DataFrame]:
    """
    Split payments using the configured business rule:
    payment_installments must be between 1 and 100 inclusive.

    Returns (valid_payments, invalid_payments).
    Does not modify the input DataFrame.
    """
    required_columns = {
        "order_id",
        "payment_sequential",
        "payment_type",
        "payment_installments",
        "payment_value",
    }

    missing = sorted(required_columns - set(payments.columns))
    if missing:
        raise ValueError(
            f"Payment dataset is missing required columns: {missing}"
        )

    valid_condition = (
        col("payment_installments").isNotNull()
        & (col("payment_installments") >= 1)
        & (col("payment_installments") <= 100)
    )

    valid_payments = payments.filter(valid_condition)
    invalid_payments = payments.filter(~valid_condition)

    return valid_payments, invalid_payments
