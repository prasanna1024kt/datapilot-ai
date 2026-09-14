from pyspark.sql import DataFrame
from pyspark.sql.functions import col, to_timestamp


class DataTransformer:

    def __init__(self, spark):
        self.spark = spark

    def standardize_column_names(self, df: DataFrame) -> DataFrame:
        for column in df.columns:
            new_column = (
                column.strip()
                .lower()
                .replace(" ", "_")
                .replace("-", "_")
            )

            if column != new_column:
                df = df.withColumnRenamed(column, new_column)

        return df

    def remove_duplicates(
        self,
        df: DataFrame,
        keys: list[str]
    ) -> DataFrame:

        return df.dropDuplicates(keys)

    def convert_timestamps(
        self,
        df: DataFrame,
        columns: list[str]
    ) -> DataFrame:

        for column in columns:
            if column in df.columns:
                df = df.withColumn(
                    column,
                    to_timestamp(col(column))
                )

        return df

    def apply(self, df: DataFrame, transformation_config: dict) -> DataFrame:

        if not transformation_config:
            return df

        # 1. Standardize column names
        if transformation_config.get(
            "standardize_column_names",
            False
        ):
            df = self.standardize_column_names(df)

        # 2. Remove duplicates
        duplicate_config = transformation_config.get(
            "remove_duplicates",
            {}
        )

        if duplicate_config.get("enabled", False):
            keys = duplicate_config.get("keys", [])

            if keys:
                df = self.remove_duplicates(df, keys)

        # 3. Convert timestamp columns
        timestamp_columns = transformation_config.get(
            "timestamp_columns",
            []
        )

        if timestamp_columns:
            df = self.convert_timestamps(
                df,
                timestamp_columns
            )

        return df