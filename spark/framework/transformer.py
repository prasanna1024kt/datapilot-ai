from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    lit,
    to_timestamp,
    trim,
    when
)


class DataTransformer:

    def __init__(self, spark):
        self.spark = spark

    # --------------------------------------------------
    # Standardize column names
    # --------------------------------------------------

    def standardize_column_names(self, df: DataFrame) -> DataFrame:

        for column in df.columns:

            new_column = (
                column.strip()
                .lower()
                .replace(" ", "_")
                .replace("-", "_")
            )

            if column != new_column:

                df = df.withColumnRenamed(
                    column,
                    new_column
                )

        return df

    # --------------------------------------------------
    # Remove duplicates
    # --------------------------------------------------

    def remove_duplicates(self, df: DataFrame, keys: list[str]) -> DataFrame:

        existing_keys = [
            key for key in keys
            if key in df.columns
        ]

        if not existing_keys:
            return df

        return df.dropDuplicates(
            existing_keys
        )

    # --------------------------------------------------
    # Convert timestamps
    # --------------------------------------------------

    def convert_timestamps(self, df: DataFrame, columns: list[str]) -> DataFrame:

        for column in columns:

            if column in df.columns:

                df = df.withColumn(
                    column,
                    to_timestamp(
                        col(column)
                    )
                )

        return df

    # --------------------------------------------------
    # Cast columns
    # --------------------------------------------------

    def cast_columns(self, df: DataFrame, columns: dict) -> DataFrame:

        for column, data_type in columns.items():

            if column in df.columns:

                df = df.withColumn(column, col(column).cast(data_type))

        return df

    # --------------------------------------------------
    # Trim string columns
    # --------------------------------------------------

    def trim_string_columns(self, df: DataFrame) -> DataFrame:

        for field in df.schema.fields:

            if field.dataType.simpleString() == "string":

                df = df.withColumn(
                    field.name,
                    trim(col(field.name))
                )

        return df

    # --------------------------------------------------
    # Handle null values
    # --------------------------------------------------

    def handle_nulls(self, df: DataFrame, null_config: dict) -> DataFrame:

        # Drop rows where configured columns are null
        drop_columns = null_config.get("drop_if_null", [])

        existing_columns = [
            column
            for column in drop_columns
            if column in df.columns
        ]

        if existing_columns:

            df = df.dropna(subset=existing_columns)

        # Fill configured values
        fill_values = null_config.get("fill", {})

        if fill_values:

            df = df.fillna(fill_values)

        return df

    # --------------------------------------------------
    # Filter rows
    # --------------------------------------------------

    def filter_rows(self, df: DataFrame, expressions: list[str]) -> DataFrame:

        for expression in expressions:

            df = df.filter(expression)

        return df

    # --------------------------------------------------
    # Drop columns
    # --------------------------------------------------

    def drop_columns(self, df: DataFrame, columns: list[str]) -> DataFrame:

        existing_columns = [
            column
            for column in columns
            if column in df.columns
        ]

        if existing_columns:

            df = df.drop(*existing_columns)

        return df

    # --------------------------------------------------
    # Apply transformations
    # --------------------------------------------------

    def apply(self, df: DataFrame, transformation_config: dict) -> DataFrame:

        if not transformation_config:
            return df

        # 1. Standardize column names
        if transformation_config.get("standardize_column_names", False):

            df = self.standardize_column_names(df)

        # 2. Trim strings
        if transformation_config.get("trim_strings", False):

            df = self.trim_string_columns(df)

        # 3. Cast columns
        cast_config = transformation_config.get("cast_columns", {})

        if cast_config:

            df = self.cast_columns(df, cast_config)

        # 4. Timestamp conversion
        timestamp_columns = (transformation_config.get("timestamp_columns", []))

        if timestamp_columns:

            df = self.convert_timestamps(df, timestamp_columns)

        # 5. Remove duplicates
        duplicate_config = (transformation_config.get("remove_duplicates", {}))

        if duplicate_config.get("enabled", False):

            keys = duplicate_config.get("keys", [])

            if keys:

                df = self.remove_duplicates(df, keys)

        # 6. Null handling
        null_config = (transformation_config.get("null_handling", {}))

        if null_config:

            df = self.handle_nulls(df, null_config)

        # 7. Row filters
        filter_config = (transformation_config.get("filters", []))

        if filter_config:

            df = self.filter_rows(df, filter_config)

        # 8. Drop columns
        drop_columns = (transformation_config.get("drop_columns", []))

        if drop_columns:

            df = self.drop_columns(df, drop_columns)

        return df
