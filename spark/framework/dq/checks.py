from pyspark.sql import DataFrame
from pyspark.sql.functions import col


class DQChecks:

    @staticmethod
    def row_count(
        df: DataFrame,
        minimum: int = 1
    ) -> dict:

        count = df.count()

        return {
            "check": "row_count",
            "status": "PASS" if count >= minimum else "FAIL",
            "actual": count,
            "expected": f">= {minimum}",
        }

    @staticmethod
    def required_columns(
        df: DataFrame,
        columns: list[str]
    ) -> dict:

        actual_columns = set(df.columns)

        missing = [
            column
            for column in columns
            if column not in actual_columns
        ]

        return {
            "check": "required_columns",
            "status": "PASS" if not missing else "FAIL",
            "actual": df.columns,
            "expected": columns,
            "missing": missing,
        }

    @staticmethod
    def null_check(
        df: DataFrame,
        columns: list[str]
    ) -> list[dict]:

        results = []

        for column in columns:

            if column not in df.columns:
                results.append({
                    "check": f"null_check:{column}",
                    "status": "FAIL",
                    "actual": "COLUMN_NOT_FOUND",
                    "expected": "column exists",
                })
                continue

            null_count = df.filter(
                col(column).isNull()
            ).count()

            results.append({
                "check": f"null_check:{column}",
                "status": "PASS" if null_count == 0 else "FAIL",
                "actual": null_count,
                "expected": 0,
            })

        return results

    @staticmethod
    def duplicate_check(
        df: DataFrame,
        keys: list[str]
    ) -> dict:

        missing = [
            key for key in keys
            if key not in df.columns
        ]

        if missing:
            return {
                "check": "duplicate_check",
                "status": "FAIL",
                "actual": "COLUMN_NOT_FOUND",
                "expected": keys,
                "missing": missing,
            }

        duplicate_count = (
            df.groupBy(*keys)
            .count()
            .filter(col("count") > 1)
            .count()
        )

        return {
            "check": "duplicate_check",
            "status": "PASS"
            if duplicate_count == 0
            else "FAIL",
            "actual": duplicate_count,
            "expected": 0,
            "keys": keys,
        }

    @staticmethod
    def range_check(df: DataFrame,column: str,minimum,maximum) -> dict:

        if column not in df.columns:
            return {
                "check": f"range_check:{column}",
                "status": "FAIL",
                "actual": "COLUMN_NOT_FOUND",
                "expected": f"{minimum} <= value <= {maximum}",
            }

        invalid_df = df.filter(
            (col(column) < minimum)
            | (col(column) > maximum)
        )

        invalid_count = invalid_df.count()

        sample_values = [
            row[column]
            for row in (
                invalid_df
                .select(column)
                .limit(10)
                .collect()
            )
        ]

        return {
            "check": f"range_check:{column}",
            "status": "PASS" if invalid_count == 0 else "FAIL",
            "actual": invalid_count,
            "invalid_count": invalid_count,
            "expected": f"{minimum} <= value <= {maximum}",
            "range": f"{minimum} <= value <= {maximum}",
            "sample_invalid_values": sample_values,
        }