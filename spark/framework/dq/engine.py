from pyspark.sql import DataFrame

from framework.dq.checks import DQChecks


class DQEngine:

    def __init__(self, spark):
        self.spark = spark

    def run(
        self,
        df: DataFrame,
        dq_config: dict
    ) -> list[dict]:

        results = []

        checks = dq_config.get("checks", {})

        # -------------------------
        # Row Count
        # -------------------------

        row_count_config = checks.get(
            "row_count",
            {}
        )

        if row_count_config.get("enabled", False):

            minimum = row_count_config.get(
                "min",
                1
            )

            results.append(
                DQChecks.row_count(
                    df,
                    minimum
                )
            )

        # -------------------------
        # Required Columns
        # -------------------------

        required_config = checks.get(
            "required_columns",
            {}
        )

        if required_config.get("enabled", False):

            columns = required_config.get(
                "columns",
                []
            )

            results.append(
                DQChecks.required_columns(
                    df,
                    columns
                )
            )

        # -------------------------
        # Null Checks
        # -------------------------

        null_config = checks.get(
            "null_checks",
            {}
        )

        if null_config.get("enabled", False):

            columns = null_config.get(
                "columns",
                []
            )

            results.extend(
                DQChecks.null_check(
                    df,
                    columns
                )
            )

        # -------------------------
        # Duplicate Check
        # -------------------------

        duplicate_config = checks.get(
            "duplicate_check",
            {}
        )

        if duplicate_config.get(
            "enabled",
            False
        ):

            keys = duplicate_config.get(
                "keys",
                []
            )

            results.append(
                DQChecks.duplicate_check(
                    df,
                    keys
                )
            )

        # -------------------------
        # Range Checks
        # -------------------------

        range_config = checks.get(
            "range_checks",
            {}
        )

        if range_config.get(
            "enabled",
            False
        ):

            for column, config in range_config.items():

                if column == "enabled":
                    continue

                minimum = config.get("min")
                maximum = config.get("max")

                results.append(
                    DQChecks.range_check(
                        df,
                        column,
                        minimum,
                        maximum
                    )
                )

        return results

    @staticmethod
    def has_failures(
        results: list[dict]
    ) -> bool:

        return any(
            result["status"] == "FAIL"
            for result in results
        )