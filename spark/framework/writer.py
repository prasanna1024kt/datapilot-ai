from pyspark.sql import DataFrame


class DataWriter:

    def write(
        self,
        df: DataFrame,
        path: str,
        file_format: str,
        mode: str = "overwrite",
        write_config: dict | None = None,
    ) -> None:

        write_config = write_config or {}

        partition_strategy = write_config.get(
            "partition_strategy",
            "none"
        )

        partitions = write_config.get(
            "partitions"
        )

        # --------------------------------------------------
        # Partition strategy
        # --------------------------------------------------

        if partition_strategy == "coalesce":

            if not partitions:
                raise ValueError(
                    "partitions is required when "
                    "partition_strategy=coalesce"
                )

            df = df.coalesce(partitions)

        elif partition_strategy == "repartition":

            if not partitions:
                raise ValueError(
                    "partitions is required when "
                    "partition_strategy=repartition"
                )

            df = df.repartition(partitions)

        elif partition_strategy == "none":
            pass

        else:
            raise ValueError(
                f"Unsupported partition_strategy: "
                f"{partition_strategy}"
            )

        # --------------------------------------------------
        # Write
        # --------------------------------------------------

        (
            df.write
            .format(file_format)
            .mode(mode)
            .save(path)
        )