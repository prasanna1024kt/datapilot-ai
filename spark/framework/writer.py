from pyspark.sql import DataFrame


class DataWriter:

    def write(
        self,
        df: DataFrame,
        path: str,
        file_format: str,
        mode: str = "overwrite"
    ) -> None:

        (
            df.write
            .format(file_format)
            .mode(mode)
            .save(path)
        )