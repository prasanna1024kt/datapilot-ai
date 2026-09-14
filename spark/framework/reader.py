from pyspark.sql import SparkSession, DataFrame


class DataReader:

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def read(
        self,
        path: str,
        file_format: str,
        options: dict | None = None
    ) -> DataFrame:

        reader = self.spark.read.format(file_format)

        if options:
            for key, value in options.items():
                reader = reader.option(key, str(value))

        return reader.load(path)