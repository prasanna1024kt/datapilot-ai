from pyspark.sql import SparkSession
from framework.config_loader import ConfigLoader
from framework.path_builder import PathBuilder
from framework.reader import DataReader
from framework.writer import DataWriter
from framework.transformer import DataTransformer


class DataPipeline:

    def __init__(self, spark: SparkSession, config_root: str):

        self.spark = spark
        self.config_loader = ConfigLoader(config_root)

        self.variables = (self.config_loader.load_variables())

        # --------------------------------------------------
        # Initialize framework components
        # --------------------------------------------------

        self.path_builder = PathBuilder(self.variables)

        self.reader = DataReader(spark)
        self.writer = DataWriter()
        self.transformer = DataTransformer(spark)

    def run(self, layer: str, table: str) -> None:

        print(
            f"Starting pipeline: "
            f"layer={layer}, table={table}",
            flush=True
        )

        # --------------------------------------------------
        # Load layer configuration
        # --------------------------------------------------

        config = (
            self.config_loader
            .load_layer_config(
                layer=layer,
                table=table
            )
        )

        source_config = config["source"]
        target_config = config["target"]

        # --------------------------------------------------
        # Resolve source path
        # --------------------------------------------------

        source_type = source_config["type"]

        if source_type == "raw":

            source_path = (self.path_builder.build_raw_file_path(source_config["file"]))
            

            source_format = (
                self.variables["storage"]
                ["formats"]["raw"]
            )

        else:

            source_table = source_config.get(
                "table",
                table
            )

            source_path = (
                self.path_builder
                .build_layer_path(
                    source_type,
                    source_table
                )
            )

            source_format = (
                self.variables["storage"]
                ["formats"][source_type]
            )

        # --------------------------------------------------
        # Resolve target path
        # --------------------------------------------------

        target_type = target_config["type"]

        target_path = (
            self.path_builder
            .build_layer_path(
                target_type,
                table
            )
        )

        target_format = (
            self.variables["storage"]
            ["formats"][target_type]
        )

        # --------------------------------------------------
        # Log pipeline paths
        # --------------------------------------------------

        print(f"Source       : {source_path}", flush=True)

        print(f"Source format: {source_format}", flush=True)

        print(f"Target       : {target_path}", flush=True)

        print(f"Target format: {target_format}", flush=True)

        # --------------------------------------------------
        # Read
        # --------------------------------------------------

        read_config = config.get("read", {})

        print("Reading source data...", flush=True)

        df = self.reader.read(
            path=source_path,
            file_format=source_format,
            options=read_config
        )

        print("Source read completed.", flush=True)

        # --------------------------------------------------
        # Transform
        # --------------------------------------------------

        transformation_config = config.get("transformations", {})

        if transformation_config:

            print("Applying transformations...", flush=True)

            df = self.transformer.apply(
                df=df,
                transformation_config=transformation_config
            )

            print("Transformations completed.", flush=True)

        else:

            print(
                "No transformations configured. "
                "Skipping transformation step.",
                flush=True
            )

        # --------------------------------------------------
        # Write configuration
        # --------------------------------------------------

        write_config = config.get(
            "write",
            {}
        )

        write_mode = write_config.get(
            "mode",
            "overwrite"
        )

        print(
            f"Write mode: {write_mode}",
            flush=True
        )

        print(
            f"Partition strategy: "
            f"{write_config.get('partition_strategy', 'none')}",
            flush=True
        )

        if "partitions" in write_config:

            print(
                f"Partitions: "
                f"{write_config['partitions']}",
                flush=True
            )

        # --------------------------------------------------
        # Write
        # --------------------------------------------------

        print(
            "Starting write...",
            flush=True
        )

        self.writer.write(
            df=df,
            path=target_path,
            file_format=target_format,
            mode=write_mode,
            write_config=write_config
        )

        # --------------------------------------------------
        # Completion
        # --------------------------------------------------

        print(
            f"Pipeline completed successfully: "
            f"layer={layer}, "
            f"table={table}, "
            f"target={target_path}",
            flush=True
        )

