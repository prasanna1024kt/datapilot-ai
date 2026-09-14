class PathBuilder:

    def __init__(self, variables: dict):
        storage = variables["storage"]

        self.bucket = storage["bucket"]
        self.layers = storage["layers"]

    def build_raw_file_path(self, file_name: str) -> str:
        return (
            f"s3a://{self.bucket}/"
            f"{self.layers['raw']}/"
            f"{file_name}"
        )

    def build_layer_path(
        self,
        layer: str,
        table: str
    ) -> str:

        if layer not in self.layers:
            raise ValueError(
                f"Unsupported layer: {layer}"
            )

        return (
            f"s3a://{self.bucket}/"
            f"{self.layers[layer]}/"
            f"{table}"
        )