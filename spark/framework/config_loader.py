from pathlib import Path
import yaml


class ConfigLoader:

    def __init__(self, config_root: str):
        self.config_root = Path(config_root)

    def load_yaml(self, file_path: Path) -> dict:
        with open(file_path, "r") as file:
            return yaml.safe_load(file)

    def load_variables(self) -> dict:
        path = self.config_root / "variables.yaml"
        return self.load_yaml(path)

    def load_layer_config(self, layer: str, table: str) -> dict:
        path = self.config_root / layer / f"{table}.yaml"

        if not path.exists():
            raise FileNotFoundError(
                f"Configuration not found: {path}"
            )

        return self.load_yaml(path)