from framework.config_loader import ConfigLoader
from framework.path_builder import PathBuilder


CONFIG_ROOT = "/opt/datapilot/config"
DATASET = "olist_order_payments"


def main():
    loader = ConfigLoader(CONFIG_ROOT)
    variables = loader.load_variables()
    config = loader.load_layer_config("quarantine", DATASET)

    quarantine = config["quarantine"]

    if not quarantine["enabled"]:
        raise ValueError("Quarantine is disabled in configuration")

    if quarantine["keys"] != ["order_id", "payment_sequential"]:
        raise ValueError("Unexpected quarantine business keys")

    path_builder = PathBuilder(variables)

    source_path = path_builder.build_layer_path(
        quarantine["source_layer"], DATASET
    )
    target_path = path_builder.build_layer_path(
        quarantine["target_layer"], DATASET
    )

    print(f"Dataset        : {DATASET}")
    print(f"Catalog        : {quarantine['catalog']}")
    print(f"Database       : {quarantine['database']}")
    print(f"Target table   : {quarantine['table']}")
    print(f"Source path    : {source_path}")
    print(f"Target path    : {target_path}")
    print(f"Business keys  : {quarantine['keys']}")
    print("Configuration validation passed.")
    print("No Iceberg table created; no S3 writes performed.")


if __name__ == "__main__":
    main()
