from framework.config_loader import ConfigLoader
from framework.path_builder import PathBuilder


CONFIG_ROOT = "/opt/datapilot/config"


def main():

    print("=" * 70)
    print("DataPilot AI - Configuration Framework Test")
    print("=" * 70)

    loader = ConfigLoader(CONFIG_ROOT)

    variables = loader.load_variables()

    bronze_config = loader.load_layer_config(
        "bronze",
        "olist_orders"
    )

    silver_config = loader.load_layer_config(
        "silver",
        "olist_orders"
    )

    gold_config = loader.load_layer_config(
        "gold",
        "olist_order_summary"
    )

    path_builder = PathBuilder(variables)

    raw_path = path_builder.build_raw_file_path(
        bronze_config["source"]["file"]
    )

    bronze_path = path_builder.build_layer_path(
        "bronze",
        bronze_config["table"]["name"]
    )

    silver_path = path_builder.build_layer_path(
        "silver",
        silver_config["table"]["name"]
    )

    gold_path = path_builder.build_layer_path(
        "gold",
        gold_config["table"]["name"]
    )

    print("\nGenerated paths:")
    print(f"RAW    : {raw_path}")
    print(f"BRONZE : {bronze_path}")
    print(f"SILVER : {silver_path}")
    print(f"GOLD   : {gold_path}")

    print("\nConfiguration loaded successfully.")
    print("=" * 70)


if __name__ == "__main__":
    main()