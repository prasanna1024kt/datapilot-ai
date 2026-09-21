import argparse

import yaml

from pyspark.sql import SparkSession

from framework.dq.engine import DQEngine
import json 

def parse_args():

    parser = argparse.ArgumentParser(
        description="DataPilot AI Data Quality Runner"
    )

    parser.add_argument(
        "--table",
        required=True
    )

    parser.add_argument(
        "--config-root",
        default="/opt/datapilot/config"
    )

    return parser.parse_args()


def load_variables(config_root):

    path = f"{config_root}/variables.yaml"

    with open(path, "r") as file:
        return yaml.safe_load(file)


def load_dq_config(config_root, table):

    path = f"{config_root}/dq/{table}.yaml"

    with open(path, "r") as file:
        return yaml.safe_load(file)


def build_silver_path(variables, table):

    bucket = variables["storage"]["bucket"]
    silver_path = variables["storage"]["layers"]["silver"]

    return f"s3a://{bucket}/{silver_path}/{table}"

def save_dq_results(table, results, overall_status):
    RESULTS_ROOT = "/opt/datapilot/results"

    path = (
        f"{RESULTS_ROOT}/dq/"
        f"{table}.json"
    )

    payload = {
        "dataset": table,
        "status": overall_status,
        "checks": results,
    }

    with open(path, "w") as file:
        json.dump(
            payload,
            file,
            indent=2,
            default=str,
        )

    print("")
    print(f"DQ results saved to: {path}")

def main():

    args = parse_args()

    spark = (
        SparkSession.builder
        .appName(f"DataPilot-DQ-{args.table}")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider"
        )
        .config(
            "spark.hadoop.fs.s3a.endpoint.region",
            "us-east-1"
        )
        .getOrCreate()
    )

    try:

        variables = load_variables(
            args.config_root
        )

        dq_config = load_dq_config(
            args.config_root,
            args.table
        )

        silver_path = build_silver_path(
            variables,
            args.table
        )

        print("")
        print("========================================")
        print(f"DQ CHECK: {args.table}")
        print("========================================")
        print(f"Source: {silver_path}")
        print("")

        df = spark.read.parquet(
            silver_path
        )

        dq_engine = DQEngine(spark)

        results = dq_engine.run(
            df,
            dq_config
        )
        overall_status = ("FAIL" if dq_engine.has_failures(results) else "PASS" )

        save_dq_results(
            table=args.table,
            results=results,
            overall_status=overall_status,
        )

        for result in results:

            print(
                f"{result['check']:<35}"
                f"{result['status']}"
            )

            if result["status"] == "FAIL":

                print(
                    f"  Expected : {result.get('expected')}"
                )

                print(
                    f"  Actual   : {result.get('actual')}"
                )

                if "invalid_count" in result:
                    print(
                        f"  Invalid Count: "
                        f"{result['invalid_count']}"
                    )

                if "sample_invalid_values" in result:
                    print(
                        f"  Sample Invalid Values: "
                        f"{result['sample_invalid_values']}"
            )

        if dq_engine.has_failures(results):

            print("")
            print("========================================")
            print("OVERALL RESULT: FAIL")
            print("========================================")

            raise RuntimeError(
                f"DQ checks failed for {args.table}"
            )

        print("")
        print("========================================")
        print("OVERALL RESULT: PASS")
        print("========================================")

    finally:

        spark.stop()


if __name__ == "__main__":
    main()