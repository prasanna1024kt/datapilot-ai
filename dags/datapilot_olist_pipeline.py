import os
import re
import subprocess
from pathlib import Path
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG, TriggerRule
from datetime import datetime
from airflow.providers.apache.spark.operators.spark_submit import (
    SparkSubmitOperator,
)


SPARK_CONNECTION = "spark_default"

PIPELINE_APPLICATION = "/opt/spark-apps/jobs/run_pipeline.py"
DQ_APPLICATION = "/opt/spark-apps/jobs/run_dq.py"

# New incremental Iceberg Gold
GOLD_APPLICATION = "/opt/spark-apps/jobs/gold_incremental_iceberg.py"
GOLD_CONFIG = "/opt/datapilot/config/gold_tables.yaml"

# Existing business summary Gold
GOLD_SUMMARY_APPLICATION = "/opt/spark-apps/jobs/run_gold.py"
RCA_APPLICATION = "/opt/datapilot/agents/rca/rca_agent.py"
RCA_RESULTS_ROOT = "/opt/datapilot/results/rca"
FRAMEWORK_ZIP = "/opt/spark-apps/framework.zip"
CONFIG_ROOT = "/opt/datapilot/config"


SPARK_CONF = {
    "spark.hadoop.fs.s3a.aws.credentials.provider":
        "software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider",
    "spark.hadoop.fs.s3a.endpoint.region":
        "us-east-1",
}


SPARK_ENV = {
    "AWS_PROFILE": "datapilot",
    "AWS_DEFAULT_REGION": "us-east-1",
    "AWS_SHARED_CREDENTIALS_FILE":
        "/opt/spark/.aws/credentials",
    "AWS_CONFIG_FILE":
        "/opt/spark/.aws/config",
}


BRONZE_TABLES = [
    "olist_customers",
    "olist_orders",
    "olist_order_items",
    "olist_order_payments",
    "olist_order_reviews",
    "olist_products",
    "olist_sellers",
    "olist_geolocation",
    "olist_product_category_translation",
]


SILVER_TABLES = [
    "olist_customers",
    "olist_orders",
    "olist_order_items",
    "olist_order_payments",
    "olist_order_reviews",
    "olist_products",
    "olist_sellers",
    "olist_geolocation",
    "olist_product_category_translation",
]


DQ_TABLES = [
    "olist_customers",
    "olist_orders",
    "olist_order_items",
    "olist_order_payments",
    "olist_order_reviews",
    "olist_products",
    "olist_sellers",
    "olist_geolocation",
    "olist_product_category_translation",
]


GOLD_TABLES = [
    "olist_customers",
    "olist_geolocation",
    "olist_order_items",
    "olist_order_payments",
    "olist_order_reviews",
    "olist_orders",
    "olist_product_category_translation",
    "olist_products",
    "olist_sellers",
]

def extract_failed_range_check(dq_result: dict) -> dict:
    """
    Extract the failed range check from a DQ result.

    Example:
        range_check:payment_installments
        range: 1 <= value <= 100
    """

    failed_checks = [
        check
        for check in dq_result.get("checks", [])
        if (
            check.get("status") == "FAIL"
            and check.get("check", "").startswith("range_check:")
        )
    ]

    if not failed_checks:
        raise ValueError(
            "No failed range_check found in DQ result."
        )

    if len(failed_checks) > 1:
        raise ValueError(
            "Multiple failed range checks found. "
            "Current RCA task expects one failed range check."
        )

    check = failed_checks[0]

    column = check["check"].split(":", 1)[1]

    range_expression = check.get("range")

    if not range_expression:
        raise ValueError(
            f"Missing range definition for {check['check']}"
        )

    match = re.match(
        r"^\s*(-?\d+(?:\.\d+)?)\s*<=\s*value\s*<=\s*(-?\d+(?:\.\d+)?)\s*$",
        range_expression,
    )

    if not match:
        raise ValueError(
            f"Unable to parse range expression: "
            f"{range_expression}"
        )

    minimum = float(match.group(1))
    maximum = float(match.group(2))

    if minimum.is_integer():
        minimum = int(minimum)

    if maximum.is_integer():
        maximum = int(maximum)

    return {
        "column": column,
        "minimum": minimum,
        "maximum": maximum,
    }



def run_rca(table: str) -> None:
    """
    Run RCA for a failed DQ dataset.

    The failed DQ rule is discovered dynamically
    from the DQ result through MCP.
    """

    from agents.rca.mcp_client import call_tool

    print(f"Starting RCA for dataset: {table}")

    # --------------------------------------------------
    # Get DQ result through MCP
    # --------------------------------------------------

    dq_result = call_tool(
        "get_dq_result",
        {
            "dataset": table,
        },
    )

    print("DQ result retrieved successfully.")

    # --------------------------------------------------
    # Validate DQ status
    # --------------------------------------------------

    if dq_result.get("status") != "FAIL":
        raise ValueError(
            f"RCA invoked for {table}, but DQ status is "
            f"{dq_result.get('status')}"
        )

    # --------------------------------------------------
    # Dynamically identify failed range check
    # --------------------------------------------------

    failed_rule = extract_failed_range_check(
        dq_result
    )

    column = failed_rule["column"]
    minimum = failed_rule["minimum"]
    maximum = failed_rule["maximum"]

    print(
        "Detected failed DQ rule: "
        f"column={column}, "
        f"minimum={minimum}, "
        f"maximum={maximum}"
    )

    # --------------------------------------------------
    # RCA output
    # --------------------------------------------------

    output_dir = Path(RCA_RESULTS_ROOT)

    output_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    output_file = (
        output_dir
        / f"{table}.json"
    )

    # --------------------------------------------------
    # Environment
    # --------------------------------------------------

    env = os.environ.copy()

    env["MCP_SERVER_URL"] = (
        "http://mcp-server:8000/mcp"
    )

    env["PYTHONPATH"] = (
        "/opt/datapilot"
        + os.pathsep
        + env.get("PYTHONPATH", "")
    )

    # --------------------------------------------------
    # Run RCA Agent
    # --------------------------------------------------

    command = [
        "python3",
        RCA_APPLICATION,

        "--dataset",
        table,

        "--column",
        column,

        "--minimum",
        str(minimum),

        "--maximum",
        str(maximum),

        "--output",
        str(output_file),
    ]

    print(
        "Running RCA Agent:\n"
        + " ".join(command)
    )

    process = subprocess.run(
        command,
        env=env,
        cwd="/opt/datapilot",
        capture_output=True,
        text=True,
        check=False,
    )

    print("RCA STDOUT:")
    print(process.stdout)

    if process.returncode != 0:
        print("RCA STDERR:")
        print(process.stderr)

        raise RuntimeError(
            f"RCA Agent failed for {table} "
            f"with exit code {process.returncode}"
        )

    if not output_file.exists():
        raise RuntimeError(
            f"RCA Agent completed but output was not "
            f"created: {output_file}"
        )

    print(
        f"RCA completed successfully: {output_file}"
    )

with DAG(
    dag_id="datapilot_olist_order_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_tasks=1,
    max_active_runs=1,
    tags=[
        "datapilot",
        "olist",
        "spark",
        "bronze",
        "silver",
        "dq",
        "gold",
    ],
) as dag:

    # ========================================================
    # Bronze
    # Raw CSV -> Bronze Parquet
    # ========================================================

    bronze_tasks = {}

    for table in BRONZE_TABLES:

        bronze_tasks[table] = SparkSubmitOperator(
            task_id=f"bronze_{table}",

            conn_id=SPARK_CONNECTION,

            application=PIPELINE_APPLICATION,

            name=f"DataPilot-Bronze-{table}",

            py_files=FRAMEWORK_ZIP,

            application_args=[
                "--layer",
                "bronze",
                "--table",
                table,
                "--config-root",
                CONFIG_ROOT,
            ],

            conf=SPARK_CONF,

            env_vars=SPARK_ENV,

            verbose=True,
        )


    # ========================================================
    # Silver
    # Bronze Parquet -> Silver Parquet
    # ========================================================

    silver_tasks = {}

    for table in SILVER_TABLES:

        silver_tasks[table] = SparkSubmitOperator(
            task_id=f"silver_{table}",

            conn_id=SPARK_CONNECTION,

            application=PIPELINE_APPLICATION,

            name=f"DataPilot-Silver-{table}",

            py_files=FRAMEWORK_ZIP,

            application_args=[
                "--layer",
                "silver",
                "--table",
                table,
                "--config-root",
                CONFIG_ROOT,
            ],

            conf=SPARK_CONF,

            env_vars=SPARK_ENV,

            verbose=True,
        )


    # ========================================================
    # DQ
    # Silver Parquet -> Data Quality Checks
    # ========================================================

    dq_tasks = {}

    for table in DQ_TABLES:

        dq_tasks[table] = SparkSubmitOperator(
            task_id=f"dq_{table}",

            conn_id=SPARK_CONNECTION,

            application=DQ_APPLICATION,

            name=f"DataPilot-DQ-{table}",

            py_files=FRAMEWORK_ZIP,

            application_args=[
                "--table",
                table,
                "--config-root",
                CONFIG_ROOT,
            ],

            conf=SPARK_CONF,

            env_vars=SPARK_ENV,

            verbose=True,
        )

    rca_tasks = {}

    for table in DQ_TABLES:

        rca_tasks[table] = PythonOperator(
            task_id=f"rca_{table}",

            python_callable=run_rca,

            op_kwargs={
                "table": table,
            },

            trigger_rule=TriggerRule.ONE_FAILED,
        )
    # ========================================================
    # Gold
    # Silver Parquet -> Gold Iceberg
    # ========================================================

    gold_tasks = {}

    for table in GOLD_TABLES:

        gold_tasks[table] = SparkSubmitOperator(
            task_id=f"gold_{table}",

            conn_id=SPARK_CONNECTION,

            application=GOLD_APPLICATION,

            name=f"DataPilot-Gold-{table}",

            application_args=[
                "--dataset",
                table,
                "--config",
                GOLD_CONFIG,
            ],

            conf=SPARK_CONF,

            env_vars=SPARK_ENV,

            verbose=True,
        )


    # ========================================================
    # Gold Business Summary
    # Gold Iceberg -> Order Summary
    # ========================================================

    gold_olist_order_summary = SparkSubmitOperator(
        task_id="gold_olist_order_summary",

        conn_id=SPARK_CONNECTION,

        application=GOLD_SUMMARY_APPLICATION,

        name="DataPilot-Gold-Olist-Order-Summary",

        py_files=FRAMEWORK_ZIP,

        application_args=[
            "--table",
            "olist_order_summary",
            "--config-root",
            CONFIG_ROOT,
        ],

        conf=SPARK_CONF,

        env_vars=SPARK_ENV,

        verbose=True,
    )


    # ========================================================
    # Dependencies
    # ========================================================

    # Bronze -> Silver
    for table in BRONZE_TABLES:
        bronze_tasks[table] >> silver_tasks[table]


    # Silver -> DQ
    for table in SILVER_TABLES:
        silver_tasks[table] >> dq_tasks[table]


    # DQ -> Gold
    #
    # Each Gold table is gated by its own DQ result.
    #
    # Example:
    #
    # olist_orders:
    # Silver -> DQ -> Gold
    #
    # olist_order_payments:
    # Silver -> DQ FAIL -> Gold BLOCKED
    #
    # ========================================================
# DQ -> Gold / RCA
# ========================================================

    for table in DQ_TABLES:

        # DQ PASS -> Gold
        dq_tasks[table] >> gold_tasks[table]

        # DQ FAIL -> RCA
        dq_tasks[table] >> rca_tasks[table]


    # All Gold tables -> Business Summary
    for table in GOLD_TABLES:
        gold_tasks[table] >> gold_olist_order_summary