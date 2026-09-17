from datetime import datetime

from airflow.sdk import DAG
from airflow.providers.apache.spark.operators.spark_submit import (
    SparkSubmitOperator,
)


SPARK_CONNECTION = "spark_default"

PIPELINE_APPLICATION = "/opt/spark-apps/jobs/run_pipeline.py"
DQ_APPLICATION = "/opt/spark-apps/jobs/run_dq.py"
GOLD_APPLICATION = "/opt/spark-apps/jobs/run_gold.py"

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


    # ========================================================
    # Gold
    # Silver Parquet -> Gold Business Dataset
    # ========================================================

    gold_olist_order_summary = SparkSubmitOperator(
        task_id="gold_olist_order_summary",

        conn_id=SPARK_CONNECTION,

        application=GOLD_APPLICATION,

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

    # All Bronze tasks must complete before Silver starts
    for table in BRONZE_TABLES:
        bronze_tasks[table] >> silver_tasks[table]


    # All Silver tasks must complete before their DQ checks
    for table in SILVER_TABLES:
        silver_tasks[table] >> dq_tasks[table]


    # Gold waits for all DQ checks
    for table in DQ_TABLES:
        dq_tasks[table] >> gold_olist_order_summary