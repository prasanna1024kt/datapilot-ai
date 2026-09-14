from datetime import datetime

from airflow.sdk import DAG
from airflow.providers.apache.spark.operators.spark_submit import (
    SparkSubmitOperator,
)


SPARK_CONNECTION = "spark_default"
SPARK_APPLICATION = "/opt/spark-apps/jobs/run_pipeline.py"
FRAMEWORK_ZIP = "/opt/spark-apps/framework.zip"
CONFIG_ROOT = "/opt/datapilot/config"

SPARK_CONF = {
    "spark.hadoop.fs.s3a.aws.credentials.provider": "software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider",
    "spark.hadoop.fs.s3a.endpoint.region": "us-east-1",
}


SPARK_ENV = {
    "AWS_PROFILE": "datapilot",
    "AWS_DEFAULT_REGION": "us-east-1",
    "AWS_SHARED_CREDENTIALS_FILE": "/opt/spark/.aws/credentials",
    "AWS_CONFIG_FILE": "/opt/spark/.aws/config",
}


with DAG(
    dag_id="datapilot_olist_order_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=[
        "datapilot",
        "olist",
        "spark",
        "bronze",
        "silver",
        "gold",
    ],
) as dag:

    # ========================================================
    # Bronze
    # Raw CSV -> Bronze Parquet
    # ========================================================

    bronze_olist_orders = SparkSubmitOperator(
        task_id="bronze_olist_orders",

        conn_id=SPARK_CONNECTION,

        application=SPARK_APPLICATION,

        name="DataPilot-Bronze-Olist-Orders",

        py_files=FRAMEWORK_ZIP,

        application_args=[
            "--layer",
            "bronze",
            "--table",
            "olist_orders",
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

    silver_olist_orders = SparkSubmitOperator(
        task_id="silver_olist_orders",

        conn_id=SPARK_CONNECTION,

        application=SPARK_APPLICATION,

        name="DataPilot-Silver-Olist-Orders",

        py_files=FRAMEWORK_ZIP,

        application_args=[
            "--layer",
            "silver",
            "--table",
            "olist_orders",
            "--config-root",
            CONFIG_ROOT,
        ],

        conf=SPARK_CONF,

        env_vars=SPARK_ENV,

        verbose=True,
    )


    # ========================================================
    # Gold
    # Silver Parquet -> Gold Aggregation
    # ========================================================

    gold_olist_order_summary = SparkSubmitOperator(
        task_id="gold_olist_order_summary",

        conn_id=SPARK_CONNECTION,

        application=SPARK_APPLICATION,

        name="DataPilot-Gold-Olist-Order-Summary",

        py_files=FRAMEWORK_ZIP,

        application_args=[
            "--layer",
            "gold",
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
    # Pipeline Dependency
    # ========================================================

    bronze_olist_orders >> silver_olist_orders >> gold_olist_order_summary