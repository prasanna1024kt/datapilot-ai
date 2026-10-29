from pathlib import Path
import shutil
import sys

DAG_PATH = Path("dags/datapilot_remediation_dry_run.py")
if not DAG_PATH.is_file():
    sys.exit(f"ERROR: {DAG_PATH} not found. Run from the DataPilot AI project root.")

source = DAG_PATH.read_text()
if "refresh_gold_olist_order_payments" in source or "def should_refresh_gold_after_apply" in source:
    sys.exit("STOP: Gold workflow code already exists. No changes made.")

tail_anchor = '''    execute_and_summarize = PythonOperator(
        task_id="execute_dry_run_and_write_summary",
        python_callable=run_remediation_dry_run,
    )'''
if tail_anchor not in source or not source.rstrip().endswith(tail_anchor):
    sys.exit("ERROR: Expected DAG task at end of file was not found. No changes made.")

imports_anchor = 'from airflow.providers.standard.operators.python import PythonOperator\n'
if imports_anchor not in source:
    sys.exit("ERROR: PythonOperator import not found. No changes made.")
source = source.replace(
    imports_anchor,
    imports_anchor
    + 'from airflow.providers.standard.operators.python import ShortCircuitOperator\n'
    + 'from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator\n'
    + 'from airflow.sdk import TriggerRule\n',
    1,
)

audit_line = 'AUDIT_DIR = Path("/opt/spark-apps/results/quarantine")'
if audit_line not in source:
    sys.exit("ERROR: AUDIT_DIR constant not found. No changes made.")
constants = '''
# Gold refresh applications (reusing datapilot_olist_pipeline.py configuration).
SPARK_CONNECTION = "spark_default"
GOLD_APPLICATION = "/opt/spark-apps/jobs/gold_incremental_iceberg.py"
GOLD_CONFIG = "/opt/datapilot/config/gold_tables.yaml"
GOLD_SUMMARY_APPLICATION = "/opt/spark-apps/jobs/run_gold.py"
FRAMEWORK_ZIP = "/opt/spark-apps/framework.zip"
CONFIG_ROOT = "/opt/datapilot/config"

SPARK_CONF = {
    "spark.hadoop.fs.s3a.aws.credentials.provider":
        "software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider",
    "spark.hadoop.fs.s3a.endpoint.region": "us-east-1",
}
SPARK_ENV = {
    "AWS_PROFILE": "datapilot",
    "AWS_DEFAULT_REGION": "us-east-1",
    "AWS_SHARED_CREDENTIALS_FILE": "/opt/spark/.aws/credentials",
    "AWS_CONFIG_FILE": "/opt/spark/.aws/config",
}
'''
source = source.replace(audit_line, audit_line + constants, 1)

helper = '''

def should_refresh_gold_after_apply(**context):
    'Allow Gold writes only after a successful remediation task in APPLY mode.'
    result = context["ti"].xcom_pull(
        task_ids="execute_dry_run_and_write_summary"
    )
    if not isinstance(result, dict):
        raise RuntimeError("Remediation task did not return execution metadata.")
    mode = str(result.get("mode", "")).upper()
    if mode == "DRY_RUN":
        logger.info("DRY_RUN selected; skipping all Gold refresh tasks.")
        return False
    if mode != "APPLY":
        raise RuntimeError(f"Unexpected remediation mode in XCom: {mode!r}")
    return True


def update_postgres_summary_after_gold_refresh(**context):
    'Record Gold outcomes while preserving existing remediation audit JSON.'
    sys.path.insert(0, str(PROJECT_ROOT))
    from agents.remediation.summary_store import upsert_execution_summary

    ti = context["ti"]
    dag_run = context["dag_run"]
    result = ti.xcom_pull(task_ids="execute_dry_run_and_write_summary")
    if not isinstance(result, dict) or not result.get("execution_id"):
        raise RuntimeError("Cannot update summary: execution_id XCom is missing.")

    payments_ti = dag_run.get_task_instance("refresh_gold_olist_order_payments")
    summary_ti = dag_run.get_task_instance("refresh_gold_olist_order_summary")
    payments_state = str(payments_ti.state or "unknown").lower()
    summary_state = str(summary_ti.state or "unknown").lower()

    gold_success = payments_state == "success" and summary_state == "success"
    details = {
        "gold_refresh": {
            "payments_task_id": "refresh_gold_olist_order_payments",
            "payments_task_status": payments_state,
            "payments_output_table": "glue_catalog.datapilot.olist_order_payments",
            "order_summary_task_id": "refresh_gold_olist_order_summary",
            "order_summary_task_status": summary_state,
            "order_summary_output_table": "glue_catalog.datapilot.olist_order_summary",
            "gold_refresh_status": "SUCCESS" if gold_success else "FAILED",
            "updated_at": utc_now(),
        }
    }
    summary = {
        "execution_id": result["execution_id"],
        "dag_id": context["dag"].dag_id,
        "dag_run_id": dag_run.run_id,
        "dataset": DATASET,
        "action": ACTION,
        "execution_mode": "APPLY",
        "execution_status": "SUCCESS" if gold_success else "FAILED",
        "error_type": None if gold_success else "GoldRefreshFailed",
        "error_message": None if gold_success else (
            f"Gold refresh task states: payments={payments_state}, "
            f"order_summary={summary_state}"
        ),
        "audit_details": details,
    }
    upsert_execution_summary(summary)
    if not gold_success:
        raise RuntimeError(
            "Gold refresh failed; inspect audit_details.gold_refresh "
            f"(payments={payments_state}, order_summary={summary_state})."
        )
'''

dag_marker = '\n\nwith DAG('
if dag_marker not in source:
    sys.exit("ERROR: DAG declaration not found. No changes made.")
source = source.replace(dag_marker, helper + dag_marker, 1)

tasks = '''

    continue_to_gold_only_after_apply = ShortCircuitOperator(
        task_id="continue_to_gold_only_after_apply",
        python_callable=should_refresh_gold_after_apply,
    )

    refresh_gold_olist_order_payments = SparkSubmitOperator(
        task_id="refresh_gold_olist_order_payments",
        conn_id=SPARK_CONNECTION,
        application=GOLD_APPLICATION,
        name="DataPilot-Remediation-Gold-Olist-Order-Payments",
        application_args=[
            "--dataset", "olist_order_payments",
            "--config", GOLD_CONFIG,
        ],
        conf=SPARK_CONF,
        env_vars=SPARK_ENV,
        verbose=True,
    )

    refresh_gold_olist_order_summary = SparkSubmitOperator(
        task_id="refresh_gold_olist_order_summary",
        conn_id=SPARK_CONNECTION,
        application=GOLD_SUMMARY_APPLICATION,
        name="DataPilot-Remediation-Gold-Olist-Order-Summary",
        py_files=FRAMEWORK_ZIP,
        application_args=[
            "--table", "olist_order_summary",
            "--config-root", CONFIG_ROOT,
        ],
        conf=SPARK_CONF,
        env_vars=SPARK_ENV,
        verbose=True,
    )

    update_gold_refresh_summary = PythonOperator(
        task_id="update_postgres_summary_after_gold_refresh",
        python_callable=update_postgres_summary_after_gold_refresh,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    execute_and_summarize >> continue_to_gold_only_after_apply
    continue_to_gold_only_after_apply >> refresh_gold_olist_order_payments
    refresh_gold_olist_order_payments >> refresh_gold_olist_order_summary
    refresh_gold_olist_order_payments >> update_gold_refresh_summary
    refresh_gold_olist_order_summary >> update_gold_refresh_summary
'''

source = source.rstrip() + tasks + "\n"
compile(source, str(DAG_PATH), "exec")

backup = DAG_PATH.with_suffix(DAG_PATH.suffix + ".bak-gold-workflow")
if backup.exists():
    sys.exit(f"ERROR: Backup already exists at {backup}; no changes made.")
shutil.copy2(DAG_PATH, backup)
DAG_PATH.write_text(source)
print(f"Patched: {DAG_PATH}")
print(f"Backup:  {backup}")
print("Python syntax check: PASSED")
print("No DAG run was triggered.")
