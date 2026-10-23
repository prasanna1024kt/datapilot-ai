import subprocess
import sys
from datetime import datetime, timezone

from airflow import DAG
from airflow.operators.python import PythonOperator


EXECUTOR = "/opt/datapilot/agents/remediation/executor.py"
PROJECT_ROOT = "/opt/datapilot"


def run_remediation_dry_run():
    command = [
        sys.executable,
        EXECUTOR,
        "--dataset", "olist_order_payments",
        "--action", "QUARANTINE_RECORDS",
        "--mode", "DRY_RUN",
    ]

    completed = subprocess.run(
        command,
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        timeout=600,
        check=False,
    )

    if completed.stdout:
        print(completed.stdout)

    if completed.stderr:
        print(completed.stderr)

    if completed.returncode != 0:
        raise RuntimeError(
            "Remediation DRY_RUN failed. "
            "Inspect the executor output and audit history before retrying."
        )


with DAG(
    dag_id="datapilot_remediation_dry_run",
    description="Manually validate approved Olist payment remediation without quarantine writes",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "datapilot",
        "retries": 0,
    },
    tags=["datapilot", "remediation", "dry-run"],
) as dag:

    remediation_dry_run = PythonOperator(
        task_id="validate_and_run_quarantine_dry_run",
        python_callable=run_remediation_dry_run,
    )
