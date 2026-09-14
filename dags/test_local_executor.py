from datetime import datetime

from airflow.sdk import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="test_local_executor",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    test = BashOperator(
        task_id="test",
        bash_command="echo 'LOCAL EXECUTOR WORKS' && python --version",
    )