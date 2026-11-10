
from copilot.integrations.airflow_service import AirflowService


def main():
    service = AirflowService()

    print("\n--- DAG discovery ---")
    dags = service.list_dags(limit=20)
    print(dags)

    dag_id = "datapilot_olist_order_pipeline"

    print("\n--- Recent DAG runs ---")
    runs = service.list_runs(dag_id, limit=5)
    print(runs)

    if runs["runs"]:
        run_id = runs["runs"][0]["dag_run_id"]

        print("\n--- DAG run details ---")
        details = service.get_run_details(dag_id, run_id)
        print(details)


if __name__ == "__main__":
    main()
