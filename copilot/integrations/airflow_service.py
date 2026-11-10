
import re
from typing import Any
from datetime import datetime, timezone
from agents.supervisor.airflow_client import AirflowClient


class AirflowService:
    """Read-only operational interface to Apache Airflow."""

    DAG_ID_PATTERN = re.compile(r"^[A-Za-z0-9_.-]{1,250}$")
    RUN_ID_PATTERN = re.compile(r"^[A-Za-z0-9_.:+-]{1,250}$")

    def __init__(self, client: AirflowClient | None = None):
        self.client = client or AirflowClient()

    @classmethod
    def _validate_dag_id(cls, dag_id: str) -> None:
        if not cls.DAG_ID_PATTERN.fullmatch(dag_id):
            raise ValueError("Invalid Airflow DAG ID.")

    @classmethod
    def _validate_run_id(cls, run_id: str) -> None:
        if not cls.RUN_ID_PATTERN.fullmatch(run_id):
            raise ValueError("Invalid Airflow DAG run ID.")

    def list_dags(self, limit: int = 100) -> dict[str, Any]:
        if not 1 <= limit <= 100:
            raise ValueError("limit must be between 1 and 100.")

        dags = self.client.list_dags(limit=limit)

        return {
            "count": len(dags),
            "dags": [
                {
                    "dag_id": dag.get("dag_id"),
                    "description": dag.get("description"),
                    "is_paused": dag.get("is_paused"),
                    "is_active": dag.get("is_active"),
                }
                for dag in dags
            ],
        }
    
    @staticmethod
    def _run_sort_timestamp(run: dict[str, Any]) -> float:
        # Prefer actual execution start time.
        timestamp = run.get("start_date") or run.get("logical_date")

        if timestamp:
            try:
                return datetime.fromisoformat(
                    timestamp.replace("Z", "+00:00")
                ).timestamp()
            except (ValueError, TypeError):
                pass

        return 0.0


    def list_runs(self, dag_id: str, limit: int = 10) -> dict[str, Any]:
        self._validate_dag_id(dag_id)

        if not 1 <= limit <= 50:
            raise ValueError("limit must be between 1 and 50.")
        runs = self.client.list_dag_runs(dag_id=dag_id, limit=100)

        runs = sorted(runs, key=self._run_sort_timestamp, reverse=True)[:limit]
        return {
            "dag_id": dag_id,
            "count": len(runs),
            "runs": [
                {
                    "dag_run_id": run.get("dag_run_id"),
                    "state": run.get("state"),
                    "logical_date": run.get("logical_date"),
                    "start_date": run.get("start_date"),
                    "end_date": run.get("end_date"),
                }
                for run in runs
            ],
        }

    def get_run_details(
        self, dag_id: str, dag_run_id: str
    ) -> dict[str, Any]:
        self._validate_dag_id(dag_id)
        self._validate_run_id(dag_run_id)

        run = self.client.get_dag_run(
            dag_id=dag_id,
            dag_run_id=dag_run_id,
        )

        tasks = self.client.list_task_instances(
            dag_id=dag_id,
            dag_run_id=dag_run_id,
            limit=500,
        )

        task_summaries = [
            {
                "task_id": task.get("task_id"),
                "state": task.get("state"),
                "try_number": task.get("try_number"),
                "start_date": task.get("start_date"),
                "end_date": task.get("end_date"),
            }
            for task in tasks
        ]

        state_counts: dict[str, int] = {}
        for task in task_summaries:
            state = task.get("state") or "unknown"
            state_counts[state] = state_counts.get(state, 0) + 1

        return {
            "dag_id": dag_id,
            "dag_run_id": dag_run_id,
            "run": {
                "state": run.get("state"),
                "logical_date": run.get("logical_date"),
                "start_date": run.get("start_date"),
                "end_date": run.get("end_date"),
            },
            "task_count": len(task_summaries),
            "task_state_counts": state_counts,
            "tasks": task_summaries,
        }
