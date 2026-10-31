
import json
import os
import time
from pathlib import Path
from typing import Any

import requests


class AirflowAPIError(RuntimeError):
    """Raised when the Airflow REST API returns an error."""


class AirflowClient:
    """JWT-authenticated client for the Airflow 3 REST API."""

    def __init__(
        self,
        base_url: str | None = None,
        username: str | None = None,
        password: str | None = None,
        password_file: str | None = None,
        timeout: int = 30,
    ):
        self.base_url = (
            base_url or os.getenv(
                "SUPERVISOR_AIRFLOW_BASE_URL",
                "http://localhost:8080",
            )
        ).rstrip("/")

        self.username = username or os.getenv(
            "SUPERVISOR_AIRFLOW_USERNAME", "admin"
        )

        self.password = password or os.getenv(
            "SUPERVISOR_AIRFLOW_PASSWORD"
        )

        self.password_file = password_file or os.getenv(
            "SUPERVISOR_AIRFLOW_PASSWORD_FILE",
            "secrets/airflow_simple_auth_passwords.json",
        )

        self.timeout = timeout
        self.session = requests.Session()
        self._token: str | None = None
        self._token_created_at = 0.0
        self._token_ttl_seconds = 300

    def _get_password(self) -> str:
        """Read the password from the configured secret source."""
        if self.password:
            return self.password

        path = Path(self.password_file)

        if not path.is_file():
            raise AirflowAPIError(
                f"Airflow password file not found: {path}"
            )

        try:
            credentials = json.loads(path.read_text())
            password = credentials.get(self.username)
        except (OSError, json.JSONDecodeError) as exc:
            raise AirflowAPIError(
                "Unable to read Airflow password file."
            ) from exc

        if not isinstance(password, str) or not password:
            raise AirflowAPIError(
                f"No password configured for user '{self.username}'."
            )

        return password

    def authenticate(self, force_refresh: bool = False) -> str:
        """Obtain and cache an Airflow JWT."""
        token_is_valid = (
            self._token is not None
            and time.monotonic() - self._token_created_at
            < self._token_ttl_seconds
        )

        if token_is_valid and not force_refresh:
            return self._token

        response = self.session.post(
            f"{self.base_url}/auth/token",
            json={
                "username": self.username,
                "password": self._get_password(),
            },
            timeout=self.timeout,
        )

        if not response.ok:
            raise AirflowAPIError(
                f"Airflow authentication failed: HTTP "
                f"{response.status_code}"
            )

        try:
            self._token = response.json()["access_token"]
        except (ValueError, KeyError) as exc:
            raise AirflowAPIError(
                "Airflow authentication response did not contain access_token."
            ) from exc

        self._token_created_at = time.monotonic()
        return self._token

    def _request(self, method: str, endpoint: str, **kwargs: Any) -> dict:
        """Send an authenticated request, refreshing JWT once on 401."""
        for attempt in range(2):
            token = self.authenticate(force_refresh=attempt == 1)

            headers = kwargs.pop("headers", {})
            headers["Authorization"] = f"Bearer {token}"
            headers.setdefault("Accept", "application/json")

            response = self.session.request(
                method,
                f"{self.base_url}{endpoint}",
                headers=headers,
                timeout=self.timeout,
                **kwargs,
            )

            if response.status_code == 401 and attempt == 0:
                continue

            if not response.ok:
                raise AirflowAPIError(
                    f"Airflow API request failed: "
                    f"{method} {endpoint} returned HTTP "
                    f"{response.status_code}: {response.text[:500]}"
                )

            if not response.content:
                return {}

            return response.json()

        raise AirflowAPIError("Airflow authentication retry failed.")

    def list_dags(self, limit: int = 100) -> list[dict]:
        """List Airflow DAGs."""
        if not 1 <= limit <= 500:
            raise ValueError("limit must be between 1 and 500.")

        result = self._request(
            "GET",
            "/api/v2/dags",
            params={"limit": limit},
        )
        return result.get("dags", [])

    def get_dag(self, dag_id: str) -> dict:
        """Retrieve details for one DAG."""
        return self._request(
            "GET",
            f"/api/v2/dags/{dag_id}",
        )

    def trigger_dag(self, dag_id: str, conf: dict | None = None, dag_run_id: str | None = None) -> dict:
        """Trigger an Airflow DAG run."""
        payload: dict[str, Any] = {
            "conf": conf or {},
        }

        if dag_run_id:
            payload["dag_run_id"] = dag_run_id

        return self._request(
            "POST",
            f"/api/v2/dags/{dag_id}/dagRuns",
            json=payload,
        )

    def get_dag_run(self, dag_id: str, dag_run_id: str) -> dict:
        """Retrieve the status of a DAG run."""
        return self._request(
            "GET",
            f"/api/v2/dags/{dag_id}/dagRuns/{dag_run_id}",
        )

    def list_dag_runs(self, dag_id: str, limit: int = 10) -> list[dict]:
        """List recent DAG runs."""
        if not 1 <= limit <= 100:
            raise ValueError("limit must be between 1 and 100.")

        result = self._request(
            "GET",
            f"/api/v2/dags/{dag_id}/dagRuns",
            params={"limit": limit},
        )
        return result.get("dag_runs", [])

    def list_task_instances(self, dag_id: str, dag_run_id: str, limit: int = 100, offset: int = 0, state: list[str] | None = None) -> list[dict]:
        """List task instances for a specific DAG run."""
        if not 1 <= limit <= 500:
            raise ValueError("limit must be between 1 and 500.")

        if offset < 0:
            raise ValueError("offset cannot be negative.")

        params: dict[str, Any] = {
            "limit": limit,
            "offset": offset,
        }

        if state:
            params["state"] = state

        result = self._request(
            "GET",
            f"/api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances",
            params=params,
        )

        return result.get("task_instances", [])

    def get_task_logs(self, dag_id: str, dag_run_id: str, task_id: str, try_number: int, max_entries: int = 500, full_content: bool = False, map_index: int = -1) -> dict[str, Any]:
        """Retrieve bounded, structured task logs with pagination."""

        if try_number < 0:
            raise ValueError("try_number cannot be negative.")

        if max_entries < 1:
            raise ValueError("max_entries must be at least 1.")

        endpoint = (
            f"/api/v2/dags/{dag_id}/dagRuns/{dag_run_id}"
            f"/taskInstances/{task_id}/logs/{try_number}"
        )

        entries: list[Any] = []
        continuation_token = None

        while len(entries) < max_entries:
            params: dict[str, Any] = {
                "full_content": full_content,
                "map_index": map_index,
            }

            if continuation_token:
                params["token"] = continuation_token

            result = self._request(
                "GET",
                endpoint,
                params=params,
            )

            page_entries = result.get("content", [])
            remaining = max_entries - len(entries)

            entries.extend(page_entries[:remaining])

            next_token = result.get("continuation_token")

            if not next_token or not page_entries:
                continuation_token = None
                break

            continuation_token = next_token

        return {
            "content": entries,
            "entry_count": len(entries),
            "continuation_token": continuation_token,
            "truncated": continuation_token is not None,
        }
    def list_task_attempts(
        self,
        dag_id: str,
        dag_run_id: str,
        task_id: str,
        map_index: int = -1,
    ) -> list[dict]:
        """Retrieve historical attempts for a task instance."""

        endpoint = (
            f"/api/v2/dags/{dag_id}/dagRuns/{dag_run_id}"
            f"/taskInstances/{task_id}/tries"
        )

        result = self._request(
            "GET",
            endpoint,
            params={"map_index": map_index},
        )

        return result.get("task_instances", [])