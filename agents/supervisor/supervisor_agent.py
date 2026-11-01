
import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from agents.supervisor.airflow_client import AirflowClient
import re
import subprocess
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_DQ_PATH = PROJECT_ROOT / "results/dq/olist_order_payments.json"
DEFAULT_OUTPUT_PATH = PROJECT_ROOT / "results/supervisor/assessment.json"
DEFAULT_DAG_ID = "datapilot_olist_order_pipeline"


class SupervisorError(RuntimeError):
    """Raised when Supervisor assessment cannot be completed."""


def load_dq_result(path: Path) -> dict[str, Any]:
    """Load an existing DataPilot DQ result."""
    if not path.is_file():
        raise SupervisorError(f"DQ result not found: {path}")

    try:
        result = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError) as exc:
        raise SupervisorError("Unable to read DQ result JSON.") from exc

    if not isinstance(result, dict):
        raise SupervisorError("DQ result must be a JSON object.")

    required = {"dataset", "status", "checks"}
    missing = required - result.keys()

    if missing:
        raise SupervisorError(
            f"DQ result is missing required fields: {sorted(missing)}"
        )

    if not isinstance(result["checks"], list):
        raise SupervisorError("DQ checks must be a list.")

    return result


def _get_current_run_task_instances(dag_run: Any) -> list[dict[str, Any]]:
    """Retrieve task instances using the Airflow 3 REST API."""

    airflow_client = AirflowClient()

    return airflow_client.list_task_instances(
        dag_id=dag_run.dag_id,
        dag_run_id=dag_run.run_id,
        limit=100,
    )


def _parse_airflow_timestamp(value: str | None) -> float | None:
    """Convert an Airflow REST API timestamp into a Unix timestamp."""

    if not value:
        return None

    try:
        parsed = datetime.fromisoformat(
            value.replace("Z", "+00:00")
        )

        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)

        return parsed.timestamp()

    except (TypeError, ValueError):
        return None


def collect_current_run_tasks(dag_run: Any) -> dict[str, Any]:
    """Collect task states from the current Airflow DAG run."""

    task_instances = _get_current_run_task_instances(dag_run)

    tasks = []

    for ti in task_instances:
        tasks.append({
            "task_id": ti.get("task_id"),
            "state": ti.get("state"),
            "try_number": ti.get("try_number"),
            "start_date": ti.get("start_date"),
            "end_date": ti.get("end_date"),
        })

    state_counts = {}

    for task in tasks:
        state = task["state"] or "unknown"
        state_counts[state] = state_counts.get(state, 0) + 1

    return {
        "dag_id": dag_run.dag_id,
        "dag_run_id": dag_run.run_id,
        "dag_run_state": dag_run.state,
        "task_count": len(tasks),
        "state_counts": state_counts,
        "tasks": tasks,
    }


def collect_dag_run_evidence(
    dag_run: Any,
    dq_tables: list[str],
    results_root: Path,
) -> dict[str, Any]:
    """Collect run-aware DQ and RCA evidence."""

    run_id = dag_run.run_id

    dq_results = {}
    rca_results = {}

    dq_root = results_root / "dq"
    rca_root = results_root / "rca" / run_id

    task_instances = _get_current_run_task_instances(dag_run)

    task_map = {
        ti.get("task_id"): ti
        for ti in task_instances
    }

    # --------------------------------------------------
    # Collect DQ results
    # --------------------------------------------------

    for dataset in dq_tables:

        dq_task_id = f"dq_{dataset}"
        dq_ti = task_map.get(dq_task_id)

        dq_path = dq_root / f"{dataset}.json"

        if dq_ti is None:
            dq_results[dataset] = {
                "status": "TASK_INSTANCE_MISSING",
            }
            continue

        dq_task_state = dq_ti.get("state")

        if not dq_path.is_file():
            dq_results[dataset] = {
                "status": "MISSING",
                "dq_task_state": dq_task_state,
                "error": "DQ result file not found.",
            }
            continue

        task_start = _parse_airflow_timestamp(
            dq_ti.get("start_date")
        )

        if task_start is None:
            dq_results[dataset] = {
                "status": "UNKNOWN_TASK_START_TIME",
                "dq_task_state": dq_task_state,
            }
            continue

        # Reject reports older than the current task execution
        report_mtime = dq_path.stat().st_mtime

        if report_mtime + 1 < task_start:
            dq_results[dataset] = {
                "status": "STALE",
                "dq_task_state": dq_task_state,
                "error": "DQ report predates the current task execution.",
            }
            continue

        try:
            dq_result = load_dq_result(dq_path)
            dq_assessment = assess_dq(dq_result)

            dq_results[dataset] = {
                **dq_assessment,
                "dq_task_state": dq_task_state,
                "report_path": str(dq_path),
                "evidence_status": "CURRENT_RUN_CANDIDATE",
            }

        except Exception as exc:
            dq_results[dataset] = {
                "status": "READ_FAILED",
                "dq_task_state": dq_task_state,
                "error": str(exc),
            }

    # --------------------------------------------------
    # Collect current-run RCA results
    # --------------------------------------------------

    for dataset in dq_tables:

        rca_task_id = f"rca_{dataset}"
        rca_ti = task_map.get(rca_task_id)

        if rca_ti is None:
            rca_results[dataset] = {
                "status": "TASK_INSTANCE_MISSING",
            }
            continue

        rca_task_state = rca_ti.get("state")

        if rca_task_state != "success":
            rca_results[dataset] = {
                "status": "NOT_SUCCESSFUL",
                "rca_task_state": rca_task_state,
            }
            continue

        rca_path = rca_root / f"{dataset}.json"

        if not rca_path.is_file():
            rca_results[dataset] = {
                "status": "REPORT_MISSING",
                "rca_task_state": rca_task_state,
            }
            continue

        try:
            rca_results[dataset] = {
                "status": "SUCCESS",
                "rca_task_state": rca_task_state,
                "report_path": str(rca_path),
                "result": json.loads(rca_path.read_text()),
            }

        except (OSError, json.JSONDecodeError) as exc:
            rca_results[dataset] = {
                "status": "READ_FAILED",
                "error": str(exc),
            }

    return {
        "dag_id": dag_run.dag_id,
        "dag_run_id": run_id,
        "dq_results": dq_results,
        "rca_results": rca_results,
    }


def assess_dq(dq_result: dict[str, Any]) -> dict[str, Any]:
    """Summarize passed and failed DQ checks."""
    checks = dq_result["checks"]

    failed_checks = [
        check for check in checks
        if str(check.get("status", "")).upper() == "FAIL"
    ]

    passed_checks = [
        check for check in checks
        if str(check.get("status", "")).upper() == "PASS"
    ]

    unknown_checks = [
        check for check in checks
        if str(check.get("status", "")).upper() not in {"PASS", "FAIL"}
    ]

    dq_status = str(dq_result["status"]).upper()

    if dq_status not in {"PASS", "FAIL"} or unknown_checks:
        decision = "REVIEW_REQUIRED"
    elif dq_status == "FAIL" or failed_checks:
        decision = "INVOKE_RCA"
    else:
        decision = "DQ_PASSED"

    return {
        "dataset": dq_result["dataset"],
        "dq_status": dq_status,
        "total_checks": len(checks),
        "passed_count": len(passed_checks),
        "failed_count": len(failed_checks),
        "unknown_count": len(unknown_checks),
        "failed_checks": failed_checks,
        "unknown_checks": unknown_checks,
        "decision": decision,
    }


def get_pipeline_status(
    airflow_client: AirflowClient,
    dag_id: str,
) -> dict[str, Any]:
    """Retrieve recent Airflow runs without triggering a DAG."""
    runs = airflow_client.list_dag_runs(dag_id, limit=5)

    if not runs:
        return {
            "dag_id": dag_id,
            "state": "NO_RUNS",
            "latest_run": None,
        }

    # Airflow returns recent runs. Sort by logical date for clarity.
    latest = max(
        runs,
        key=lambda run: (
            run.get("logical_date")
            or run.get("start_date")
            or ""
        ),
    )

    return {
        "dag_id": dag_id,
        "state": latest.get("state", "UNKNOWN"),
        "latest_run": {
            "dag_run_id": latest.get("dag_run_id"),
            "state": latest.get("state"),
            "logical_date": latest.get("logical_date"),
            "start_date": latest.get("start_date"),
            "end_date": latest.get("end_date"),
        },
    }

def get_task_failure_diagnostics(airflow_client: AirflowClient, dag_id: str, dag_run_id: str) -> dict:
    """Collect task failure, attempt, and log evidence without modifying Airflow."""

    task_instances = airflow_client.list_task_instances(
        dag_id,
        dag_run_id,
        limit=100,
    )

    failed_tasks = []

    for task in task_instances:
        state = task.get("state")

        if state not in {"failed", "upstream_failed"}:
            continue

        task_id = task.get("task_id")

        task_info = {
            "task_id": task_id,
            "state": state,
            "operator": task.get("operator"),
            "try_number": task.get("try_number"),
            "start_date": task.get("start_date"),
            "end_date": task.get("end_date"),
            "attempts": [],
            "log_evidence": None,
        }

        if state == "failed":
            attempts = airflow_client.list_task_attempts(
                dag_id,
                dag_run_id,
                task_id,
            )

            task_info["attempts"] = [
                {
                    "try_number": attempt.get("try_number"),
                    "state": attempt.get("state"),
                    "operator": attempt.get("operator"),
                    "hostname": attempt.get("hostname"),
                    "end_date": attempt.get("end_date"),
                }
                for attempt in attempts
            ]

            failed_attempts = [
                attempt
                for attempt in attempts
                if attempt.get("state") == "failed"
            ]

            if failed_attempts:
                latest_attempt = max(
                    failed_attempts,
                    key=lambda item: item.get("try_number", 0),
                )

                try:
                    logs = airflow_client.get_task_logs(
                        dag_id,
                        dag_run_id,
                        task_id,
                        latest_attempt.get("try_number"),
                        full_content=True,
                    )

                    log_entries = logs.get("content", [])

                    failure_indicators = (
                        "traceback",
                        "exception",
                        "error",
                        "failed",
                        "caused by:",
                    )

                    useful_entries = [
                        entry
                        for entry in log_entries
                        if any(
                            indicator in str(entry.get("event", "")).lower()
                            for indicator in failure_indicators
                        )
                    ]

                    task_info["log_evidence"] = {
                        "status": (
                            "AVAILABLE"
                            if useful_entries
                            else "UNKNOWN_INSUFFICIENT_LOG_EVIDENCE"
                        ),
                        "entry_count": len(log_entries),
                        "useful_entry_count": len(useful_entries),
                        "entries": log_entries,
                    }

                except Exception as exc:
                    task_info["log_evidence"] = {
                        "status": "LOG_RETRIEVAL_FAILED",
                        "error": str(exc),
                    }

        failed_tasks.append(task_info)

    has_failed_task = any(
        task["state"] == "failed"
        for task in failed_tasks
    )

    has_log_evidence = any(
        task.get("log_evidence", {}).get("status") == "AVAILABLE"
        for task in failed_tasks
        if task.get("log_evidence")
    )

    return {
        "status": (
            "ERROR_EVIDENCE_AVAILABLE"
            if has_log_evidence
            else "UNKNOWN_INSUFFICIENT_LOG_EVIDENCE"
            if has_failed_task
            else "NO_FAILED_TASKS"
        ),
        "failed_task_count": len(failed_tasks),
        "tasks": failed_tasks,
    }

def make_decision(dq_assessment: dict[str, Any], pipeline_assessment: dict[str, Any],) -> dict[str, Any]:
    """Evaluate DQ and pipeline status as independent signals."""

    dq_decision = dq_assessment["decision"]
    dq_status = str(dq_assessment["dq_status"]).upper()
    pipeline_state = str(pipeline_assessment["state"]).lower()

    # Independent pipeline assessment
    if pipeline_state in {"queued", "running"}:
        pipeline_decision = "PIPELINE_IN_PROGRESS"
    elif pipeline_state == "failed":
        pipeline_decision = "INVESTIGATE_PIPELINE_FAILURE"
    elif pipeline_state == "success":
        pipeline_decision = "PIPELINE_SUCCESS"
    else:
        pipeline_decision = "PIPELINE_STATUS_UNKNOWN"

    # Independent DQ assessment
    if dq_decision == "INVOKE_RCA":
        dq_action = "INVOKE_RCA"
    elif dq_decision == "REVIEW_REQUIRED":
        dq_action = "REVIEW_DQ_EVIDENCE"
    elif dq_status == "PASS":
        dq_action = "DQ_PASSED"
    else:
        dq_action = "REVIEW_DQ_EVIDENCE"

    # Combined Supervisor decision
    if dq_action == "REVIEW_DQ_EVIDENCE":
        decision = "REVIEW_REQUIRED"

    elif dq_action == "INVOKE_RCA" and pipeline_state == "failed":
        decision = "INVOKE_RCA_AND_INVESTIGATE_PIPELINE"

    elif dq_action == "INVOKE_RCA" and pipeline_state in {
        "queued", "running"
    }:
        decision = "PIPELINE_IN_PROGRESS_WITH_DQ_FAILURE"

    elif dq_action == "INVOKE_RCA":
        decision = "INVOKE_RCA"

    elif pipeline_state == "failed":
        decision = "INVESTIGATE_PIPELINE_FAILURE"

    elif pipeline_state in {"queued", "running"}:
        decision = "PIPELINE_IN_PROGRESS"

    elif pipeline_state == "success":
        decision = "PIPELINE_HEALTHY"

    else:
        decision = "REVIEW_REQUIRED"

    # Recommended actions
    next_actions = []

    if dq_action == "INVOKE_RCA":
        next_actions.append("INVOKE_EXISTING_RCA_AGENT")

    if pipeline_decision == "INVESTIGATE_PIPELINE_FAILURE":
        next_actions.append("INVESTIGATE_HISTORICAL_PIPELINE_FAILURE")

    if pipeline_decision == "PIPELINE_IN_PROGRESS":
        next_actions.append("MONITOR_PIPELINE_EXECUTION")

    if decision == "PIPELINE_HEALTHY":
        next_actions.append("CONTINUE_NORMAL_MONITORING")

    if decision == "REVIEW_REQUIRED":
        next_actions.append("REVIEW_AVAILABLE_EVIDENCE")

    return {
        "decision": decision,
        "reason": (
            f"DQ assessment: {dq_action}; "
            f"pipeline assessment: {pipeline_decision}."
        ),
        "dq_decision": dq_action,
        "pipeline_decision": pipeline_decision,
        "recommended_next_actions": next_actions,
    }


def extract_failed_range_check(
    dq_assessment: dict[str, Any],
) -> dict[str, Any] | None:
    """Extract the first failed numeric range check."""

    for check in dq_assessment["failed_checks"]:
        check_name = check.get("check", "")

        match = re.fullmatch(
            r"range_check:(.+)",
            check_name,
        )

        if not match:
            continue

        column = match.group(1)
        range_expression = check.get("range", "")

        range_match = re.fullmatch(
            r"\s*(-?\d+(?:\.\d+)?)\s*<=\s*value\s*<=\s*(-?\d+(?:\.\d+)?)\s*",
            range_expression,
        )

        if not range_match:
            continue

        return {
            "column": column,
            "minimum": range_match.group(1),
            "maximum": range_match.group(2),
        }

    return None


def invoke_rca_agent(
    dataset: str,
    dq_assessment: dict[str, Any],
) -> dict[str, Any]:
    """Invoke the existing RCA agent and load its JSON result."""

    investigation = extract_failed_range_check(dq_assessment)

    if not investigation:
        return {
            "status": "SKIPPED",
            "reason": "No supported failed numeric range check found.",
        }

    safe_dataset = re.sub(r"[^A-Za-z0-9_.-]", "_", dataset)

    output_path = (
        PROJECT_ROOT
        / "results"
        / "rca"
        / f"{safe_dataset}_supervisor_rca.json"
    )

    output_path.parent.mkdir(parents=True, exist_ok=True)

    command = [
        sys.executable,
        "-m",
        "agents.rca.rca_agent",
        "--dataset",
        dataset,
        "--column",
        investigation["column"],
        "--minimum",
        investigation["minimum"],
        "--maximum",
        investigation["maximum"],
        "--output",
        str(output_path),
    ]

    try:
        result = subprocess.run(
            command,
            cwd=PROJECT_ROOT,
            capture_output=True,
            text=True,
            timeout=900,
            check=False,
        )

    except subprocess.TimeoutExpired:
        return {
            "status": "TIMEOUT",
            "investigation_scope": investigation,
            "error": "RCA agent exceeded the 900-second timeout.",
        }
    except OSError as exc:
        return {
            "status": "FAILED",
            "investigation_scope": investigation,
            "error": f"Unable to start RCA agent: {exc}",
        }

    if result.returncode != 0:
        return {
            "status": "FAILED",
            "investigation_scope": investigation,
            "error": result.stderr[-3000:],
        }

    if not output_path.is_file():
        return {
            "status": "FAILED",
            "investigation_scope": investigation,
            "error": "RCA output file was not created.",
        }

    try:
        rca_result = json.loads(output_path.read_text())
    except (OSError, json.JSONDecodeError) as exc:
        return {
            "status": "FAILED",
            "investigation_scope": investigation,
            "error": f"Unable to read RCA result: {exc}",
        }

    return {
        "status": "SUCCESS",
        "investigation_scope": investigation,
        "output_path": str(output_path),
        "result": rca_result,
    }

def run_dag_assessment(
    dag_run: Any,
    dq_tables: list[str],
    gold_tables: list[str],
) -> dict[str, Any]:
    """
    Assess the current DataPilot DAG run.

    Uses current Airflow task states and run-specific
    DQ/RCA evidence. Does not invoke RCA or trigger DAGs.
    """

    run_id = dag_run.run_id

    results_root = PROJECT_ROOT / "results"

    print("Starting DataPilot DAG Supervisor assessment.")
    print(f"DAG ID: {dag_run.dag_id}")
    print(f"DAG Run ID: {run_id}")

    # --------------------------------------------------
    # Collect current-run task states
    # --------------------------------------------------

    task_assessment = collect_current_run_tasks(dag_run)

    task_map = {
        task["task_id"]: task
        for task in task_assessment["tasks"]
    }

    # --------------------------------------------------
    # Collect DQ and RCA evidence
    # --------------------------------------------------

    evidence = collect_dag_run_evidence(
        dag_run=dag_run,
        dq_tables=dq_tables,
        results_root=results_root,
    )

    dq_results = evidence["dq_results"]
    rca_results = evidence["rca_results"]

    # --------------------------------------------------
    # Collect Gold execution status
    # --------------------------------------------------

    gold_results = {}

    for dataset in gold_tables:

        task_id = f"gold_{dataset}"
        task = task_map.get(task_id)

        gold_results[dataset] = (
            task["state"] if task else "TASK_INSTANCE_MISSING"
        )

    summary_task = task_map.get("gold_olist_order_summary")

    summary_status = (
        summary_task["state"]
        if summary_task
        else "TASK_INSTANCE_MISSING"
    )

    # --------------------------------------------------
    # Summarize DQ outcomes
    # --------------------------------------------------

    dq_status_counts = {
        "PASS": 0,
        "FAIL": 0,
        "MISSING": 0,
        "STALE": 0,
        "OTHER": 0,
    }

    for result in dq_results.values():

        status = result.get("dq_status") or result.get("status")

        if status in {"PASS", "FAIL", "MISSING", "STALE"}:
            dq_status_counts[status] += 1
        else:
            dq_status_counts["OTHER"] += 1

    # --------------------------------------------------
    # Determine pipeline assessment
    # --------------------------------------------------

    failed_tasks = [
        task for task in task_assessment["tasks"]
        if task["state"] == "failed"
    ]

    upstream_failed_tasks = [
        task for task in task_assessment["tasks"]
        if task["state"] == "upstream_failed"
    ]

    missing_or_invalid_dq = (
        dq_status_counts["MISSING"]
        + dq_status_counts["STALE"]
        + dq_status_counts["OTHER"]
    )

    if missing_or_invalid_dq:
        decision = "REVIEW_REQUIRED"

    elif dq_status_counts["FAIL"] > 0:
        decision = "DQ_FAILURE_INVESTIGATION_REQUIRED"

    elif failed_tasks:
        decision = "PIPELINE_FAILURE_INVESTIGATION_REQUIRED"

    elif summary_status != "success":
        decision = "GOLD_SUMMARY_NOT_COMPLETED"

    else:
        decision = "PIPELINE_HEALTHY"

    # --------------------------------------------------
    # Build final assessment
    # --------------------------------------------------

    assessment = {
        "agent": "DataPilot Supervisor",
        "assessment_type": "IN_DAG_RUN_ASSESSMENT",
        "assessment_timestamp": datetime.now(timezone.utc).isoformat(),

        "pipeline": {
            "dag_id": dag_run.dag_id,
            "dag_run_id": run_id,
            "task_count": task_assessment["task_count"],
            "task_state_counts": task_assessment["state_counts"],
            "failed_task_count": len(failed_tasks),
            "upstream_failed_task_count": len(upstream_failed_tasks),
        },

        "dq_assessment": {
            "dataset_count": len(dq_results),
            "status_counts": dq_status_counts,
            "results": dq_results,
        },

        "rca_assessment": {
            "results": rca_results,
        },

        "gold_assessment": {
            "dataset_results": gold_results,
            "business_summary_status": summary_status,
        },

        "supervisor_decision": {
            "decision": decision,
            "recommended_next_actions": (
                ["REVIEW_DQ_AND_RCA_EVIDENCE"]
                if decision == "DQ_FAILURE_INVESTIGATION_REQUIRED"
                else ["INVESTIGATE_PIPELINE_FAILURE"]
                if decision == "PIPELINE_FAILURE_INVESTIGATION_REQUIRED"
                else ["REVIEW_AVAILABLE_EVIDENCE"]
                if decision in {
                    "REVIEW_REQUIRED",
                    "GOLD_SUMMARY_NOT_COMPLETED",
                }
                else ["CONTINUE_NORMAL_MONITORING"]
            ),
        },

        "execution_safety": {
            "remediation_executed": False,
            "dag_triggered": False,
            "rca_invoked_by_supervisor": False,
        },
    }

    # --------------------------------------------------
    # Save run-specific assessment
    # --------------------------------------------------

    output_dir = PROJECT_ROOT / "results" / "supervisor"
    output_dir.mkdir(parents=True, exist_ok=True)

    safe_run_id = re.sub(r"[^A-Za-z0-9_.-]", "_", run_id)

    output_path = output_dir / f"{safe_run_id}_assessment.json"

    output_path.write_text(
        json.dumps(assessment, indent=2, default=str) + "\n"
    )

    print(f"Supervisor assessment saved: {output_path}")
    print(f"Supervisor decision: {decision}")

    return assessment


def _get_remediation_execution_metadata(
    dag_run: Any,
    requested_mode: str,
    execution_state: str,
) -> dict[str, Any]:
    """Resolve the remediation execution ID without affecting run assessment."""

    run_conf = dag_run.conf or {}

    if requested_mode == "GOLD_REFRESH_ONLY":
        execution_id = run_conf.get("execution_id")
        return {
            "execution_id": execution_id,
            "execution_id_source": "DAG_RUN_CONFIGURATION",
            "execution_id_status": (
                "AVAILABLE" if execution_id else "NOT_PROVIDED"
            ),
        }

    if requested_mode not in {"DRY_RUN", "APPLY"}:
        return {
            "execution_id": None,
            "execution_id_source": "NOT_APPLICABLE",
            "execution_id_status": "UNSUPPORTED_MODE",
        }

    if execution_state != "success":
        return {
            "execution_id": None,
            "execution_id_source": "EXECUTOR_XCOM",
            "execution_id_status": "EXECUTOR_NOT_SUCCESSFUL",
        }

    try:
        airflow_client = AirflowClient()
        xcom_response = airflow_client.get_task_xcom(
            dag_id=dag_run.dag_id,
            dag_run_id=dag_run.run_id,
            task_id="execute_remediation_and_write_summary",
            key="return_value",
        )
        xcom_value = xcom_response.get("value") if isinstance(xcom_response, dict) else None
        execution_id = (
            xcom_value.get("execution_id")
            if isinstance(xcom_value, dict)
            else None
        )

        if isinstance(execution_id, str) and execution_id.strip():
            return {
                "execution_id": execution_id,
                "execution_id_source": "EXECUTOR_XCOM",
                "execution_id_status": "AVAILABLE",
            }

        return {
            "execution_id": None,
            "execution_id_source": "EXECUTOR_XCOM",
            "execution_id_status": "MISSING_FROM_XCOM",
        }

    except Exception as exc:
        # Traceability is best-effort; it must not fail the Supervisor assessment.
        return {
            "execution_id": None,
            "execution_id_source": "EXECUTOR_XCOM",
            "execution_id_status": "XCOM_RETRIEVAL_FAILED",
            "execution_id_error": str(exc),
        }


def run_remediation_dag_assessment(dag_run: Any) -> dict[str, Any]:
    """Assess a remediation DAG run without executing remediation."""

    run_id = dag_run.run_id
    run_conf = dag_run.conf or {}
    requested_mode = str(run_conf.get("mode", "DRY_RUN")).upper()

    print("Starting DataPilot Remediation Supervisor assessment.")
    print(f"DAG ID: {dag_run.dag_id}")
    print(f"DAG Run ID: {run_id}")
    print(f"Requested mode: {requested_mode}")

    task_assessment = collect_current_run_tasks(dag_run)

    task_map = {
        task["task_id"]: task
        for task in task_assessment["tasks"]
    }

    monitored_task_ids = [
        "execute_remediation_and_write_summary",
        "validate_existing_apply_for_gold",
        "continue_to_gold_only_after_apply",
        "refresh_gold_olist_order_payments",
        "refresh_gold_olist_order_summary",
        "update_postgres_summary_after_gold_refresh",
        "update_postgres_summary_after_gold_failure",
    ]

    remediation_tasks = {
        task_id: (
            task_map[task_id]["state"]
            if task_id in task_map
            else "TASK_INSTANCE_MISSING"
        )
        for task_id in monitored_task_ids
    }

    failed_tasks = [
        task for task in task_assessment["tasks"]
        if task["state"] == "failed"
    ]

    upstream_failed_tasks = [
        task for task in task_assessment["tasks"]
        if task["state"] == "upstream_failed"
    ]

    execution_state = remediation_tasks[
        "execute_remediation_and_write_summary"
    ]
    validation_state = remediation_tasks[
        "validate_existing_apply_for_gold"
    ]
    payments_state = remediation_tasks[
        "refresh_gold_olist_order_payments"
    ]
    gold_summary_state = remediation_tasks[
        "refresh_gold_olist_order_summary"
    ]

    postgres_success_state = remediation_tasks[
        "update_postgres_summary_after_gold_refresh"
    ]
    postgres_failure_state = remediation_tasks[
        "update_postgres_summary_after_gold_failure"
    ]

    if requested_mode == "DRY_RUN":
        if execution_state == "success":
            decision = "DRY_RUN_COMPLETED_NO_GOLD_REFRESH"
        elif execution_state in {"failed", "upstream_failed"}:
            decision = "REMEDIATION_DRY_RUN_FAILED"
        else:
            decision = "REVIEW_REMEDIATION_EXECUTION"

    elif requested_mode == "APPLY":
        if execution_state != "success":
            decision = "APPLY_EXECUTION_FAILED_OR_INCOMPLETE"
        elif payments_state == "success" and gold_summary_state == "success":
            decision = (
                "APPLY_AND_GOLD_REFRESH_SUCCESS"
                if postgres_success_state == "success"
                else "GOLD_SUCCESS_SUMMARY_UPDATE_INCOMPLETE"
            )
        elif (
            payments_state in {"failed", "upstream_failed"}
            or gold_summary_state in {"failed", "upstream_failed"}
        ):
            decision = "APPLY_COMMITTED_GOLD_REFRESH_FAILED"
        else:
            decision = "APPLY_COMPLETED_GOLD_OUTCOME_UNKNOWN"

    elif requested_mode == "GOLD_REFRESH_ONLY":
        if validation_state != "success":
            decision = "EXISTING_APPLY_VALIDATION_FAILED_OR_INCOMPLETE"
        elif payments_state == "success" and gold_summary_state == "success":
            decision = (
                "GOLD_REFRESH_ONLY_SUCCESS"
                if postgres_success_state == "success"
                else "GOLD_SUCCESS_SUMMARY_UPDATE_INCOMPLETE"
            )
        elif (
            payments_state in {"failed", "upstream_failed"}
            or gold_summary_state in {"failed", "upstream_failed"}
        ):
            decision = "GOLD_REFRESH_ONLY_FAILED"
        else:
            decision = "GOLD_REFRESH_OUTCOME_UNKNOWN"

    else:
        decision = "UNSUPPORTED_REMEDIATION_MODE"

    execution_metadata = _get_remediation_execution_metadata(
        dag_run=dag_run,
        requested_mode=requested_mode,
        execution_state=execution_state,
    )

    assessment = {
        "agent": "DataPilot Supervisor",
        "assessment_type": "REMEDIATION_DAG_RUN_ASSESSMENT",
        "assessment_timestamp": datetime.now(timezone.utc).isoformat(),
        "pipeline": {
            "dag_id": dag_run.dag_id,
            "dag_run_id": run_id,
            "dag_run_state": dag_run.state,
            "task_count": task_assessment["task_count"],
            "task_state_counts": task_assessment["state_counts"],
            "failed_task_count": len(failed_tasks),
            "upstream_failed_task_count": len(upstream_failed_tasks),
        },
        "remediation_assessment": {
            "requested_mode": requested_mode,
            **execution_metadata,
            "task_results": remediation_tasks,
        },
        "supervisor_decision": {
            "decision": decision,
            "recommended_next_actions": (
                ["REVIEW_REMEDIATION_EXECUTION_EVIDENCE"]
                if decision in {
                    "REMEDIATION_DRY_RUN_FAILED",
                    "APPLY_EXECUTION_FAILED_OR_INCOMPLETE",
                    "EXISTING_APPLY_VALIDATION_FAILED_OR_INCOMPLETE",
                    "UNSUPPORTED_REMEDIATION_MODE",
                    "APPLY_COMPLETED_GOLD_OUTCOME_UNKNOWN",
                    "GOLD_REFRESH_OUTCOME_UNKNOWN",
                }
                else ["REVIEW_GOLD_REFRESH_FAILURE"]
                if decision in {
                    "APPLY_COMMITTED_GOLD_REFRESH_FAILED",
                    "GOLD_REFRESH_ONLY_FAILED",
                }
                else ["REVIEW_POSTGRES_SUMMARY_UPDATE"]
                if decision == "GOLD_SUCCESS_SUMMARY_UPDATE_INCOMPLETE"
                else ["REVIEW_AVAILABLE_EVIDENCE"]
                if decision == "REVIEW_REMEDIATION_EXECUTION"
                else ["CONTINUE_NORMAL_MONITORING"]
            ),
        },
        "execution_safety": {
            "remediation_executed_by_supervisor": False,
            "dag_triggered_by_supervisor": False,
            "approval_granted_by_supervisor": False,
            "source_data_modified_by_supervisor": False,
        },
    }

    output_dir = (
        PROJECT_ROOT / "results" / "supervisor" / "remediation"
    )
    output_dir.mkdir(parents=True, exist_ok=True)

    safe_run_id = re.sub(r"[^A-Za-z0-9_.-]", "_", run_id)
    output_path = output_dir / f"{safe_run_id}_assessment.json"

    assessment["output_path"] = str(output_path)

    output_path.write_text(
        json.dumps(assessment, indent=2, default=str) + "\n"
    )

    print(f"Supervisor assessment saved: {output_path}")
    print(f"Supervisor decision: {decision}")

    return assessment

def run_assessment(
    dq_path: Path = DEFAULT_DQ_PATH,
    dag_id: str = DEFAULT_DAG_ID,
    output_path: Path = DEFAULT_OUTPUT_PATH,
) -> dict[str, Any]:
    """Run a read-only Supervisor assessment."""
    dq_result = load_dq_result(dq_path)
    dq_assessment = assess_dq(dq_result)

    airflow_client = AirflowClient()
    pipeline_assessment = get_pipeline_status(
        airflow_client,
        dag_id,
    )
    
    latest_run = pipeline_assessment.get("latest_run", {})

    if latest_run.get("state") == "failed":
        try:
            pipeline_assessment["task_diagnostics"] = (
                get_task_failure_diagnostics(
                    airflow_client,
                    dag_id,
                    latest_run["dag_run_id"],
                )
            )
        except Exception as exc:
            pipeline_assessment["task_diagnostics"] = {
                "status": "DIAGNOSTIC_COLLECTION_FAILED",
                "error": str(exc),
            }

    final_decision = make_decision(
        dq_assessment,
        pipeline_assessment,
    )
    rca_assessment = None
    if dq_assessment["decision"] == "INVOKE_RCA":
        print("Supervisor: invoking existing RCA agent...")

        rca_assessment = invoke_rca_agent(
            dq_result["dataset"],
            dq_assessment,
        )

    assessment = {
        "agent": "DataPilot Supervisor",
        "assessment_timestamp": datetime.now(timezone.utc).isoformat(),
        "execution_mode": "READ_ONLY",
        "dataset": dq_result["dataset"],
        "dq_assessment": dq_assessment,
        "pipeline_assessment": pipeline_assessment,
        "supervisor_decision": final_decision,
        "remediation_executed": False,
        "dag_triggered": False,
        "rca_assessment": rca_assessment,
    }

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(assessment, indent=2, default=str) + "\n"
    )

    return assessment


def main() -> None:
    parser = argparse.ArgumentParser(
        description="DataPilot AI Supervisor - read-only assessment"
    )
    parser.add_argument(
        "--dataset",
        default="olist_order_payments",
        help="Dataset name",
    )
    parser.add_argument(
        "--dag-id",
        default=DEFAULT_DAG_ID,
        help="Airflow DAG ID",
    )
    parser.add_argument(
        "--dq-file",
        type=Path,
        default=None,
        help="Path to the DQ result JSON",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT_PATH,
        help="Assessment output JSON path",
    )

    args = parser.parse_args()

    dq_path = args.dq_file or (
        PROJECT_ROOT / "results" / "dq" / f"{args.dataset}.json"
    )

    result = run_assessment(
        dq_path=dq_path,
        dag_id=args.dag_id,
        output_path=args.output,
    )

    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
