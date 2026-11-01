import hashlib
import json
import logging
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.sensors.python import PythonSensor

PROJECT_ROOT = Path("/opt/datapilot")
EXECUTOR = PROJECT_ROOT / "agents/remediation/executor.py"
HISTORY_DIR = PROJECT_ROOT / "results/remediation/history"

DATASET = "olist_order_payments"
ACTION = "QUARANTINE_RECORDS"

PLAN_PATH = (
    PROJECT_ROOT / "results/remediation"
    / f"{DATASET}_candidate.json"
)
APPROVAL_PATH = (
    PROJECT_ROOT / "results/approvals"
    / f"{DATASET}.json"
)

logger = logging.getLogger(__name__)


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def get_execution_id(context):
    dag_id = context["dag"].dag_id
    run_id = context["dag_run"].run_id
    digest = hashlib.sha256(
        f"{dag_id}:{run_id}".encode()
    ).hexdigest()[:16]
    return f"airflow-approval-{digest}"


def load_json(path):
    with path.open(encoding="utf-8") as file:
        return json.load(file)


def fingerprint_plan(plan):
    from agents.remediation.plan_fingerprint import (
        validate_plan_fingerprint,
    )
    return validate_plan_fingerprint(plan)


def save_summary(summary):
    # Import in task context so the Airflow PostgresHook
    # uses the configured Airflow connection.
    sys.path.insert(0, str(PROJECT_ROOT))
    from agents.remediation.summary_store import (
        upsert_execution_summary,
    )
    upsert_execution_summary(summary)


def make_summary(context, status, fingerprint=None, approval=None):
    approval = approval or {}
    return {
        "execution_id": get_execution_id(context),
        "dag_id": context["dag"].dag_id,
        "dag_run_id": context["dag_run"].run_id,
        "dataset": DATASET,
        "action": ACTION,
        "execution_mode": "DRY_RUN",
        "execution_status": status,
        "approval_status": approval.get("status"),
        "approved_by": approval.get("approved_by"),
        "plan_fingerprint": fingerprint or approval.get(
            "plan_fingerprint"
        ),
        "invalid_count": None,
        "quarantined_count": 0,
        "target_table": None,
        "quarantine_path": None,
        "source_modified": None,
        "s3_write_performed": None,
        "started_at": None,
        "completed_at": None,
        "error_type": None,
        "error_message": None,
        "audit_details": {
            "plan_file": str(PLAN_PATH),
            "approval_file": str(APPROVAL_PATH),
            "history_file": str(
                HISTORY_DIR / f"{get_execution_id(context)}.json"
            ),
        },
    }


def validate_candidate_plan(**context):
    if not PLAN_PATH.is_file():
        raise FileNotFoundError(f"Candidate plan missing: {PLAN_PATH}")

    plan = load_json(PLAN_PATH)
    fingerprint = fingerprint_plan(plan)

    expected = {
        "dataset": DATASET,
        "action": ACTION,
        "status": "PENDING_APPROVAL",
        "requires_approval": True,
        "execution_mode": "DRY_RUN",
    }
    for field, expected_value in expected.items():
        if plan.get(field) != expected_value:
            raise ValueError(
                f"Candidate field {field!r} must equal "
                f"{expected_value!r}"
            )

    summary = make_summary(
        context, "WAITING_APPROVAL", fingerprint=fingerprint
    )
    summary["audit_details"].update({
        "approval_validation": "PENDING",
        "safety_validation": "NOT_STARTED",
    })
    save_summary(summary)

    logger.info("Candidate fingerprint validated: %s", fingerprint)
    return fingerprint


def approval_matches_candidate(**context):
    plan = load_json(PLAN_PATH)
    fingerprint = fingerprint_plan(plan)

    if plan.get("dataset") != DATASET or plan.get("action") != ACTION:
        raise ValueError("Candidate dataset/action mismatch.")

    if plan.get("status") != "PENDING_APPROVAL":
        raise ValueError("Candidate plan is no longer pending approval.")

    if not APPROVAL_PATH.is_file():
        logger.info("Waiting for approval record.")
        return False

    approval = load_json(APPROVAL_PATH)

    if approval.get("status") == "REJECTED":
        summary = make_summary(
            context, "REJECTED", fingerprint, approval
        )
        summary["completed_at"] = utc_now()
        summary["audit_details"]["approval_validation"] = "REJECTED"
        save_summary(summary)
        raise ValueError("Remediation was rejected.")

    if approval.get("status") != "APPROVED":
        logger.info("Approval is not yet approved.")
        return False

    if approval.get("dataset") != DATASET:
        raise ValueError("Approval dataset mismatch.")

    if approval.get("action") != ACTION:
        raise ValueError("Approval action mismatch.")

    if approval.get("plan_fingerprint") != fingerprint:
        logger.warning("Approval fingerprint does not match the plan.")
        return False

    if approval.get("execution_authorized") is not True:
        logger.info("Execution is not authorized.")
        return False

    if approval.get("execution_scope") != "DRY_RUN_ONLY":
        raise ValueError("Approval must be restricted to DRY_RUN_ONLY.")

    summary = make_summary(
        context, "APPROVED", fingerprint, approval
    )
    summary["audit_details"].update({
        "approval_validation": "PASSED",
        "execution_scope": "DRY_RUN_ONLY",
    })
    save_summary(summary)

    logger.info("Approval validated for fingerprint: %s", fingerprint)
    return True


def run_remediation_execution(**context):
    # Revalidate immediately before invoking the executor.
    if not approval_matches_candidate(**context):
        raise RuntimeError("Approval validation failed before execution.")

    plan = load_json(PLAN_PATH)
    fingerprint = fingerprint_plan(plan)
    approval = load_json(APPROVAL_PATH)
    execution_id = get_execution_id(context)

    summary = make_summary(
        context, "RUNNING", fingerprint, approval
    )
    summary["started_at"] = utc_now()
    summary["audit_details"].update({
        "approval_validation": "PASSED",
        "execution_scope": "DRY_RUN_ONLY",
        "safety_validation": "PENDING",
    })
    save_summary(summary)

    command = [
        sys.executable,
        str(EXECUTOR),
        "--dataset", DATASET,
        "--action", ACTION,
        "--mode", "DRY_RUN",
        "--plan-file", str(PLAN_PATH),
        "--execution-id", execution_id,
    ]

    try:
        completed = subprocess.run(
            command,
            cwd=str(PROJECT_ROOT),
            capture_output=True,
            text=True,
            timeout=600,
            check=False,
        )

        if completed.stdout:
            logger.info("Executor stdout:\n%s", completed.stdout)
        if completed.stderr:
            logger.info("Executor stderr:\n%s", completed.stderr)

        if completed.returncode != 0:
            raise RuntimeError(
                f"Executor returned exit code {completed.returncode}."
            )

        summary["execution_status"] = "EXECUTED_PENDING_VERIFICATION"
        summary["audit_details"].update({
            "subprocess_returncode": completed.returncode,
            "verification": "PENDING",
        })
        save_summary(summary)
        return execution_id

    except Exception as exc:
        summary["execution_status"] = "FAILED"
        summary["completed_at"] = utc_now()
        summary["error_type"] = type(exc).__name__
        summary["error_message"] = str(exc)[:2000]
        summary["audit_details"]["execution_stage"] = "EXECUTION"
        try:
            save_summary(summary)
        except Exception:
            logger.exception("Could not persist failed execution summary.")
        raise


def verify_dry_run(**context):
    execution_id = context["ti"].xcom_pull(
        task_ids="execute_remediation_dry_run"
    )
    if not execution_id:
        raise RuntimeError("Execution ID was not returned by executor task.")

    history_path = HISTORY_DIR / f"{execution_id}.json"
    summary = make_summary(context, "FAILED")
    summary["started_at"] = None

    try:
        if not history_path.is_file():
            raise FileNotFoundError(
                f"Execution history missing: {history_path}"
            )

        result = load_json(history_path)
        execution = result.get("execution") or {}
        dq_validation = result.get("dq_validation") or {}
        approval = result.get("approval") or {}
        quarantine_config = result.get("quarantine_config") or {}

        if result.get("dataset") != DATASET:
            raise RuntimeError("History dataset mismatch.")
        if result.get("action") != ACTION:
            raise RuntimeError("History action mismatch.")
        if execution.get("execution_id") != execution_id:
            raise RuntimeError("History execution ID mismatch.")
        if execution.get("mode") != "DRY_RUN":
            raise RuntimeError("History does not confirm DRY_RUN mode.")
        if execution.get("status") != "VALIDATED":
            raise RuntimeError(
                f"Executor status is not VALIDATED: "
                f"{execution.get('status')!r}"
            )
        if execution.get("executed") is not False:
            raise RuntimeError("Safety check failed: executed is not false.")
        if execution.get("source_modification") is not False:
            raise RuntimeError(
                "Safety check failed: source modification is not false."
            )
        if execution.get("s3_write_performed") is not False:
            raise RuntimeError(
                "Safety check failed: S3 write is not explicitly false."
            )

        catalog = quarantine_config.get("catalog")
        database = quarantine_config.get("database")
        table = quarantine_config.get("table")
        target_table = (
            f"{catalog}.{database}.{table}"
            if catalog and database and table else None
        )

        summary = make_summary(
            context,
            "SUCCESS",
            fingerprint=approval.get("plan_fingerprint"),
            approval=approval,
        )
        summary.update({
            "invalid_count": dq_validation.get("invalid_count"),
            "quarantined_count": 0,
            "target_table": target_table,
            "source_modified": False,
            "s3_write_performed": False,
            "completed_at": utc_now(),
            "audit_details": {
                "plan_file": str(PLAN_PATH),
                "approval_file": str(APPROVAL_PATH),
                "history_file": str(history_path),
                "executor_status": execution.get("status"),
                "executor_message": execution.get("message"),
                "execution_scope": "DRY_RUN_ONLY",
                "safety_validation": "PASSED",
            },
        })
        save_summary(summary)
        logger.info("DRY_RUN audit verification passed: %s", execution_id)

    except Exception as exc:
        summary["execution_id"] = execution_id
        summary["dag_id"] = context["dag"].dag_id
        summary["dag_run_id"] = context["dag_run"].run_id
        summary["dataset"] = DATASET
        summary["action"] = ACTION
        summary["execution_mode"] = "DRY_RUN"
        summary["execution_status"] = "FAILED"
        summary["completed_at"] = utc_now()
        summary["error_type"] = type(exc).__name__
        summary["error_message"] = str(exc)[:2000]
        summary["audit_details"].update({
            "history_file": str(history_path),
            "safety_validation": "FAILED_OR_INCOMPLETE",
        })
        try:
            save_summary(summary)
        except Exception:
            logger.exception("Could not persist failed verification summary.")
        raise


with DAG(
    dag_id="datapilot_remediation_approval",
    description=(
        "Fingerprint-bound approval and DRY_RUN remediation "
        "with PostgreSQL execution auditing"
    ),
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "datapilot", "retries": 0},
    dagrun_timeout=timedelta(days=8),
    tags=["datapilot", "remediation", "approval", "dry-run", "audit"],
) as dag:

    validate_plan = PythonOperator(
        task_id="validate_candidate_plan",
        python_callable=validate_candidate_plan,
    )

    wait_for_approval = PythonSensor(
        task_id="wait_for_matching_approval",
        python_callable=approval_matches_candidate,
        poke_interval=60,
        timeout=7 * 24 * 60 * 60,
        mode="reschedule",
    )

    execute_dry_run = PythonOperator(
        task_id="execute_remediation_dry_run",
        python_callable=run_remediation_execution,
    )

    verify_result = PythonOperator(
        task_id="verify_dry_run_result",
        python_callable=verify_dry_run,
    )

    validate_plan >> wait_for_approval >> execute_dry_run >> verify_result

