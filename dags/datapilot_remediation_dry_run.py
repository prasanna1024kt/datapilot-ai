import hashlib
import json
import logging
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.python import ShortCircuitOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sdk import TriggerRule
from airflow.exceptions import AirflowException
from airflow.providers.standard.operators.python import BranchPythonOperator

PROJECT_ROOT = Path("/opt/datapilot")
EXECUTOR = PROJECT_ROOT / "agents/remediation/executor.py"
PLAN_FILE = (
    PROJECT_ROOT
    / "results/remediation/olist_order_payments_candidate.json"
)
HISTORY_DIR = PROJECT_ROOT / "results/remediation/history"
APPROVAL_FILE = PROJECT_ROOT / "results/approvals/olist_order_payments_apply.json"
AUDIT_DIR = Path("/opt/spark-apps/results/quarantine")
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


DATASET = "olist_order_payments"
ACTION = "QUARANTINE_RECORDS"

logger = logging.getLogger(__name__)


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def read_history(execution_id):
    history_file = HISTORY_DIR / f"{execution_id}.json"

    if not history_file.is_file():
        return None

    with history_file.open() as file:
        return json.load(file)


def get_target_table(quarantine_config):
    catalog = quarantine_config.get("catalog")
    database = quarantine_config.get("database")
    table = quarantine_config.get("table")

    if catalog and database and table:
        return f"{catalog}.{database}.{table}"

    return None


def route_remediation_mode(**context):
    """Route recovery runs around the remediation executor."""

    mode = str(
        (context["dag_run"].conf or {}).get("mode", "DRY_RUN")
    ).upper()

    if mode in {"DRY_RUN", "APPLY"}:
        return "execute_dry_run_and_write_summary"

    if mode == "GOLD_REFRESH_ONLY":
        return "validate_existing_apply_for_gold"

    raise AirflowException(
        "mode must be DRY_RUN, APPLY, or GOLD_REFRESH_ONLY."
    )

def run_remediation_dry_run(**context):
    # Ensure the project modules are importable by the Airflow task.
    sys.path.insert(0, str(PROJECT_ROOT))

    from agents.remediation.summary_store import (
        upsert_execution_summary,
    )

    dag_run = context["dag_run"]
    dag_id = context["dag"].dag_id
    dag_run_id = dag_run.run_id
    run_conf = dag_run.conf or {}
    mode = str(run_conf.get("mode", "DRY_RUN")).upper()
    confirm_write = run_conf.get("confirm_write") is True
    if mode not in {"DRY_RUN", "APPLY"}:
        raise ValueError("mode must be DRY_RUN or APPLY.")
    if mode == "APPLY" and not confirm_write:
        raise ValueError("APPLY requires dag_run.conf confirm_write=true.")

    # Stable across retries of the same DAG run; unique across runs.
    run_hash = hashlib.sha256(
        f"{dag_id}:{dag_run_id}".encode()
    ).hexdigest()[:16]

    execution_id = f"airflow-olist-order-payments-{run_hash}"
    started_at = utc_now()

    if not PLAN_FILE.is_file():
        raise FileNotFoundError(
            f"Candidate remediation plan not found: {PLAN_FILE}"
        )

    if not EXECUTOR.is_file():
        raise FileNotFoundError(
            f"Remediation executor not found: {EXECUTOR}"
        )

    summary = {
        "execution_id": execution_id,
        "dag_id": dag_id,
        "dag_run_id": dag_run_id,
        "dataset": DATASET,
        "action": ACTION,
        "execution_mode": mode,
        "execution_status": "RUNNING",
        "approval_status": None,
        "approved_by": None,
        "plan_fingerprint": None,
        "invalid_count": None,
        "quarantined_count": 0,
        "target_table": None,
        "quarantine_path": None,
        "source_modified": None,
        "s3_write_performed": None,
        "started_at": started_at,
        "completed_at": None,
        "error_type": None,
        "error_message": None,
        "audit_details": {
            "plan_file": str(PLAN_FILE),
            "history_file": str(
                HISTORY_DIR / f"{execution_id}.json"
            ),
            "safety_validation": "PENDING",
        },
    }

    # If this fails, stop before invoking the executor.
    upsert_execution_summary(summary)

    completed = None
    history = None

    try:
                # Validate approval BEFORE invoking the executor.
        if mode == "APPLY":
            if not APPROVAL_FILE.is_file():
                raise FileNotFoundError(
                    f"APPLY approval file not found: {APPROVAL_FILE}"
                )

            approved = json.loads(APPROVAL_FILE.read_text())

            if approved.get("status") != "APPROVED":
                raise RuntimeError("APPLY approval status is not APPROVED.")

            if approved.get("execution_scope") != "APPLY":
                raise RuntimeError("Approval execution_scope must be APPLY.")

            if approved.get("execution_authorized") is not True:
                raise RuntimeError("Approval does not authorize execution.")

            plan = json.loads(PLAN_FILE.read_text())
            plan_fingerprint = plan.get("plan_fingerprint")

            if (
                not plan_fingerprint
                or approved.get("plan_fingerprint") != plan_fingerprint
            ):
                raise RuntimeError(
                    "Approval fingerprint does not match the candidate plan."
                )
            if approved.get("expected_invalid_count") is None:
                raise RuntimeError(
                    "Approval is missing expected_invalid_count."
                )
        command = [
            sys.executable,
            str(EXECUTOR),
            "--dataset", DATASET,
            "--action", ACTION,
            "--mode", mode,
            "--plan-file", str(PLAN_FILE),
            "--execution-id", execution_id,
        ]
        if mode == "APPLY":
            command.append("--confirm-write")

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

        # Read the per-execution history, not the shared latest-result file.
        history = read_history(execution_id)

        if completed.returncode != 0:
            raise RuntimeError(
                "Executor returned non-zero exit code "
                f"{completed.returncode}."
            )

        if not isinstance(history, dict):
            raise RuntimeError(
                "Executor did not produce the expected execution history."
            )

        execution = history.get("execution")
        if not isinstance(execution, dict):
            raise RuntimeError(
                "Execution history is missing the execution object."
            )

        if history.get("dataset") != DATASET:
            raise RuntimeError("History dataset does not match the request.")

        if history.get("action") != ACTION:
            raise RuntimeError("History action does not match the request.")

        if execution.get("execution_id") != execution_id:
            raise RuntimeError("History execution ID does not match the run.")

        if execution.get("mode") != mode:
            raise RuntimeError("History mode does not match requested mode.")

        if mode == "DRY_RUN":
            if execution.get("executed") is not False:
                raise RuntimeError("DRY_RUN must not execute writes.")
            if execution.get("s3_write_performed") is not False:
                raise RuntimeError("DRY_RUN must not write to S3.")
            if execution.get("source_modification") is not False:
                raise RuntimeError("DRY_RUN must confirm source unchanged.")
        else:
            if execution.get("executed") is not True:
                raise RuntimeError("APPLY history does not confirm execution.")
            if execution.get("s3_write_performed") is not True:
                raise RuntimeError("APPLY history does not confirm an S3 write.")
            if execution.get("source_modification") is not False:
                raise RuntimeError("APPLY must confirm source unchanged.")

        executor_status = str(execution.get("status", "")).upper()

        if mode == "APPLY" and executor_status != "COMMITTED_AND_VERIFIED":
            raise RuntimeError(f"APPLY did not commit and verify: {executor_status!r}")
        if mode == "DRY_RUN" and (not executor_status or executor_status in {"FAILED", "FAILURE", "ERROR"}):
            raise RuntimeError(f"DRY_RUN failed: {executor_status!r}")

        dq_validation = history.get("dq_validation") or {}
        approval = history.get("approval") or {}
        quarantine_config = history.get("quarantine_config") or {}
        audit = None
        if mode == "APPLY":
            approved = json.loads(APPROVAL_FILE.read_text())
            expected_fingerprint = approved.get("plan_fingerprint")
            if approved.get("status") != "APPROVED" or approved.get("execution_scope") != "APPLY" or approved.get("execution_authorized") is not True:
                raise RuntimeError("APPLY approval is not valid.")
            if approval.get("plan_fingerprint") != expected_fingerprint:
                raise RuntimeError("History approval fingerprint does not match the approved file.")
            if approval.get("target_table") != approved.get("target_table") or approval.get("target_path") != approved.get("target_path"):
                raise RuntimeError("History approval target differs from approved target.")
            audit_file = AUDIT_DIR / f"{DATASET}_{execution_id}_apply.json"
            if not audit_file.is_file():
                raise RuntimeError(f"APPLY audit file missing: {audit_file}")
            audit = json.loads(audit_file.read_text())
            norm = lambda value: str(value or "").replace("`", "").replace("s3a://", "s3://").rstrip("/")
            if audit.get("status") != "COMMITTED_AND_VERIFIED" or audit.get("mode") != "APPLY":
                raise RuntimeError("Audit does not confirm COMMITTED_AND_VERIFIED APPLY.")
            if audit.get("invalid_count") != approved.get("expected_invalid_count") or audit.get("expected_count") != approved.get("expected_invalid_count"):
                raise RuntimeError("Audit record counts do not match approval.")
            if norm(audit.get("target_table")) != norm(approved.get("target_table")) or norm(audit.get("quarantine_path")) != norm(approved.get("target_path")):
                raise RuntimeError("Audit target differs from approved target.")
            if audit.get("s3_write_performed") is not True or audit.get("source_modified") is not False:
                raise RuntimeError("Audit does not confirm write completed and source unchanged.")

        summary.update({
            "execution_status": "SUCCESS",
            "approval_status": approval.get("status"),
            "approved_by": approval.get("approved_by"),
            "plan_fingerprint": approval.get("plan_fingerprint"),
            "invalid_count": (audit.get("invalid_count") if audit else dq_validation.get("invalid_count")),
            "quarantined_count": (audit.get("invalid_count", 0) if audit else 0),
            "target_table": get_target_table(quarantine_config),
            "quarantine_path": (audit.get("quarantine_path") if audit else None),
            "source_modified": False,
            "s3_write_performed": (True if audit else False),
            "completed_at": utc_now(),
            "error_type": None,
            "error_message": None,
            "audit_details": {
                "plan_file": str(PLAN_FILE),
                "history_file": str(
                    HISTORY_DIR / f"{execution_id}.json"
                ),
                "executor_status": executor_status,
                "executor_message": execution.get("message"),
                "executed": execution.get("executed") is True,
                "safety_validation": "PASSED",
                "subprocess_returncode": completed.returncode,
            },
        })

        # A failed database update must fail the task visibly.
        upsert_execution_summary(summary)

        logger.info(
            "%s completed and summary saved: %s",
            mode,
            execution_id,
        )
        return {
            "execution_id": execution_id,
            "mode": mode,
        }

    except Exception as exc:
        # Preserve any history fields that can safely be extracted.
        execution = (
            history.get("execution", {})
            if isinstance(history, dict)
            else {}
        )
        dq_validation = (
            history.get("dq_validation", {})
            if isinstance(history, dict)
            else {}
        )
        approval = (
            history.get("approval", {})
            if isinstance(history, dict)
            else {}
        )
        quarantine_config = (
            history.get("quarantine_config", {})
            if isinstance(history, dict)
            else {}
        )

        summary.update({
            "execution_status": "FAILED",
            "approval_status": approval.get("status"),
            "approved_by": approval.get("approved_by"),
            "plan_fingerprint": approval.get("plan_fingerprint"),
            "invalid_count": dq_validation.get("invalid_count"),
            "target_table": get_target_table(quarantine_config),
            "source_modified": execution.get("source_modification"),
            "s3_write_performed": execution.get("s3_write_performed"),
            "completed_at": utc_now(),
            "error_type": type(exc).__name__,
            "error_message": str(exc)[:2000],
            "audit_details": {
                "plan_file": str(PLAN_FILE),
                "history_file": str(
                    HISTORY_DIR / f"{execution_id}.json"
                ),
                "safety_validation": "FAILED_OR_INCOMPLETE",
                "subprocess_returncode": (
                    completed.returncode if completed else None
                ),
            },
        })

        try:
            upsert_execution_summary(summary)
        except Exception:
            logger.exception(
                "Could not persist FAILED summary for %s",
                execution_id,
            )
        raise



def validate_existing_apply_for_gold(**context):
    """Validate an existing committed APPLY without rerunning remediation."""
    from airflow.exceptions import AirflowException

    execution_id = (context["dag_run"].conf or {}).get("execution_id")
    if not execution_id:
        raise AirflowException(
            "GOLD_REFRESH_ONLY requires an existing execution_id."
        )

    history = read_history(execution_id)
    if not isinstance(history, dict):
        raise AirflowException(f"History not found for {execution_id!r}.")

    execution = history.get("execution", {})
    approval = history.get("approval", {})
    dq_validation = history.get("dq_validation", {})

    checks = {
        "dataset": execution.get("dataset") == DATASET,
        "action": execution.get("action") == ACTION,
        "mode": execution.get("mode") == "APPLY",
        "execution_id": execution.get("execution_id") == execution_id,
        "executed": execution.get("executed") is True,
        "status": execution.get("status") == "COMMITTED_AND_VERIFIED",
        "s3_write": execution.get("s3_write_performed") is True,
        "source_unchanged": execution.get("source_modification") is False,
        "approval_status": approval.get("status") == "APPROVED",
        "approval_scope": approval.get("execution_scope") == "APPLY",
        "authorized": approval.get("execution_authorized") is True,
        "approved_dataset": approval.get("dataset") == DATASET,
        "approved_action": approval.get("action") == ACTION,
        "approved_source_unchanged":
            approval.get("source_modification_authorized") is False,
        "invalid_count_matches":
            approval.get("expected_invalid_count")
            == dq_validation.get("invalid_count"),
    }

    failed_checks = [
        name for name, passed in checks.items() if not passed
    ]
    if failed_checks:
        raise AirflowException(
            f"Existing execution failed Gold recovery validation: {failed_checks}"
        )

    logger.info(
        "Validated existing execution %s; remediation will not run.",
        execution_id,
    )
    return {
        "execution_id": execution_id,
        "mode": "GOLD_REFRESH_ONLY",
    }



def should_refresh_gold_after_apply(**context):
    """Allow Gold refresh after APPLY or validated Gold-only recovery."""
    ti = context["ti"]
    run_conf = context["dag_run"].conf or {}
    requested_mode = str(run_conf.get("mode", "DRY_RUN")).upper()

    if requested_mode == "GOLD_REFRESH_ONLY":
        result = ti.xcom_pull(
            task_ids="validate_existing_apply_for_gold"
        )
        if (
            not isinstance(result, dict)
            or not result.get("execution_id")
            or result.get("mode") != "GOLD_REFRESH_ONLY"
        ):
            raise RuntimeError(
                "Gold-only recovery validation did not return execution metadata."
            )
        return True

    result = ti.xcom_pull(
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


def update_postgres_summary_after_gold_refresh(gold_outcome, **context):
    """Record the overall Gold outcome without ORM or callback-XCom lookups.

    The task's trigger rule supplies the outcome: ALL_SUCCESS for the success
    updater, ONE_FAILED for the failure updater. Trigger rules do not expose
    each upstream task's individual state, so failure details deliberately do
    not guess which Gold task failed.
    """
    sys.path.insert(0, str(PROJECT_ROOT))
    from agents.remediation.summary_store import upsert_execution_summary

    ti = context["ti"]
    dag_run = context["dag_run"]
    run_conf = dag_run.conf or {}
    requested_mode = str(run_conf.get("mode", "DRY_RUN")).upper()

    # DRY_RUN must not create or modify a Gold-refresh audit result.
    if requested_mode == "DRY_RUN":
        logger.info("DRY_RUN selected; no Gold refresh audit update is required.")
        return

    if requested_mode == "GOLD_REFRESH_ONLY":
        result = ti.xcom_pull(task_ids="validate_existing_apply_for_gold")
        expected_mode = "GOLD_REFRESH_ONLY"
    elif requested_mode == "APPLY":
        result = ti.xcom_pull(task_ids="execute_dry_run_and_write_summary")
        expected_mode = "APPLY"
    else:
        raise RuntimeError(f"Unsupported remediation mode: {requested_mode!r}")

    if (
        not isinstance(result, dict)
        or not result.get("execution_id")
        or result.get("mode") != expected_mode
    ):
        raise RuntimeError(
            f"Cannot update Gold summary: valid {expected_mode} "
            "execution metadata is missing."
        )

    gold_success = str(gold_outcome).upper() == "SUCCESS"
    if gold_success:
        payments_state = "success"
        summary_state = "success"
        status = "SUCCESS"
        error_type = None
        error_message = None
    else:
        # ONE_FAILED tells us at least one Gold task failed or was upstream-failed,
        # but not which one. Do not fabricate individual task states.
        payments_state = "failed_or_upstream_failed_not_individually_resolved"
        summary_state = "failed_or_upstream_failed_not_individually_resolved"
        status = "FAILED"
        error_type = "GoldRefreshFailed"
        error_message = (
            "At least one Gold refresh task failed or was upstream-failed; "
            "individual task states are not exposed by this trigger-rule path."
        )

    details = {
        "gold_refresh": {
            "payments_task_id": "refresh_gold_olist_order_payments",
            "payments_task_status": payments_state,
            "payments_output_table": "glue_catalog.datapilot.olist_order_payments",
            "order_summary_task_id": "refresh_gold_olist_order_summary",
            "order_summary_task_status": summary_state,
            "requested_mode": requested_mode,
            "order_summary_output_table": "glue_catalog.datapilot.olist_order_summary",
            "gold_refresh_status": status,
            "outcome_source": "Airflow trigger rule",
            "updated_at": utc_now(),
        }
    }
    summary = {
        "execution_id": result["execution_id"],
        "dag_id": context["dag"].dag_id,
        "dag_run_id": dag_run.run_id,
        "dataset": DATASET,
        "action": ACTION,
        # This audit row represents the original committed APPLY execution.
        "execution_mode": "APPLY",
        "execution_status": status,
        "error_type": error_type,
        "error_message": error_message,
        "audit_details": details,
    }
    upsert_execution_summary(summary)

    if not gold_success:
        raise RuntimeError(
            "Gold refresh failed; inspect audit_details.gold_refresh."
        )


with DAG(
    dag_id="datapilot_remediation_dry_run",
    description=(
        "Run Olist payment remediation in DRY_RUN by default; "
        "APPLY requires explicit trigger configuration and approval"
    ),
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "datapilot", "retries": 0},
    tags=["datapilot", "remediation", "dry-run", "apply", "audit"],
) as dag:
    

    route_mode = BranchPythonOperator(
        task_id="route_remediation_mode",
        python_callable=route_remediation_mode,
    )

    execute_and_summarize = PythonOperator(
        task_id="execute_dry_run_and_write_summary",
        python_callable=run_remediation_dry_run,
    )
    
    validate_existing_apply = PythonOperator(
        task_id="validate_existing_apply_for_gold",
        python_callable=validate_existing_apply_for_gold,
    )
    continue_to_gold_only_after_apply = ShortCircuitOperator(
        task_id="continue_to_gold_only_after_apply",
        python_callable=should_refresh_gold_after_apply,
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
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

    # Exactly one updater should run: success after both Gold tasks succeed,
    # failure when either Gold task fails. Neither updater queries Airflow's ORM.
    update_gold_refresh_summary = PythonOperator(
        task_id="update_postgres_summary_after_gold_refresh",
        python_callable=update_postgres_summary_after_gold_refresh,
        op_kwargs={"gold_outcome": "SUCCESS"},
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )
    update_gold_refresh_failure_summary = PythonOperator(
        task_id="update_postgres_summary_after_gold_failure",
        python_callable=update_postgres_summary_after_gold_refresh,
        op_kwargs={"gold_outcome": "FAILED"},
        trigger_rule=TriggerRule.ONE_FAILED,
    )
    route_mode >> execute_and_summarize
    route_mode >> validate_existing_apply

    execute_and_summarize >> continue_to_gold_only_after_apply
    validate_existing_apply >> continue_to_gold_only_after_apply

    continue_to_gold_only_after_apply >> refresh_gold_olist_order_payments
    refresh_gold_olist_order_payments >> refresh_gold_olist_order_summary

    refresh_gold_olist_order_payments >> update_gold_refresh_summary
    refresh_gold_olist_order_summary >> update_gold_refresh_summary
    refresh_gold_olist_order_payments >> update_gold_refresh_failure_summary
    refresh_gold_olist_order_summary >> update_gold_refresh_failure_summary
