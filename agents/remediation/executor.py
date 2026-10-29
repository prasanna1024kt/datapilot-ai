import argparse
import json
from pathlib import Path
import subprocess
from datetime import datetime, timezone
import uuid
import os
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[2]

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from agents.remediation.plan_fingerprint import validate_plan_fingerprint

APPROVALS_DIR = PROJECT_ROOT / "results" / "approvals"
REMEDIATION_DIR = PROJECT_ROOT / "results" / "remediation"


ALLOWED_EXECUTION_ACTIONS = {
    "QUARANTINE_RECORDS",
}

def validate_remediation_plan(remediation: dict, dataset: str, action: str) -> None:

    if remediation.get("dataset") != dataset:
        raise RuntimeError("Remediation plan dataset mismatch.")

    if remediation.get("action") != action:
        raise RuntimeError("Remediation plan action mismatch.")

    if remediation.get("requires_approval") is not True:
        raise RuntimeError("Plan does not require the expected approval.")

    if remediation.get("status") != "PENDING_APPROVAL":
        raise RuntimeError(
            "Plan is not awaiting approval. "
            f"Current status: {remediation.get('status')}"
        )

    if remediation.get("execution_mode") != "DRY_RUN":
        raise RuntimeError("Unexpected remediation execution mode.")


def load_json(path: Path) -> dict:
    if not path.exists():
        raise FileNotFoundError(f"File not found: {path}")

    with open(path) as file:
        return json.load(file)


DQ_RESULTS_DIR = PROJECT_ROOT / "results" / "dq"
CONFIG_DIR = PROJECT_ROOT / "config"


def validate_dq_failure(dataset: str) -> dict:
    """Validate the failed DQ range check without executing remediation."""
    if dataset != "olist_order_payments":
        raise RuntimeError("Dataset is not enabled for remediation.")

    dq_report = load_json(DQ_RESULTS_DIR / f"{dataset}.json")
    dq_config_path = CONFIG_DIR / "dq" / f"{dataset}.yaml"

    if dq_report.get("dataset") != dataset:
        raise RuntimeError("DQ report dataset mismatch.")

    if dq_report.get("status") != "FAIL":
        raise RuntimeError("DQ report does not show a failure.")

    if not dq_config_path.is_file():
        raise FileNotFoundError("DQ configuration file not found.")

    import yaml

    with dq_config_path.open("r", encoding="utf-8") as file:
        dq_config = yaml.safe_load(file)

    rule = (
        dq_config.get("checks", {})
        .get("range_checks", {})
        .get("payment_installments")
    )

    if not rule:
        raise RuntimeError("payment_installments range rule not found.")

    expected_min = rule.get("min")
    expected_max = rule.get("max")

    failed_checks = [
        check
        for check in dq_report.get("checks", [])
        if check.get("check") == "range_check:payment_installments"
    ]

    if len(failed_checks) != 1:
        raise RuntimeError(
            "Expected exactly one payment_installments range check."
        )

    check = failed_checks[0]

    if check.get("status") != "FAIL":
        raise RuntimeError("The installment range check is not failing.")

    if check.get("invalid_count") != check.get("actual"):
        raise RuntimeError("DQ invalid_count and actual count disagree.")

    if check.get("invalid_count") != 2:
        raise RuntimeError(
            "Invalid count differs from the currently approved count of 2."
        )

    if expected_min != 1 or expected_max != 100:
        raise RuntimeError(
            "Configured range differs from the expected rule 1–100."
        )

    return {
        "dataset": dataset,
        "dq_status": dq_report["status"],
        "check": check["check"],
        "invalid_count": check["invalid_count"],
        "minimum": expected_min,
        "maximum": expected_max,
        "validated": True,
    }


def validate_approval(approval: dict,dataset: str,action: str,remediation: dict,mode: str,) -> None:
    """Validate approval identity, plan fingerprint, and execution scope."""

    if approval.get("dataset") != dataset:
        raise RuntimeError("Approval dataset does not match requested dataset.")

    if approval.get("action") != action:
        raise RuntimeError("Approval action does not match requested action.")

    if approval.get("status") != "APPROVED":
        raise RuntimeError(
            f"Remediation is not approved. "
            f"Current status: {approval.get('status')}"
        )

    if approval.get("execution_authorized") is not True:
        raise RuntimeError(
            "Execution is not authorized by the approval record."
        )

    try:
        fingerprint = validate_plan_fingerprint(remediation)
    except ValueError as exc:
        raise RuntimeError(str(exc)) from exc
    if approval.get("plan_fingerprint") != fingerprint:
        raise RuntimeError(
            "Approval fingerprint does not match the current plan."
        )
    if mode not in {"DRY_RUN", "APPLY"}:
        raise RuntimeError("Unsupported execution mode for approval.")

    expected_scope = (
        "DRY_RUN_ONLY" if mode == "DRY_RUN" else "APPLY"
    )
    if approval.get("execution_scope") != expected_scope:
        raise RuntimeError(
            f"Approval scope must be {expected_scope} for {mode} execution."
        )

def validate_action(action: str) -> None:
    if action not in ALLOWED_EXECUTION_ACTIONS:
        raise RuntimeError(
            f"Action '{action}' is not currently executable. "
            f"Allowed actions: {sorted(ALLOWED_EXECUTION_ACTIONS)}"
        )


def validate_quarantine_config(dataset: str) -> dict:
    """Validate the quarantine configuration without performing writes."""
    if dataset != "olist_order_payments":
        raise RuntimeError("Dataset is not enabled for remediation.")

    config_path = CONFIG_DIR / "quarantine" / f"{dataset}.yaml"
    if not config_path.is_file():
        raise FileNotFoundError(
            f"Quarantine configuration not found: {config_path}"
        )

    import yaml

    with config_path.open("r", encoding="utf-8") as file:
        config = yaml.safe_load(file)

    if not isinstance(config, dict):
        raise RuntimeError("Quarantine configuration must be a YAML mapping.")

    if config.get("dataset") != dataset:
        raise RuntimeError("Quarantine configuration dataset mismatch.")

    quarantine = config.get("quarantine")
    if not isinstance(quarantine, dict):
        raise RuntimeError("Quarantine settings are missing or invalid.")

    expected_values = {
        "enabled": True,
        "catalog": "glue_catalog",
        "database": "datapilot",
        "table": "olist_order_payments_quarantine",
        "format": "iceberg",
        "source_layer": "silver",
        "target_layer": "quarantine",
        "keys": ["order_id", "payment_sequential"],
        "validate_source_duplicates": True,
    }

    for key, expected in expected_values.items():
        if quarantine.get(key) != expected:
            raise RuntimeError(
                f"Invalid quarantine configuration for '{key}': "
                f"expected {expected!r}, got {quarantine.get(key)!r}."
            )

    return {
        "dataset": dataset,
        "validated": True,
        "enabled": True,
        "catalog": quarantine["catalog"],
        "database": quarantine["database"],
        "table": quarantine["table"],
        "format": quarantine["format"],
        "source_layer": quarantine["source_layer"],
        "target_layer": quarantine["target_layer"],
        "keys": quarantine["keys"],
        "validate_source_duplicates": quarantine[
            "validate_source_duplicates"
        ],
    }

def execute_quarantine(dataset: str, remediation_plan: dict) -> dict:

    """
    Dry-run quarantine executor.

    This intentionally does NOT modify S3 or source data yet.
    It creates an execution/audit record showing what would be
    quarantined.
    """

    execution_result = {
        "action": "QUARANTINE_RECORDS",
        "dataset": dataset,
        "executed": False,
        "mode": "DRY_RUN",
        "message": (
            "Quarantine execution is authorized, but actual data "
            "movement is not enabled yet."
        ),
        "source_modification": False,
    }

    return execution_result


def write_execution_history(result: dict) -> Path:
    """Persist one execution audit without overwriting an existing record."""

    execution = result.get("execution", {})
    execution_id = execution.get("execution_id")

    if not isinstance(execution_id, str) or not execution_id:
        raise ValueError("Execution audit requires a valid execution_id.")

    if any(
        char not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_"
        for char in execution_id
    ):
        raise ValueError("Execution ID contains unsupported characters.")

    history_dir = REMEDIATION_DIR / "history"
    history_dir.mkdir(parents=True, exist_ok=True)

    history_path = history_dir / f"{execution_id}.json"
    temp_path = history_dir / f".{execution_id}.{uuid.uuid4().hex}.tmp"

    try:
        with temp_path.open("x", encoding="utf-8") as file:
            json.dump(result, file, indent=2)
            file.write("\n")
            file.flush()
            os.fsync(file.fileno())

        # Atomic publication; fails if the destination already exists.
        os.link(temp_path, history_path)
        return history_path

    finally:
        try:
            temp_path.unlink()
        except FileNotFoundError:
            pass

def write_validation_failure(dataset: str,action: str,execution_id: str, stage: str, error: Exception, mode: str = "DRY_RUN",) -> Path:
    """Record a validation failure without invoking Spark or modifying data."""
    result = {
        "dataset": dataset,
        "action": action,
        "execution": {
            "action": action,
            "dataset": dataset,
            "execution_id": execution_id,
            "status": "FAILED",
            "failure_stage": stage,
            "message": f"Validation failed during {stage}.",
            "error": f"{type(error).__name__}: {error}",
            "mode": mode,
            "executed": False,
            "source_modification": False,
            "s3_write_performed": False,
        },
    }
    try:
        return write_execution_history(result)
    except Exception as audit_error:
        # Preserve the original validation exception as the primary error.
        if hasattr(error, "add_note"):
            error.add_note(
                "Additionally, writing the validation-failure audit failed: "
                f"{type(audit_error).__name__}: {audit_error}"
            )
        raise error from audit_error


def execute(dataset: str, action: str,mode: str = "DRY_RUN",confirm_write: bool = False,plan_file: str | None = None,allow_unapproved_dry_run: bool = False,execution_id: str | None = None,) -> dict:

    if dataset != "olist_order_payments":
        raise RuntimeError("Dataset is not enabled for remediation.")
    if mode not in {"DRY_RUN", "APPLY"}:
        raise RuntimeError("Execution mode must be DRY_RUN or APPLY.")
    if mode == "APPLY" and confirm_write is not True:
        raise RuntimeError("APPLY requires explicit --confirm-write.")
    if mode == "DRY_RUN" and confirm_write:
        raise RuntimeError("--confirm-write is only valid with --mode APPLY.")
    if execution_id is None:
        execution_id = (
            f"quarantine-{dataset}-"
            f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S')}-"
            f"{uuid.uuid4().hex[:8]}"
        )
    elif (
        not isinstance(execution_id, str)
        or not execution_id
        or any(
            char not in
            "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_"
            for char in execution_id
        )
    ):
        raise ValueError(
            "Execution ID must contain only letters, digits, hyphens, and underscores."
        )

    
    remediation_path = (
        Path(plan_file).resolve()
        if plan_file
        else REMEDIATION_DIR / f"{dataset}.json"
    )
    approval_filename = (
        f"{dataset}_apply.json"
        if mode == "APPLY"
        else f"{dataset}.json"
    )
    approval_path = APPROVALS_DIR / approval_filename

    if allow_unapproved_dry_run and mode != "DRY_RUN":
        raise RuntimeError(
            "Unapproved execution is allowed only in DRY_RUN mode."
        )

    try:
        remediation = load_json(remediation_path)

        if allow_unapproved_dry_run:
            approval = {
                "status": "NOT_REQUIRED_FOR_DRY_RUN",
                "approved_by": None,
            }
        else:
            approval = load_json(approval_path)
    except Exception as exc:
        write_validation_failure(dataset, action, execution_id, "input_loading", exc, mode=mode)
        raise
    validations = [("action_validation", lambda: validate_action(action)),]
    if not allow_unapproved_dry_run:
        validations.append(("approval_validation",lambda: validate_approval(approval, dataset, action, remediation, mode),))
    validations.extend([("plan_fingerprint_validation",lambda: validate_plan_fingerprint(remediation),),
        ("plan_validation",lambda: validate_remediation_plan(remediation, dataset, action),),
        (
            "quarantine_config_validation",
            lambda: validate_quarantine_config(dataset),
        ),
        (
            "dq_validation",
            lambda: validate_dq_failure(dataset),
        ),
    ])
    dq_validation = None
    quarantine_config = None

    for stage, validate in validations:
        try:
            validation_result = validate()

            if stage == "dq_validation":
                dq_validation = validation_result
            elif stage == "quarantine_config_validation":
                quarantine_config = validation_result

        except Exception as exc:
            write_validation_failure(
                dataset, action, execution_id, stage, exc, mode=mode,
            )
            raise

    if action != "QUARANTINE_RECORDS":
        raise RuntimeError(f"No executor implemented for action: {action}")

    try:
        if mode == "DRY_RUN":
            spark_report = run_quarantine_dry_run(
                dataset=dataset,
                dq_validation=dq_validation,
                execution_id=execution_id,
            )
        else:
            spark_report = run_quarantine_apply(
                dataset=dataset,
                dq_validation=dq_validation,
                execution_id=execution_id,
                approval_path=approval_path,
                plan_path=remediation_path,
                quarantine_config=quarantine_config,
            )
    except Exception as exc:
        # A failed APPLY subprocess can fail after Iceberg has committed. Unless
        # a successful audit proves otherwise, the write outcome is UNKNOWN.
        apply_outcome_unknown = mode == "APPLY"
        failure_result = {
            "dataset": dataset,
            "action": action,
            "approval": approval,
            "dq_validation": dq_validation,
            "execution": {
                "action": action,
                "dataset": dataset,
                "executed": False,
                "mode": mode,
                "execution_id": execution_id,
                "status": "APPLY_OUTCOME_UNKNOWN" if apply_outcome_unknown else "FAILED",
                "message": (
                    "Apply did not return a verified success report. Inspect the target table and Spark logs before retrying."
                    if apply_outcome_unknown else "Spark quarantine dry-run failed."
                ),
                "error": f"{type(exc).__name__}: {exc}",
                "source_modification": None if apply_outcome_unknown else False,
                "s3_write_performed": None if apply_outcome_unknown else False,
            },
        }

        try:
            write_execution_history(failure_result)
        except Exception as audit_error:
            if hasattr(exc, "add_note"):
                exc.add_note(
                    "Additionally, writing the Spark-failure audit failed: "
                    f"{type(audit_error).__name__}: {audit_error}"
                )
            raise exc from audit_error
        raise

    is_apply = mode == "APPLY"
    execution_result = {
        "action": action,
        "dataset": dataset,
        "executed": is_apply,
        "status": "COMMITTED_AND_VERIFIED" if is_apply else "VALIDATED",
        "mode": mode,
        "execution_id": execution_id,
        "message": (
            "Quarantine records were committed and verified. Silver remains unchanged."
            if is_apply else
            "Spark dry-run validated successfully. Actual quarantine writes remain disabled."
        ),
        "source_modification": False,
        "s3_write_performed": is_apply,
        "spark_validation": spark_report,
    }

    result = {
        "dataset": dataset,
        "action": action,
        "approval": approval,
        "dq_validation": dq_validation,
        "quarantine_config": quarantine_config,
        "execution": execution_result,
    }

    history_path = write_execution_history(result)

    output_path = REMEDIATION_DIR / f"{dataset}_execution.json"
    temp_path = output_path.with_name(
        f".{output_path.name}.{uuid.uuid4().hex}.tmp"
    )

    try:
        with temp_path.open("x", encoding="utf-8") as file:
            json.dump(result, file, indent=2)
            file.write("\n")
            file.flush()
            os.fsync(file.fileno())

        os.replace(temp_path, output_path)

    except Exception as exc:
        raise RuntimeError(
            f"{mode} execution finished and per-execution history "
            f"was recorded at {history_path}, but latest-execution "
            f"summary publication failed: "
            f"{type(exc).__name__}: {exc}"
        ) from exc

    finally:
        try:
            temp_path.unlink()
        except FileNotFoundError:
            pass

    return result

def run_quarantine_dry_run(dataset: str,dq_validation: dict,execution_id: str,) -> dict:
    """Run and validate the Spark quarantine job in dry-run mode."""

    import subprocess

    if dataset != "olist_order_payments":
        raise RuntimeError("Dataset is not enabled for remediation.")

    if dq_validation.get("validated") is not True:
        raise RuntimeError("DQ validation has not passed.")

    if dq_validation.get("invalid_count") != 2:
        raise RuntimeError("Unexpected invalid record count.")

    container_job = Path("/opt/spark-apps/jobs/quarantine_records.py")
    container_audit_dir = Path("/opt/spark-apps/results/quarantine")

    if container_job.is_file():
        # MCP container: submit directly to the Spark cluster.
        command = [
            "/opt/spark/bin/spark-submit",
            "--master",
            "spark://spark-master:7077",
            str(container_job),
        ]
        audit_dir = container_audit_dir
        cwd = "/opt/datapilot"
    else:
        # Mac host: use the existing Compose Spark container.
        command = [
            "docker-compose",
            "exec",
            "-T",
            "spark",
            "/opt/spark/bin/spark-submit",
            "/opt/spark-apps/jobs/quarantine_records.py",
        ]
        audit_dir = (
            PROJECT_ROOT / "spark" / "results" / "quarantine"
        )
        cwd = str(PROJECT_ROOT)

    command.extend([
        "--dataset", dataset,
        "--execution-id", execution_id,
        "--expected-count", str(dq_validation["invalid_count"]),
    ])

    completed = subprocess.run(
        command,
        cwd=cwd,
        capture_output=True,
        text=True,
        timeout=300,
        check=False,
    )

    if completed.returncode != 0:
        raise RuntimeError(
            "Spark quarantine dry-run failed. "
            f"Exit code: {completed.returncode}. "
            "Inspect Spark or MCP container logs."
        )

    audit_path = (
        audit_dir / f"{dataset}_{execution_id}_dry_run.json"
    )

    if not audit_path.is_file():
        raise RuntimeError(
            f"Spark audit file not found: {audit_path}"
        )

    with audit_path.open("r", encoding="utf-8") as file:
        report = json.load(file)

    if (
        report.get("dataset") != dataset
        or report.get("execution_id") != execution_id
        or report.get("mode") != "DRY_RUN"
        or report.get("status") != "VALIDATED"
        or report.get("invalid_count") != dq_validation["invalid_count"]
        or report.get("s3_write_performed") is not False
        or report.get("source_modified") is not False
    ):
        raise RuntimeError(
            "Spark dry-run audit failed safety validation."
        )

    return report

def get_expected_quarantine_destination(dataset: str, quarantine_config: dict) -> tuple[str, str]:
    """Derive the approved Iceberg table and S3 destination."""

    import yaml

    variables_path = CONFIG_DIR / "variables.yaml"

    if not variables_path.is_file():
        raise FileNotFoundError(
            f"Central variables file not found: {variables_path}"
        )

    with variables_path.open("r", encoding="utf-8") as file:
        variables = yaml.safe_load(file)

    if not isinstance(variables, dict):
        raise RuntimeError("Variables configuration must be a mapping.")

    bucket = variables.get("storage", {}).get("bucket")
    layers = variables.get("storage", {}).get("layers", {})

    quarantine_layer = layers.get(
        quarantine_config["target_layer"]
    )

    if not bucket or not quarantine_layer:
        raise RuntimeError(
            "Bucket or quarantine layer is missing from configuration."
        )

    expected_table = (
        f"{quarantine_config['catalog']}."
        f"{quarantine_config['database']}."
        f"{quarantine_config['table']}"
    )

    expected_path = (
        f"s3://{bucket}/"
        f"{quarantine_layer.strip('/')}/"
        f"{dataset}"
    )

    return expected_table, expected_path


def normalize_table_identifier(value: object) -> str:
    """Normalize optionally quoted multipart SQL table identifiers."""
    if not isinstance(value, str):
        return ""
    return ".".join(
        part.strip().strip("`").strip('"')
        for part in value.strip().split(".")
    )


def normalize_s3_uri(value: object) -> str:
    """Normalize S3 and S3A URI schemes for destination comparison."""
    if not isinstance(value, str):
        return ""
    normalized = value.strip()
    if normalized.startswith("s3a://"):
        normalized = "s3://" + normalized[len("s3a://"):]
    return normalized.rstrip("/")


def run_quarantine_apply(dataset: str, dq_validation: dict, execution_id: str, approval_path: Path, plan_path: Path, quarantine_config: dict) -> dict:

    """Invoke the explicitly confirmed Spark APPLY job and verify its audit."""
    if dataset != "olist_order_payments":
        raise RuntimeError("Dataset is not enabled for remediation.")
    if dq_validation.get("validated") is not True or dq_validation.get("invalid_count") != 2:
        raise RuntimeError("DQ validation is not the expected approved result.")

    container_job = Path("/opt/spark-apps/jobs/quarantine_records_candidate.py")
    container_audit_dir = Path("/opt/spark-apps/results/quarantine")

    if container_job.is_file():
        command = [
            "/opt/spark/bin/spark-submit",
            "--master", "spark://spark-master:7077",
            str(container_job),
        ]
        audit_dir = container_audit_dir
        cwd = "/opt/datapilot"
    else:
        command = [
            "docker-compose", "exec", "-T", "spark",
            "/opt/spark/bin/spark-submit",
            "/opt/spark-apps/jobs/quarantine_records_candidate.py",
        ]
        audit_dir = PROJECT_ROOT / "spark" / "results" / "quarantine"
        cwd = str(PROJECT_ROOT)

    command.extend([
        "--dataset", dataset,
        "--execution-id", execution_id,
        "--expected-count", str(dq_validation["invalid_count"]),
        "--mode", "APPLY",
        "--confirm-write",
        "--approval-file", str(approval_path),
        "--plan-file", str(plan_path),
    ])
    completed = subprocess.run(
        command, cwd=cwd, capture_output=True, text=True, timeout=600, check=False
    )
    audit_path = audit_dir / f"{dataset}_{execution_id}_apply.json"
    if completed.returncode != 0:
        # Preserve bounded diagnostics in execution history. Do not retry APPLY
        # automatically: Spark may have committed before the process failed.
        stdout_tail = (completed.stdout or "")[-6000:]
        stderr_tail = (completed.stderr or "")[-6000:]
        raise RuntimeError(
            "Spark APPLY did not exit successfully. The write outcome may "
            "be unknown; inspect Iceberg and Spark logs before retrying. "
            f"Exit code: {completed.returncode}.\n"
            f"STDOUT (last 6000 chars):\n{stdout_tail}\n"
            f"STDERR (last 6000 chars):\n{stderr_tail}"
        )

    if not audit_path.is_file():
        raise RuntimeError(
            "Spark APPLY returned success but its audit report is missing; "
            "the write outcome cannot be confirmed."
        )

    with audit_path.open("r", encoding="utf-8") as file:
        report = json.load(file)
    expected_table, expected_path = get_expected_quarantine_destination(
        dataset,
        quarantine_config,
    )
    if normalize_table_identifier(report.get("target_table")) != normalize_table_identifier(expected_table):
        raise RuntimeError(
            "Spark APPLY audit target table mismatch. "
            "Inspect the target before retrying."
        )

    if normalize_s3_uri(report.get("quarantine_path")) != normalize_s3_uri(expected_path):
        raise RuntimeError(
            "Spark APPLY audit quarantine path mismatch. "
            "Inspect the target before retrying."
        )
    if (
        report.get("dataset") != dataset
        or report.get("execution_id") != execution_id
        or report.get("mode") != "APPLY"
        or report.get("status") != "COMMITTED_AND_VERIFIED"
        or report.get("invalid_count") != dq_validation["invalid_count"]
        or report.get("s3_write_performed") is not True
        or report.get("source_modified") is not False
    ):
        raise RuntimeError(
            "Spark APPLY audit failed safety validation; "
            "inspect the target before retrying."
        )
    return report

def print_result(result: dict) -> None:

    print("\n" + "=" * 40)
    print("DATAPILOT AI REMEDIATION EXECUTION")
    print("=" * 40)

    print(f"Dataset          : {result['dataset']}")
    print(f"Action           : {result['action']}")
    print(
        f"Approved By      : "
        f"{result['approval'].get('approved_by')}"
    )
    print(
        f"Approval Status  : "
        f"{result['approval'].get('status')}"
    )

    execution = result["execution"]

    print(
        f"Executed         : "
        f"{execution.get('executed')}"
    )

    print(
        f"Execution Mode   : "
        f"{execution.get('mode')}"
    )

    print("\nMessage:")
    print(execution.get("message"))

    print(
        f"\nSource Modified  : "
        f"{execution.get('source_modification')}"
    )

    print("=" * 40)


def main():

    parser = argparse.ArgumentParser(
        description="DataPilot AI remediation executor"
    )

    parser.add_argument(
        "--dataset",
        required=True,
    )

    parser.add_argument(
        "--action",
        required=True,
        choices=[
            "QUARANTINE_RECORDS",
            "SOURCE_CORRECTION",
            "TRANSFORMATION_FIX",
            "UPDATE_DQ_RULE",
            "NO_ACTION",
            "MANUAL_REVIEW",
        ],
    )

    parser.add_argument(
        "--mode", choices=["DRY_RUN", "APPLY"], default="DRY_RUN",
        help="DRY_RUN is the default; APPLY requires --confirm-write.",
    )
    parser.add_argument(
        "--confirm-write", action="store_true",
        help="Explicitly authorize persistent quarantine writes with --mode APPLY.",
    )

    parser.add_argument(
        "--plan-file",
        help="Optional path to the remediation plan JSON.",
    )
    parser.add_argument(
        "--execution-id",
        help="Optional execution ID supplied by the orchestrator.",
    )
    parser.add_argument(
        "--allow-unapproved-dry-run",
        action="store_true",
        help="Skip approval validation only for a fingerprint-validated DRY_RUN.",
    )
    args = parser.parse_args()
    result = execute(
    dataset=args.dataset,
    action=args.action,
    mode=args.mode,
    confirm_write=args.confirm_write,
    plan_file=args.plan_file,
    allow_unapproved_dry_run=args.allow_unapproved_dry_run,
    execution_id=args.execution_id,
    )
    print_result(result)


if __name__ == "__main__":
    main()