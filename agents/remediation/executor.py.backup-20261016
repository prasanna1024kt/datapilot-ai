import argparse
import json
from pathlib import Path
import subprocess
from datetime import datetime, timezone
import uuid

PROJECT_ROOT = Path(__file__).resolve().parents[2]
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

def validate_approval(approval: dict,dataset: str,action: str,) -> None:

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


def validate_action(action: str) -> None:
    if action not in ALLOWED_EXECUTION_ACTIONS:
        raise RuntimeError(
            f"Action '{action}' is not currently executable. "
            f"Allowed actions: {sorted(ALLOWED_EXECUTION_ACTIONS)}"
        )


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


def execute(dataset: str, action: str) -> dict:
    import uuid
    from datetime import datetime, timezone

    remediation_path = REMEDIATION_DIR / f"{dataset}.json"
    approval_path = APPROVALS_DIR / f"{dataset}.json"

    remediation = load_json(remediation_path)
    approval = load_json(approval_path)

    validate_action(action)
    validate_approval(approval, dataset, action)
    validate_remediation_plan(remediation, dataset, action)

    dq_validation = validate_dq_failure(dataset)

    execution_id = (
        f"quarantine-{dataset}-"
        f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S')}-"
        f"{uuid.uuid4().hex[:8]}"
    )

    if action != "QUARANTINE_RECORDS":
        raise RuntimeError(f"No executor implemented for action: {action}")

    spark_report = run_quarantine_dry_run(
        dataset=dataset,
        dq_validation=dq_validation,
        execution_id=execution_id,
    )

    execution_result = {
        "action": action,
        "dataset": dataset,
        "executed": False,
        "mode": "DRY_RUN",
        "execution_id": execution_id,
        "message": (
            "Spark dry-run validated successfully. "
            "Actual quarantine writes remain disabled."
        ),
        "source_modification": False,
        "s3_write_performed": False,
        "spark_validation": spark_report,
    }

    result = {
        "dataset": dataset,
        "action": action,
        "approval": approval,
        "dq_validation": dq_validation,
        "execution": execution_result,
    }

    output_path = REMEDIATION_DIR / f"{dataset}_execution.json"
    temp_path = output_path.with_suffix(".json.tmp")

    with temp_path.open("w", encoding="utf-8") as file:
        json.dump(result, file, indent=2)
        file.write("\n")
        file.flush()
        import os
        os.fsync(file.fileno())

    temp_path.replace(output_path)

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

    args = parser.parse_args()

    result = execute(
        dataset=args.dataset,
        action=args.action,
    )

    print_result(result)


if __name__ == "__main__":
    main()