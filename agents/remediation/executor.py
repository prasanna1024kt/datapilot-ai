import argparse
import json
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]

APPROVALS_DIR = PROJECT_ROOT / "results" / "approvals"
REMEDIATION_DIR = PROJECT_ROOT / "results" / "remediation"

ALLOWED_EXECUTION_ACTIONS = {
    "QUARANTINE_RECORDS",
}


def load_json(path: Path) -> dict:
    if not path.exists():
        raise FileNotFoundError(f"File not found: {path}")

    with open(path) as file:
        return json.load(file)


def validate_approval(
    approval: dict,
    dataset: str,
    action: str,
) -> None:

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


def execute_quarantine(
    dataset: str,
    remediation_plan: dict,
) -> dict:

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


def execute(
    dataset: str,
    action: str,
) -> dict:

    remediation_path = (
        REMEDIATION_DIR / f"{dataset}.json"
    )

    approval_path = (
        APPROVALS_DIR / f"{dataset}.json"
    )

    remediation = load_json(remediation_path)
    approval = load_json(approval_path)

    validate_action(action)

    validate_approval(
        approval=approval,
        dataset=dataset,
        action=action,
    )

    if remediation.get("dataset") != dataset:
        raise RuntimeError(
            "Remediation plan dataset does not match requested dataset."
        )

    if remediation.get("action") != action:
        raise RuntimeError(
            "Remediation plan action does not match requested action."
        )

    if action == "QUARANTINE_RECORDS":
        execution_result = execute_quarantine(
            dataset=dataset,
            remediation_plan=remediation,
        )

    else:
        raise RuntimeError(
            f"No executor implemented for action: {action}"
        )

    result = {
        "dataset": dataset,
        "action": action,
        "approval": approval,
        "execution": execution_result,
    }

    output_path = (
        REMEDIATION_DIR
        / f"{dataset}_execution.json"
    )

    with open(output_path, "w") as file:
        json.dump(result, file, indent=2)

    return result


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