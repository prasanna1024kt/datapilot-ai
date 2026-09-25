import argparse
import json
from pathlib import Path


# =========================================================
# Remediation Actions
# =========================================================

ALLOWED_ACTIONS = {
    "SOURCE_CORRECTION",
    "QUARANTINE_RECORDS",
    "TRANSFORMATION_FIX",
    "UPDATE_DQ_RULE",
    "NO_ACTION",
    "MANUAL_REVIEW",
}


# =========================================================
# Load RCA Result
# =========================================================

def load_rca_result(path: str) -> dict:

    with open(path, "r") as file:
        return json.load(file)


# =========================================================
# Validate RCA
# =========================================================

def validate_rca(rca: dict):

    required_fields = [
        "dataset",
        "dq_status",
        "classification",
        "root_cause",
        "recommendation",
        "remediation",
        "next_actions",
        "confidence",
    ]

    for field in required_fields:

        if field not in rca:
            raise ValueError(
                f"RCA result missing required field: {field}"
            )

    remediation = rca["remediation"]

    if not isinstance(remediation, dict):
        raise ValueError(
            "RCA remediation must be an object."
        )

    action = remediation.get("action")

    if action not in ALLOWED_ACTIONS:
        raise ValueError(
            f"Unsupported remediation action: {action}"
        )

    if "requires_approval" not in remediation:
        raise ValueError(
            "remediation.requires_approval is missing."
        )

    if "reason" not in remediation:
        raise ValueError(
            "remediation.reason is missing."
        )


# =========================================================
# Build Remediation Plan
# =========================================================

def build_remediation_plan(rca: dict) -> dict:

    remediation = rca["remediation"]

    plan = {
        "dataset": rca["dataset"],
        "classification": rca["classification"],
        "confidence": rca["confidence"],
        "dq_status": rca["dq_status"],

        "action": remediation["action"],

        "requires_approval": remediation[
            "requires_approval"
        ],

        "status": "PENDING_APPROVAL",

        "execution_mode": "DRY_RUN",

        "reason": remediation["reason"],

        "root_cause": rca["root_cause"],

        "recommendation": rca["recommendation"],

        "next_actions": rca["next_actions"],

        "execution": {
            "executed": False,
            "executed_by": None,
            "execution_result": None
        }
    }

    return plan


# =========================================================
# Save Plan
# =========================================================

def save_plan(
    path: str,
    plan: dict
):

    output_path = Path(path)

    output_path.parent.mkdir(
        parents=True,
        exist_ok=True
    )

    with open(
        output_path,
        "w"
    ) as file:

        json.dump(
            plan,
            file,
            indent=2
        )

    print(
        f"\nRemediation plan saved to: {path}"
    )


# =========================================================
# Print Plan
# =========================================================

def print_plan(plan: dict):

    print("\n")
    print("========================================")
    print("DATAPILOT AI REMEDIATION PLAN")
    print("========================================")

    print(
        f"Dataset           : "
        f"{plan['dataset']}"
    )

    print(
        f"DQ Status         : "
        f"{plan['dq_status']}"
    )

    print(
        f"Classification    : "
        f"{plan['classification']}"
    )

    print(
        f"Confidence        : "
        f"{plan['confidence']}"
    )

    print(
        f"Action            : "
        f"{plan['action']}"
    )

    print(
        f"Requires Approval : "
        f"{plan['requires_approval']}"
    )

    print(
        f"Execution Mode    : "
        f"{plan['execution_mode']}"
    )

    print(
        f"Status            : "
        f"{plan['status']}"
    )

    print(
        "\nReason:"
    )

    print(
        plan["reason"]
    )

    print(
        "\nRoot Cause:"
    )

    print(
        plan["root_cause"]
    )

    print(
        "\nRecommendation:"
    )

    print(
        plan["recommendation"]
    )

    print(
        "\nNext Actions:"
    )

    for action in plan["next_actions"]:

        print(
            f"  - {action}"
        )

    print(
        "\nExecution:"
    )

    print(
        f"  Executed : "
        f"{plan['execution']['executed']}"
    )

    print("========================================")


# =========================================================
# Main
# =========================================================

def main():

    parser = argparse.ArgumentParser(
        description=(
            "DataPilot AI Remediation Agent "
            "(Dry Run)"
        )
    )

    parser.add_argument(
        "--rca-result",
        required=True,
        help="Path to RCA JSON result."
    )

    parser.add_argument(
        "--output",
        required=True,
        help="Path for remediation plan."
    )

    args = parser.parse_args()

    # -----------------------------------------------------
    # Load RCA
    # -----------------------------------------------------

    rca = load_rca_result(
        args.rca_result
    )

    # -----------------------------------------------------
    # Validate
    # -----------------------------------------------------

    validate_rca(
        rca
    )

    # -----------------------------------------------------
    # Build plan
    # -----------------------------------------------------

    plan = build_remediation_plan(
        rca
    )

    # -----------------------------------------------------
    # Save
    # -----------------------------------------------------

    save_plan(
        path=args.output,
        plan=plan
    )

    # -----------------------------------------------------
    # Display
    # -----------------------------------------------------

    print_plan(
        plan
    )


if __name__ == "__main__":
    main()
