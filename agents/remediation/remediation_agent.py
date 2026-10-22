import argparse
import json
from pathlib import Path
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from agents.remediation.plan_fingerprint import attach_plan_fingerprint
# =========================================================
# Remediation Actions
# =========================================================

ALLOWED_ACTIONS = {
    "QUARANTINE_RECORDS",
    "TRANSFORMATION_FIX",
    "UPDATE_DQ_RULE",
    "NO_ACTION",
    "MANUAL_REVIEW",
}

ALLOWED_CONFIDENCE = {
    "HIGH",
    "MEDIUM",
    "LOW",
}


# =========================================================
# Load RCA Result
# =========================================================

def load_rca_result(path: str) -> dict:

    with open(path, "r", encoding="utf-8") as file:
        return json.load(file)


# =========================================================
# Validate RCA
# =========================================================

def validate_rca(rca: dict) -> None:

    if not isinstance(rca, dict):
        raise ValueError(
            "RCA result must be a JSON object."
        )

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
            raise ValueError(f"RCA result missing required field: {field}")

    # -----------------------------------------------------
    # DQ status
    # -----------------------------------------------------

    if rca["dq_status"] != "FAIL":
        raise ValueError(
            "Remediation requires dq_status = FAIL."
        )

    # -----------------------------------------------------
    # Basic field validation
    # -----------------------------------------------------

    if not isinstance(rca["dataset"], str) or not rca["dataset"].strip():

        raise ValueError(
            "RCA dataset must be a non-empty string."
        )

    if not isinstance(rca["root_cause"], str) or not rca["root_cause"].strip():

        raise ValueError(
            "RCA root_cause must be a non-empty string."
        )

    if not isinstance(rca["recommendation"], str) or not rca["recommendation"].strip():

        raise ValueError(
            "RCA recommendation must be a non-empty string."
        )

    # -----------------------------------------------------
    # Confidence
    # -----------------------------------------------------

    if rca["confidence"] not in ALLOWED_CONFIDENCE:

        raise ValueError("Unsupported RCA confidence: " f"{rca['confidence']}")

    # -----------------------------------------------------
    # Next actions
    # -----------------------------------------------------

    if not isinstance(rca["next_actions"], list):

        raise ValueError("RCA next_actions must be a list.")

    # -----------------------------------------------------
    # Remediation
    # -----------------------------------------------------

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

    # -----------------------------------------------------
    # Approval
    # -----------------------------------------------------

    if "requires_approval" not in remediation:

        raise ValueError(
            "remediation.requires_approval is missing."
        )

    if not isinstance(
        remediation["requires_approval"],
        bool
    ):

        raise ValueError(
            "remediation.requires_approval "
            "must be boolean."
        )

    # -----------------------------------------------------
    # Reason
    # -----------------------------------------------------

    if "reason" not in remediation:

        raise ValueError(
            "remediation.reason is missing."
        )

    if not isinstance(
        remediation["reason"],
        str
    ) or not remediation["reason"].strip():

        raise ValueError(
            "remediation.reason must be "
            "a non-empty string."
        )

    # -----------------------------------------------------
    # DQ rule changes always require approval
    # -----------------------------------------------------

    if action == "UPDATE_DQ_RULE":

        if remediation["requires_approval"] is not True:

            raise ValueError(
                "UPDATE_DQ_RULE must require "
                "human approval."
            )


# =========================================================
# Build Remediation Plan
# =========================================================

def build_remediation_plan(
    rca: dict
) -> dict:

    remediation = rca["remediation"]

    action = remediation["action"]

    plan = {
        "dataset": rca["dataset"],

        "classification": (rca["classification"]),

        "confidence": (rca["confidence"]),

        "dq_status": (rca["dq_status"]),

        "action": action,

        "requires_approval": (remediation["requires_approval"]),

        "status": "PENDING_APPROVAL",

        "execution_mode": "DRY_RUN",

        "reason": (remediation["reason"]),

        "root_cause": (rca["root_cause"]),

        "recommendation": (rca["recommendation"]),

        "next_actions": (rca["next_actions"]),

        # -------------------------------------------------
        # Governance
        # -------------------------------------------------

        "business_justification_required": (
            action == "UPDATE_DQ_RULE"
        ),

        # -------------------------------------------------
        # Rule change is intentionally empty for now.
        #
        # It must be supplied by the remediation reasoning
        # or verified business proposal. We must not guess
        # the new business rule.
        # -------------------------------------------------

        "rule_change": None,

        # -------------------------------------------------
        # Execution audit
        # -------------------------------------------------

        "execution": {
            "executed": False,
            "executed_by": None,
            "execution_result": None
        }
    }

    return attach_plan_fingerprint(plan)
# =========================================================
# Save Plan
# =========================================================

def save_plan(path: str, plan: dict) -> None:
    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    plan = attach_plan_fingerprint(plan)

    with output_path.open("w", encoding="utf-8") as file:
        json.dump(plan, file, indent=2, ensure_ascii=False, allow_nan=False)

    print(f"\nRemediation plan saved to: {path}")


# =========================================================
# Print Plan
# =========================================================

def print_plan(plan: dict) -> None:

    print()
    print("=" * 50)
    print("DATAPILOT AI REMEDIATION PLAN")
    print("=" * 50)

    print(f"Dataset           : {plan['dataset']}")

    print(f"DQ Status         : {plan['dq_status']}")

    print(f"Classification    : {plan['classification']}")

    print(f"Confidence        : {plan['confidence']}")

    print(f"Action            : {plan['action']}")

    print(f"Requires Approval : {plan['requires_approval']}")

    print(f"Business Rule     : {plan['business_justification_required']}")

    print(f"Execution Mode    : {plan['execution_mode']}")

    print(f"Status            : {plan['status']}")

    print("\nReason:")

    print( plan["reason"])

    print("\nRoot Cause:")

    print(plan["root_cause"])

    print("\nRecommendation:")

    print(plan["recommendation"])

    print("\nRule Change:")

    if plan["rule_change"]:

        print(json.dumps(plan["rule_change"], indent=2))

    else:

        print(
            "Not specified. "
            "Business validation required."
        )

    print("\nNext Actions:")

    for action in plan["next_actions"]:

        print(f"  - {action}")

    print("\nExecution:")

    print(
        f"  Executed : "
        f"{plan['execution']['executed']}"
    )

    print("=" * 50)

# =========================================================
# Main
# =========================================================

def main():

    parser = argparse.ArgumentParser(description=("DataPilot AI Remediation Agent ""(Dry Run)"))

    parser.add_argument("--rca-result",required=True,help="Path to RCA JSON result.")

    parser.add_argument("--output",required=True,help="Path for remediation plan.")

    args = parser.parse_args()

    # -----------------------------------------------------
    # Load RCA
    # -----------------------------------------------------

    rca = load_rca_result(args.rca_result)
    # -----------------------------------------------------
    # Validate
    # -----------------------------------------------------

    validate_rca(rca)

    # -----------------------------------------------------
    # Build plan
    # -----------------------------------------------------

    plan = build_remediation_plan(rca)

    # -----------------------------------------------------
    # Save
    # -----------------------------------------------------
    save_plan(path=args.output, plan=plan)

    # -----------------------------------------------------
    # Display
    # -----------------------------------------------------

    print_plan(plan)

if __name__ == "__main__":
    main()