"""Create a human review request without approving or executing a plan."""

import argparse
import json
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path

from agents.remediation.plan_fingerprint import validate_plan_fingerprint

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_PLAN = (
    PROJECT_ROOT
    / "results"
    / "remediation"
    / "olist_order_payments_candidate.json"
)
REQUESTS_DIR = PROJECT_ROOT / "results" / "approvals" / "requests"


def main():
    parser = argparse.ArgumentParser(
        description="Validate a remediation plan and optionally create a pending review request."
    )
    parser.add_argument("--plan", type=Path, default=DEFAULT_PLAN)
    parser.add_argument(
        "--create-request",
        action="store_true",
        help="Create a separate PENDING_REVIEW artifact; never approve or execute.",
    )
    args = parser.parse_args()

    plan_path = args.plan.resolve()
    plan = json.loads(plan_path.read_text(encoding="utf-8"))
    fingerprint = validate_plan_fingerprint(plan)

    expected = {
        "dataset": "olist_order_payments",
        "action": "QUARANTINE_RECORDS",
        "status": "PENDING_APPROVAL",
        "requires_approval": True,
        "execution_mode": "DRY_RUN",
    }
    for field, value in expected.items():
        if plan.get(field) != value:
            raise SystemExit(
                f"Rejected: plan field {field!r} must equal {value!r}."
            )

    print("Candidate plan fingerprint validated:", fingerprint)
    print("Approval status: NOT APPROVED")
    print("Execution performed: NO")

    if not args.create_request:
        print("Review request not created. Use --create-request to create one.")
        return

    request = {
        "schema_version": 1,
        "dataset": plan["dataset"],
        "action": plan["action"],
        "status": "PENDING_REVIEW",
        "plan_fingerprint": fingerprint,
        "plan_path": str(plan_path.relative_to(PROJECT_ROOT)),
        "requested_at": datetime.now(timezone.utc).isoformat(),
        "reviewer": None,
        "review_decision": None,
        "execution_scope": "DRY_RUN_ONLY",
        "execution_authorized": False,
        "review_checklist": [
            "Verify the two invalid records and their business keys.",
            "Confirm quarantine is appropriate for downstream consumption.",
            "Confirm source/data-owner follow-up is required.",
            "Approve or reject this exact plan fingerprint.",
        ],
        "notes": (
            "This is a review request, not an approval. "
            "It cannot authorize execution."
        ),
    }

    REQUESTS_DIR.mkdir(parents=True, exist_ok=True)
    destination = REQUESTS_DIR / (
        f"{plan['dataset']}_{fingerprint}.json"
    )

    # Publish without overwriting an existing request.
    fd, temp_name = tempfile.mkstemp(
        prefix=".approval-request-",
        suffix=".tmp",
        dir=REQUESTS_DIR,
    )
    temp_path = Path(temp_name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as file:
            json.dump(request, file, indent=2)
            file.write("\n")
            file.flush()
            os.fsync(file.fileno())

        os.link(temp_path, destination)
    finally:
        temp_path.unlink(missing_ok=True)

    print("Pending review request created:", destination.relative_to(PROJECT_ROOT))
    print("Request status: PENDING_REVIEW")
    print("Reviewer decision: NOT SET")
    print("Execution authorized: False")
    print("No approval was created. No Spark job or data write was performed.")


if __name__ == "__main__":
    main()
