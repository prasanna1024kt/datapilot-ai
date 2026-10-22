import copy
import sys
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from agents.remediation.plan_fingerprint import (
    attach_plan_fingerprint,
    calculate_plan_fingerprint,
    validate_plan_fingerprint,
)
from agents.remediation.executor import validate_approval


@pytest.fixture
def plan():
    return attach_plan_fingerprint({
        "dataset": "olist_order_payments",
        "action": "QUARANTINE_RECORDS",
        "requires_approval": True,
        "status": "PENDING_APPROVAL",
        "execution_mode": "DRY_RUN",
        "reason": "Test plan",
    })


@pytest.fixture
def approval(plan):
    return {
        "dataset": "olist_order_payments",
        "action": "QUARANTINE_RECORDS",
        "status": "APPROVED",
        "execution_authorized": True,
        "approved_by": "test-reviewer",
        "plan_fingerprint": plan["plan_fingerprint"],
    }


def test_valid_fingerprint_is_accepted(plan):
    assert validate_plan_fingerprint(plan) == plan["plan_fingerprint"]


def test_key_order_does_not_change_fingerprint():
    first = {"dataset": "sample", "action": "QUARANTINE_RECORDS"}
    second = {"action": "QUARANTINE_RECORDS", "dataset": "sample"}

    assert calculate_plan_fingerprint(first) == calculate_plan_fingerprint(second)


def test_modified_plan_invalidates_fingerprint(plan):
    changed = copy.deepcopy(plan)
    changed["reason"] = "Changed after review"

    with pytest.raises(ValueError, match="fingerprint mismatch"):
        validate_plan_fingerprint(changed)


def test_missing_fingerprint_is_rejected():
    with pytest.raises(ValueError, match="missing or invalid"):
        validate_plan_fingerprint({
            "dataset": "olist_order_payments",
            "action": "QUARANTINE_RECORDS",
        })


def test_matching_approval_is_accepted(plan, approval):
    validate_approval(
        approval,
        "olist_order_payments",
        "QUARANTINE_RECORDS",
        plan,
    )


def test_approval_without_fingerprint_is_rejected(plan, approval):
    approval.pop("plan_fingerprint")

    with pytest.raises(RuntimeError, match="Approval fingerprint"):
        validate_approval(
            approval,
            "olist_order_payments",
            "QUARANTINE_RECORDS",
            plan,
        )


def test_stale_approval_is_rejected_after_plan_changes(plan, approval):
    changed = copy.deepcopy(plan)
    changed["reason"] = "Changed after approval"

    with pytest.raises(RuntimeError, match="fingerprint"):
        validate_approval(
            approval,
            "olist_order_payments",
            "QUARANTINE_RECORDS",
            changed,
        )
