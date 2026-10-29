import importlib.util
import json
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from agents.remediation.plan_fingerprint import attach_plan_fingerprint


JOB_PATH = PROJECT_ROOT / "spark" / "jobs" / "quarantine_records_candidate.py"

spec = importlib.util.spec_from_file_location(
    "quarantine_records_candidate_under_test",
    JOB_PATH,
)
job = importlib.util.module_from_spec(spec)
spec.loader.exec_module(job)


def make_plan():
    return attach_plan_fingerprint({
        "dataset": "olist_order_payments",
        "action": "QUARANTINE_RECORDS",
        "requires_approval": True,
        "status": "PENDING_APPROVAL",
        "execution_mode": "DRY_RUN",
        "reason": "Isolated fingerprint test",
    })


def write_json(path, value):
    path.write_text(json.dumps(value), encoding="utf-8")
    return str(path)


@pytest.mark.parametrize("tamper", ["plan", "approval"])
def test_apply_rejects_invalid_fingerprint_before_spark(
    tmp_path, monkeypatch, tamper
):
    plan = make_plan()
    approval = {
        "dataset": "olist_order_payments",
        "action": "QUARANTINE_RECORDS",
        "status": "APPROVED",
        "execution_authorized": True,
        "plan_fingerprint": plan["plan_fingerprint"],
        "execution_scope": "APPLY",
    }

    if tamper == "plan":
        plan["reason"] = "Changed after fingerprinting"
    else:
        approval["plan_fingerprint"] = "0" * 64

    approval_path = write_json(tmp_path / "approval.json", approval)
    plan_path = write_json(tmp_path / "plan.json", plan)

    monkeypatch.setattr(
        sys,
        "argv",
        [
            "quarantine_records_candidate.py",
            "--dataset", "olist_order_payments",
            "--execution-id", "fingerprint-guard-test",
            "--mode", "APPLY",
            "--confirm-write",
            "--approval-file", approval_path,
            "--plan-file", plan_path,
        ],
    )

    with patch.object(job, "create_spark") as create_spark:
        with pytest.raises(RuntimeError, match="fingerprint"):
            job.main()

        create_spark.assert_not_called()

def test_apply_rejects_dry_run_only_approval_before_spark(tmp_path, monkeypatch):
    plan = make_plan()
    approval = {
        "dataset": "olist_order_payments",
        "action": "QUARANTINE_RECORDS",
        "status": "APPROVED",
        "execution_authorized": True,
        "execution_scope": "DRY_RUN_ONLY",
        "plan_fingerprint": plan["plan_fingerprint"],
    }

    approval_path = write_json(tmp_path / "approval.json", approval)
    plan_path = write_json(tmp_path / "plan.json", plan)

    monkeypatch.setattr(
        sys,
        "argv",
        [
            "quarantine_records_candidate.py",
            "--dataset", "olist_order_payments",
            "--execution-id", "scope-guard-test",
            "--mode", "APPLY",
            "--confirm-write",
            "--approval-file", approval_path,
            "--plan-file", plan_path,
        ],
    )

    with patch.object(job, "create_spark") as create_spark:
        with pytest.raises(RuntimeError, match="execution_scope|APPLY-scoped"):
            job.main()

        create_spark.assert_not_called()