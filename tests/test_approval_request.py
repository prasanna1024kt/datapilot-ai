import json
import sys
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from agents.remediation import approval_request
from agents.remediation.plan_fingerprint import attach_plan_fingerprint


def make_plan():
    return attach_plan_fingerprint({
        "dataset": "olist_order_payments",
        "action": "QUARANTINE_RECORDS",
        "status": "PENDING_APPROVAL",
        "requires_approval": True,
        "execution_mode": "DRY_RUN",
        "reason": "Test plan for approval workflow.",
    })


@pytest.fixture
def workspace(tmp_path, monkeypatch):
    monkeypatch.setattr(approval_request, "PROJECT_ROOT", tmp_path)

    plan_path = (
        tmp_path / "results" / "remediation"
        / "olist_order_payments_candidate.json"
    )
    plan_path.parent.mkdir(parents=True)
    plan_path.write_text(
        json.dumps(make_plan(), indent=2),
        encoding="utf-8",
    )

    requests_dir = tmp_path / "results" / "approvals" / "requests"
    monkeypatch.setattr(approval_request, "REQUESTS_DIR", requests_dir)

    return tmp_path, plan_path, requests_dir


def run_command(monkeypatch, plan_path, *extra_args):
    monkeypatch.setattr(
        sys,
        "argv",
        ["approval_request", "--plan", str(plan_path), *extra_args],
    )
    approval_request.main()


def test_review_only_does_not_create_request(workspace, monkeypatch):
    _, plan_path, requests_dir = workspace

    run_command(monkeypatch, plan_path)

    assert not requests_dir.exists()


def test_create_request_remains_pending_and_unauthorized(
    workspace, monkeypatch
):
    _, plan_path, requests_dir = workspace

    run_command(monkeypatch, plan_path, "--create-request")

    files = list(requests_dir.glob("*.json"))
    assert len(files) == 1

    request = json.loads(files[0].read_text(encoding="utf-8"))

    assert request["status"] == "PENDING_REVIEW"
    assert request["reviewer"] is None
    assert request["review_decision"] is None
    assert request["execution_authorized"] is False
    assert request["execution_scope"] == "DRY_RUN_ONLY"

    plan = json.loads(plan_path.read_text(encoding="utf-8"))
    assert request["plan_fingerprint"] == plan["plan_fingerprint"]


def test_tampered_plan_is_rejected(workspace, monkeypatch):
    _, plan_path, requests_dir = workspace

    plan = json.loads(plan_path.read_text(encoding="utf-8"))
    plan["reason"] = "Modified after fingerprinting"
    plan_path.write_text(json.dumps(plan), encoding="utf-8")

    with pytest.raises(ValueError, match="fingerprint"):
        run_command(monkeypatch, plan_path, "--create-request")

    assert not requests_dir.exists()


def test_existing_request_is_not_overwritten(workspace, monkeypatch):
    _, plan_path, requests_dir = workspace

    run_command(monkeypatch, plan_path, "--create-request")
    existing = next(requests_dir.glob("*.json"))
    original = existing.read_bytes()

    with pytest.raises(FileExistsError):
        run_command(monkeypatch, plan_path, "--create-request")

    assert existing.read_bytes() == original
