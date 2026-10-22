"""Canonical fingerprinting for DataPilot AI remediation plans."""

import hashlib
import json


FINGERPRINT_FIELD = "plan_fingerprint"


def calculate_plan_fingerprint(plan: dict) -> str:
    """Return SHA-256 of a canonical plan, excluding its stored fingerprint."""
    if not isinstance(plan, dict):
        raise ValueError("Remediation plan must be a JSON object.")

    canonical_plan = {
        key: value
        for key, value in plan.items()
        if key != FINGERPRINT_FIELD
    }

    canonical_json = json.dumps(
        canonical_plan,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    )

    return hashlib.sha256(
        canonical_json.encode("utf-8")
    ).hexdigest()


def attach_plan_fingerprint(plan: dict) -> dict:
    """Return a copy of the plan with its calculated fingerprint."""
    result = dict(plan)
    result.pop(FINGERPRINT_FIELD, None)
    result[FINGERPRINT_FIELD] = calculate_plan_fingerprint(result)
    return result


def validate_plan_fingerprint(plan: dict) -> str:
    """Reject missing or stale fingerprints and return the verified value."""
    stored = plan.get(FINGERPRINT_FIELD)

    if not isinstance(stored, str) or len(stored) != 64:
        raise ValueError("Remediation plan fingerprint is missing or invalid.")

    expected = calculate_plan_fingerprint(plan)

    if not __import__("hmac").compare_digest(stored, expected):
        raise ValueError(
            "Remediation plan fingerprint mismatch; approval is stale."
        )

    return expected