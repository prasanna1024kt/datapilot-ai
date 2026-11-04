def rca_baseline_target(inputs):
    """Deterministic RCA baseline for evaluating the LangSmith pipeline."""

    dq = inputs.get("dq_result") or {}
    evidence = inputs.get("rca_evidence") or {}

    bronze = evidence.get("bronze") or {}
    silver = evidence.get("silver") or {}
    audit = evidence.get("remediation_audit") or {}

    bronze_count = bronze.get("invalid_count")
    silver_count = silver.get("invalid_count")

    if (
        isinstance(bronze_count, int)
        and isinstance(silver_count, int)
        and bronze_count > 0
        and silver_count > 0
    ):
        classification = "UPSTREAM_DATA_QUALITY"
        action = "QUARANTINE_RECORDS"
        confidence = "HIGH"

    elif (
        bronze_count == 0
        and isinstance(silver_count, int)
        and silver_count > 0
    ):
        classification = "TRANSFORMATION_DATA_QUALITY"
        action = "TRANSFORMATION_FIX"
        confidence = "HIGH"

    else:
        classification = "RCA_INCONCLUSIVE"
        action = "MANUAL_REVIEW"
        confidence = "LOW"

    count_words = {
        0: "zero",
        1: "one",
        2: "two",
        3: "three",
        4: "four",
        5: "five",
    }

    def count_label(value):
        return count_words.get(value, str(value))

    evidence_facts = [
        "DQ reports a failure."
        if dq.get("status") == "FAIL"
        else f"DQ reports a {dq.get('status')} status."
    ]

    if (
        bronze_count == 0
        and silver_count == 0
        and dq.get("status") == "FAIL"
    ):
        evidence_facts.append(
            "Bronze and Silver both report zero invalid records."
        )
    else:
        if bronze:
            prefix = "Current Bronze evidence" if audit.get("count", 0) > 0 else "Bronze"
            evidence_facts.append(
                f"{prefix} contains {count_label(bronze_count)} invalid records."
            )
        else:
            evidence_facts.append("Bronze evidence is unavailable.")

        if silver:
            prefix = "Current Silver evidence" if audit.get("count", 0) > 0 else "Silver"
            evidence_facts.append(
                f"{prefix} contains {count_label(silver_count)} invalid records."
            )
        else:
            evidence_facts.append("Silver evidence is unavailable.")

    if audit.get("count", 0) > 0:
        evidence_facts.extend([
            "Historical remediation was committed and verified.",
            "Historical remediation does not prove the current incident is resolved.",
        ])

    return {
        "classification": classification,
        "evidence": evidence_facts,
        "recommendation": (
            "Review the proposed remediation before execution."
        ),
        "remediation": {
            "action": action,
            "requires_approval": True,
            "reason": "All proposed remediation requires human approval.",
        },
        "confidence": confidence,
    }
