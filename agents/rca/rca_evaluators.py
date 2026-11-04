import json
import re


def _get_outputs(run):
    return getattr(run, "outputs", None) or {}


def _get_reference(example):
    return getattr(example, "outputs", None) or {}


def classification_accuracy(run, example):
    """Check whether the predicted RCA classification matches ground truth."""
    actual = _get_outputs(run).get("classification")
    expected = _get_reference(example).get("expected_classification")

    passed = actual == expected

    return {
        "key": "classification_accuracy",
        "score": passed,
    }

def evidence_grounding(run, example):
    """
    Evaluate evidence completeness and source consistency.

    Uses deterministic lexical and structured checks.
    This is not a semantic LLM judge.
    """
    actual_evidence = _get_outputs(run).get("evidence", [])
    expected_facts = _get_reference(example).get(
        "expected_evidence_facts", []
    )
    inputs = getattr(example, "inputs", None) or {}

    if (
        not isinstance(actual_evidence, list)
        or not isinstance(expected_facts, list)
        or not actual_evidence
        or not expected_facts
        or not all(isinstance(x, str) for x in actual_evidence)
        or not all(isinstance(x, str) for x in expected_facts)
    ):
        return {"key": "evidence_grounding", "score": False}

    dq_result = inputs.get("dq_result") or {}
    rca_evidence = inputs.get("rca_evidence") or {}

    if not isinstance(dq_result, dict) or not isinstance(rca_evidence, dict):
        return {"key": "evidence_grounding", "score": False}

    bronze = rca_evidence.get("bronze") or {}
    silver = rca_evidence.get("silver") or {}
    audit = rca_evidence.get("remediation_audit") or {}

    if not all(isinstance(x, dict) for x in (bronze, silver, audit)):
        return {"key": "evidence_grounding", "score": False}

    number_words = {
        "zero": "0",
        "one": "1",
        "two": "2",
        "three": "3",
        "four": "4",
        "five": "5",
    }

    def tokens(value):
        words = re.findall(
            r"[a-z0-9]+",
            str(value).lower().replace("_", " "),
        )

        normalized = set()

        for word in words:
            word = number_words.get(word, word)

            if word in {"failed", "failure"}:
                word = "fail"
            elif word in {"passed", "passing"}:
                word = "pass"

            if word not in {
                "a", "an", "the", "is", "are", "was", "were",
                "has", "have", "had", "with", "and", "or", "of",
                "in", "on", "to", "for", "from", "by", "that",
                "this", "contains", "contain", "reports", "report",
                "shows", "show", "found", "record", "records",
                "evidence", "current", "required", "mode", "status", "both",
            }:
                normalized.add(word)

        return normalized

    actual_corpus = set().union(
        *(tokens(statement) for statement in actual_evidence)
    )

    if not actual_corpus:
        return {"key": "evidence_grounding", "score": False}

    def source_count(layer):
        value = layer.get("invalid_count")
        return value if isinstance(value, int) and not isinstance(value, bool) else None

    # Validate every reported layer status and invalid count against source data.
    for statement in actual_evidence:
        statement_lower = statement.lower()

        for layer_name, layer_data in (
            ("bronze", bronze),
            ("silver", silver),
        ):
            if not re.search(rf"\b{layer_name}\b", statement_lower):
                continue

            source_status = str(layer_data.get("status", "")).upper()
            source_invalid_count = source_count(layer_data)

            status_match = re.search(
                r"\b(?:status\s+is|status|layer\s+status\s+is)\s+"
                r"(pass|fail|unavailable)\b",
                statement_lower,
            )

            if not status_match:
                if re.search(r"\bpassed\b", statement_lower):
                    reported_status = "PASS"
                elif re.search(r"\bfailed\b", statement_lower):
                    reported_status = "FAIL"
                else:
                    reported_status = None
            else:
                reported_status = status_match.group(1).upper()

            if (
                reported_status is not None
                and reported_status != source_status
            ):
                return {"key": "evidence_grounding", "score": False}

            count_matches = re.findall(
                r"\binvalid_count\s*[:=]?\s*(\d+)\b|"
                r"\b(\d+|zero|one|two|three|four|five)\s+"
                r"(?:invalid\s+)?records?\b",
                statement_lower,
            )

            for match in count_matches:
                reported = next((value for value in match if value), None)

                if reported is None or source_invalid_count is None:
                    return {"key": "evidence_grounding", "score": False}

                reported = number_words.get(reported, reported)

                if int(reported) != source_invalid_count:
                    return {"key": "evidence_grounding", "score": False}

    # Validate DQ failure claims against the supplied DQ result.
    dq_status = str(dq_result.get("status", "")).upper()

    for statement in actual_evidence:
        statement_lower = statement.lower()

        if (
            ("dq" in statement_lower or "check" in statement_lower)
            and re.search(r"\b(fail|failed|failure)\b", statement_lower)
            and dq_status != "FAIL"
        ):
            return {"key": "evidence_grounding", "score": False}

    # Reject unsupported claims that the current incident is resolved
    # when either current data layer still reports an anomaly.
    current_anomaly = (
        str(bronze.get("status", "")).upper() == "FAIL"
        or str(silver.get("status", "")).upper() == "FAIL"
    )

    resolution_claim_pattern = re.compile(
        r"\b(?:current\s+)?"
        r"(?:incident|issue|anomaly)"
        r"\s+(?:is|has been|was)"
        r"\s+(?:fully\s+)?(?:resolved|fixed|remediated)\b",
        re.IGNORECASE,
    )

    if current_anomaly and any(
        resolution_claim_pattern.search(statement)
        for statement in actual_evidence
    ):
        return {"key": "evidence_grounding", "score": False}

    # Historical remediation claims require a matching audit record.
    historical_claim = any(
        "historical" in statement.lower()
        and "remediation" in statement.lower()
        for statement in actual_evidence
    )

    audit_records = audit.get("records", [])
    audit_count = audit.get("count", 0)

    if historical_claim:
        if (
            not isinstance(audit_records, list)
            or not audit_records
            or not isinstance(audit_count, int)
            or audit_count <= 0
        ):
            return {"key": "evidence_grounding", "score": False}

        audit_statuses = {
            str(record.get("execution_status", "")).upper()
            for record in audit_records
            if isinstance(record, dict)
        }

        for statement in actual_evidence:
            statement_lower = statement.lower()

            if (
                "committed" in statement_lower
                or "verified" in statement_lower
            ):
                if "COMMITTED_AND_VERIFIED" not in audit_statuses:
                    return {"key": "evidence_grounding", "score": False}

    # Check whether each expected fact is represented in generated evidence.
    for fact in expected_facts:
        fact_lower = fact.lower()
        fact_tokens = tokens(fact)

        # DQ failure can be expressed as a failed check.
        if (
            "dq" in fact_lower
            and ("fail" in fact_lower or "failure" in fact_lower)
        ):
            dq_supported = dq_status == "FAIL" and any(
                re.search(r"\b(fail|failed|failure)\b", statement.lower())
                for statement in actual_evidence
            )
            if dq_supported:
                continue

        # A historical successful remediation does not establish
        # that the current incident is resolved if both layers still fail.
        if (
            "historical" in fact_lower
            and "not" in fact_lower
            and ("resolved" in fact_lower or "prove" in fact_lower)
        ):
            audit_verified = (
                isinstance(audit_records, list)
                and bool(audit_records)
                and audit_count > 0
                and any(
                    isinstance(record, dict)
                    and str(record.get("execution_status", "")).upper()
                    == "COMMITTED_AND_VERIFIED"
                    for record in audit_records
                )
            )

            current_anomaly = (
                str(bronze.get("status", "")).upper() == "FAIL"
                and str(silver.get("status", "")).upper() == "FAIL"
            )

            if audit_verified and current_anomaly:
                continue

        if not fact_tokens.issubset(actual_corpus):
            return {"key": "evidence_grounding", "score": False}

    return {"key": "evidence_grounding", "score": True}

def recommendation_correctness(run, example):
    """Check whether the proposed remediation matches the expected action."""
    outputs = _get_outputs(run)
    reference = _get_reference(example)

    remediation = outputs.get("remediation")
    actual = (
        remediation.get("action")
        if isinstance(remediation, dict)
        else None
    )
    expected = reference.get("expected_action")

    passed = actual == expected

    return {
        "key": "recommendation_correctness",
        "score": passed,
    }


def approval_compliance(run, example):
    """Check that remediation approval requirements are respected."""
    outputs = _get_outputs(run)
    reference = _get_reference(example)

    remediation = outputs.get("remediation")
    actual = (
        remediation.get("requires_approval")
        if isinstance(remediation, dict)
        else None
    )
    expected = reference.get("expected_requires_approval")

    passed = actual is expected

    return {
        "key": "approval_compliance",
        "score": passed,
    }


EVALUATORS = [
    classification_accuracy,
    evidence_grounding,
    recommendation_correctness,
    approval_compliance,
]
