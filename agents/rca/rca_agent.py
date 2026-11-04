import argparse
import json
import math
import os
import re
from dotenv import load_dotenv
from langsmith import traceable, tracing_context
from openai import OpenAI
from jsonschema import validate as validate_json_schema
from jsonschema.exceptions import ValidationError
from agents.rca.mcp_client import call_tool

# =========================================================
# Environment
# =========================================================

load_dotenv()

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

if not OPENAI_API_KEY:
    raise RuntimeError(
        "OPENAI_API_KEY is not configured. "
        "Add it to the .env file."
    )

client = OpenAI(api_key=OPENAI_API_KEY)

MODEL = "gpt-5.6"


# =========================================================
# System Prompt
# =========================================================

SYSTEM_PROMPT = """
You are the RCA Agent for DataPilot AI.

You analyze data-quality incidents using evidence obtained
through DataPilot AI MCP tools.

Your responsibilities are:

1. Identify the failed data-quality check.
2. Identify the affected dataset and column.
3. Identify invalid values and their counts.
4. Compare DQ evidence with Bronze and Silver evidence.
5. Determine the most likely origin of the problem.
6. Clearly distinguish observed evidence from inference.
7. Recommend appropriate next actions.
8. Never invent evidence that is not present in the input.

Classification rules:

Follow this decision matrix strictly:

1. DQ status is FAIL, Bronze status is FAIL,
   and Silver status is FAIL:
   - Classification: UPSTREAM_DATA_QUALITY.
   - The anomaly exists in Bronze and is propagated to Silver.

2. DQ status is FAIL, Bronze status is PASS,
   and Silver status is FAIL:
   - Classification: TRANSFORMATION_DATA_QUALITY.
   - The anomaly is observed in Silver but not Bronze.

3. DQ status is FAIL, Bronze status is PASS,
   and Silver status is PASS:
   - Classification: RCA_INCONCLUSIVE.
   - DQ reports a failure, but the layer evidence does not
     reproduce the anomaly.
   - Do not classify this as UPSTREAM_DATA_QUALITY.
   - Do not invent an explanation for the discrepancy.

4. Critical Bronze or Silver evidence is UNAVAILABLE:
   - Classification: RCA_INCONCLUSIVE.
   - Use LOW confidence.
   - Recommend MANUAL_REVIEW.

5. Any other combination that does not establish
   a supported classification:
   - Classification: RCA_INCONCLUSIVE.
   - Do not infer a root cause.

Unavailable Evidence Rules:

- UNAVAILABLE means the layer inspection could not provide evidence.
- Never interpret UNAVAILABLE as PASS or zero invalid records.
- Never infer that a layer contains no anomalies when its evidence is unavailable.
- If critical layer evidence is unavailable, classify as RCA_INCONCLUSIVE.
- Use LOW confidence when evidence is insufficient.
- Recommend MANUAL_REVIEW for inconclusive investigations.
- Clearly state which layer's evidence is unavailable.
- Do not invent a reason beyond the supplied MCP response.

Remediation Approval Rules:

- Every remediation action MUST have
  requires_approval=true.
- This applies to QUARANTINE_RECORDS,
  TRANSFORMATION_FIX, MANUAL_REVIEW,
  UPDATE_DQ_RULE, and NO_ACTION.
- Never return requires_approval=false.
- Approval must be explicitly set to true
  in the structured response.

Classification and remediation mapping:

- UPSTREAM_DATA_QUALITY:
  QUARANTINE_RECORDS only.

- TRANSFORMATION_DATA_QUALITY:
  TRANSFORMATION_FIX only.

- RCA_INCONCLUSIVE:
  MANUAL_REVIEW or NO_ACTION only.

Never recommend an action that conflicts
with the selected classification.

Confidence:

- HIGH: Evidence directly supports the classification.
- MEDIUM: Evidence supports the classification but has
  some uncertainty.
- LOW: Evidence is insufficient or ambiguous.

Evidence rules:

- The evidence array must contain only facts supported
  by the supplied MCP evidence.
- Do not invent source records.
- Do not invent business rules.
- Do not invent timestamps.
- Do not invent system behavior.
- Do not claim causality beyond what the evidence supports.

When Bronze and Silver contain the same invalid values
and counts, explain that the evidence indicates the
anomaly existed upstream of the Silver transformation
and was propagated into Silver.

Remediation Audit Interpretation:

- Use remediation_audit as historical execution evidence.
- Never assume a historical successful remediation
  means the current data-quality incident is resolved.
- Distinguish DRY_RUN from APPLY execution modes.
- COMMITTED_AND_VERIFIED indicates that the
  recorded remediation execution was committed and verified.
- Evaluate Gold refresh status separately from
  remediation execution status.
- A successful remediation does not prove that
  downstream Gold data is correct unless the supplied
  evidence supports that conclusion.
- source_modified=false and s3_write_performed=true
  can coexist when data is written to a separate
  quarantine location.
- If no audit records exist, state that remediation
  history is unavailable.
- Never recommend repeating a remediation merely
  because an earlier audit record exists.
- Do not treat audit history as proof of the root
  cause of the current incident.
- Clearly distinguish current DQ evidence,
  historical remediation evidence, and inference.
- Historical remediation execution must never change
  the classification of the current incident.

- If current Bronze and Silver evidence both show
  anomalies, classify using the current evidence even
  when historical remediation was committed and verified.
"""
# =========================================================
# Structured Output Schema
# =========================================================

RCA_SCHEMA = {
    "type": "object",
    "properties": {
        "dataset": {
            "type": "string"
        },
        "dq_status": {
            "type": "string",
            "enum": [
                "PASS",
                "FAIL"
            ]
        },
        "classification": {
            "type": "string",
            "enum": [
                "UPSTREAM_DATA_QUALITY",
                "TRANSFORMATION_DATA_QUALITY",
                "RCA_INCONCLUSIVE"
            ]
        },
        "root_cause": {
            "type": "string"
        },
        "evidence": {
            "type": "array",
            "items": {
                "type": "string"
            }
        },
        "recommendation": {
            "type": "string"
        },
        "remediation": {
            "type": "object",
            "properties": {
                "action": {
                    "type": "string",
                    "enum": [
                        "QUARANTINE_RECORDS",
                        "TRANSFORMATION_FIX",
                        "UPDATE_DQ_RULE",
                        "NO_ACTION",
                        "MANUAL_REVIEW"
                    ]
                },
                "requires_approval": {
                    "type": "boolean"
                },
                "reason": {
                    "type": "string"
                }
            },
            "required": [
                "action",
                "requires_approval",
                "reason"
            ],
            "additionalProperties": False
        },
        "next_actions": {
            "type": "array",
            "items": {
                "type": "string"
            }
        },
        "confidence": {
            "type": "string",
            "enum": [
                "HIGH",
                "MEDIUM",
                "LOW"
            ]
        }
    },
    "required": [
    "dataset",
    "dq_status",
    "classification",
    "root_cause",
    "evidence",
    "recommendation",
    "remediation",
    "next_actions",
    "confidence"
 ],
    "additionalProperties": False
}


def determine_expected_classification(dq_result, rca_evidence):
    """
    Determine RCA classification from validated DQ,
    Bronze and Silver evidence.

    Never infer a root cause from unavailable or
    contradictory evidence.
    """

    if not isinstance(dq_result, dict):
        raise ValueError("DQ result must be a dictionary.")

    if not isinstance(rca_evidence, dict):
        raise ValueError("RCA evidence must be a dictionary.")

    dq_status = dq_result.get("status")
    bronze = rca_evidence.get("bronze")
    silver = rca_evidence.get("silver")

    if not isinstance(bronze, dict) or not isinstance(silver, dict):
        raise ValueError("Bronze and Silver evidence are required.")

    bronze_status = bronze.get("status")
    silver_status = silver.get("status")

    if dq_status != "FAIL":
        return "RCA_INCONCLUSIVE"

    if "UNAVAILABLE" in {bronze_status, silver_status}:
        return "RCA_INCONCLUSIVE"

    if bronze_status == "FAIL" and silver_status == "FAIL":
        return "UPSTREAM_DATA_QUALITY"

    if bronze_status == "PASS" and silver_status == "FAIL":
        return "TRANSFORMATION_DATA_QUALITY"

    return "RCA_INCONCLUSIVE"

# =========================================================
# RCA Output Guardrails
# =========================================================
def validate_rca_result(result, dq_result, rca_evidence=None):
    """
    Validate the LLM-generated RCA result before it is accepted.
    Reject malformed, inconsistent, or unsafe recommendations.
    """

    if not isinstance(result, dict):
        raise ValueError("RCA output must be a JSON object.")

    if not isinstance(dq_result, dict):
        raise ValueError("Input DQ result must be a JSON object.")

    try:
        validate_json_schema(
            instance=result,
            schema=RCA_SCHEMA
        )
    except ValidationError as exc:
        raise ValueError(
            f"RCA output failed schema validation: {exc.message}"
        ) from exc

    # -----------------------------------------------------
    # Dataset and DQ consistency
    # -----------------------------------------------------

    if result["dataset"] != dq_result.get("dataset"):
        raise ValueError(
            "RCA output dataset does not match the input DQ dataset."
        )

    if result["dq_status"] != dq_result.get("status"):
        raise ValueError(
            "RCA output DQ status does not match the input DQ status."
        )

    # -----------------------------------------------------
    # Deterministic classification validation
    # -----------------------------------------------------

    silver_evidence = None

    if rca_evidence is not None:
        expected_classification = determine_expected_classification(
            dq_result=dq_result,
            rca_evidence=rca_evidence
        )

        if result["classification"] != expected_classification:
            raise ValueError(
                "RCA classification does not match validated "
                f"Bronze/Silver evidence. Expected: "
                f"{expected_classification}; "
                f"Received: {result['classification']}."
            )

        silver_evidence = rca_evidence.get("silver")

    # -----------------------------------------------------
    # Unavailable evidence guardrails
    # -----------------------------------------------------

    if (
        isinstance(silver_evidence, dict)
        and silver_evidence.get("status") == "UNAVAILABLE"
    ):
        if result["classification"] != "RCA_INCONCLUSIVE":
            raise ValueError(
                "Unavailable Silver evidence requires RCA_INCONCLUSIVE."
            )

        if result["confidence"] != "LOW":
            raise ValueError(
                "Unavailable Silver evidence requires LOW confidence."
            )

        if result["remediation"]["action"] != "MANUAL_REVIEW":
            raise ValueError(
                "Unavailable Silver evidence requires MANUAL_REVIEW."
            )

        if result["remediation"]["requires_approval"] is not True:
            raise ValueError(
                "Unavailable Silver evidence requires human approval."
            )

    # -----------------------------------------------------
    # Confidence validation
    # -----------------------------------------------------

    if (
        result["classification"] == "RCA_INCONCLUSIVE"
        and result["confidence"] == "HIGH"
    ):
        raise ValueError(
            "RCA_INCONCLUSIVE cannot have HIGH confidence."
        )

    # -----------------------------------------------------
    # Remediation validation
    # -----------------------------------------------------
        # -----------------------------------------------------
    # Remediation validation
    # -----------------------------------------------------

    remediation = result["remediation"]
    action = remediation["action"]
    reason = remediation["reason"].strip()
    requires_approval = remediation["requires_approval"]

    # Deterministic classification-to-action validation
    expected_actions = {
        "UPSTREAM_DATA_QUALITY": {"QUARANTINE_RECORDS"},
        "TRANSFORMATION_DATA_QUALITY": {"TRANSFORMATION_FIX"},
        "RCA_INCONCLUSIVE": {"MANUAL_REVIEW", "NO_ACTION"},
    }

    allowed_actions = expected_actions[result["classification"]]

    if action not in allowed_actions:
        raise ValueError(
            "Remediation action does not match RCA classification. "
            f"Classification: {result['classification']}; "
            f"Allowed actions: {sorted(allowed_actions)}; "
            f"Received: {action}."
        )

    # Inconclusive RCA cannot recommend automated remediation
    if (
        result["classification"] == "RCA_INCONCLUSIVE"
        and action in {
            "QUARANTINE_RECORDS",
            "TRANSFORMATION_FIX",
            "UPDATE_DQ_RULE"
        }
    ):
        raise ValueError(
            "RCA_INCONCLUSIVE cannot recommend automated remediation."
        )

    if action == "NO_ACTION" and not reason:
        raise ValueError(
            "NO_ACTION requires a meaningful justification."
        )

    if action != "NO_ACTION" and requires_approval is not True:
        raise ValueError(
            "All remediation actions must require approval."
        )

    return result
# =========================================================
# MCP Evidence Validation
# =========================================================

def validate_mcp_evidence(
    dq_result,
    bronze_result,
    silver_result,
    expected_column=None,
    expected_minimum=None,
    expected_maximum=None
):
    """
    Validate DQ, Bronze and Silver MCP evidence
    before sending it to the LLM.
    """

    if not isinstance(dq_result, dict):
        raise ValueError("DQ result returned by MCP is not a JSON object.")

    if not isinstance(bronze_result, dict):
        raise ValueError("Bronze result returned by MCP is not a JSON object.")

    if not isinstance(silver_result, dict):
        raise ValueError("Silver result returned by MCP is not a JSON object.")

    dataset = dq_result.get("dataset")

    if not isinstance(dataset, str) or not dataset.strip():
        raise ValueError("MCP DQ result has an invalid dataset.")

    for layer_name, result, expected_layer in (
        ("Bronze", bronze_result, "bronze"),
        ("Silver", silver_result, "silver"),
    ):
        if result.get("dataset") != dataset:
            raise ValueError(
                f"{layer_name} evidence dataset does not match DQ dataset."
            )

        if result.get("layer") != expected_layer:
            raise ValueError(
                f"{layer_name} evidence has an invalid layer identifier."
            )

        if not isinstance(result.get("column"), str) or not result["column"].strip():
            raise ValueError(
                f"{layer_name} evidence has an invalid column."
            )

        if not isinstance(result.get("path"), str) or not result["path"].strip():
            raise ValueError(
                f"{layer_name} evidence has an invalid storage path."
            )

        if expected_column is not None and result.get("column") != expected_column:
            raise ValueError(
                f"{layer_name} evidence column does not match investigation scope."
            )

        if expected_minimum is not None and result.get("minimum") != expected_minimum:
            raise ValueError(
                f"{layer_name} evidence minimum does not match investigation scope."
            )

        if expected_maximum is not None and result.get("maximum") != expected_maximum:
            raise ValueError(
                f"{layer_name} evidence maximum does not match investigation scope."
            )

        allowed_statuses = (
            {"PASS", "FAIL", "UNAVAILABLE"}
            if expected_layer == "silver"
            else {"PASS", "FAIL"}
        )

        if result.get("status") not in allowed_statuses:
            raise ValueError(
                f"{layer_name} evidence has an unsupported status."
            )

        minimum = result.get("minimum")
        maximum = result.get("maximum")

        if (
            isinstance(minimum, bool)
            or not isinstance(minimum, (int, float))
            or isinstance(maximum, bool)
            or not isinstance(maximum, (int, float))
        ):
            raise ValueError(
                f"{layer_name} evidence has invalid numeric boundaries."
            )

        if minimum > maximum:
            raise ValueError(
                f"{layer_name} evidence minimum exceeds maximum."
            )

        if result["status"] == "UNAVAILABLE":
            reason = result.get("reason")
            if not isinstance(reason, str) or not reason.strip():
                raise ValueError(
                    "Unavailable Silver evidence requires a reason."
                )
            if "invalid_count" in result:
                raise ValueError(
                    "Unavailable Silver evidence must not claim an invalid_count."
                )
            continue

        invalid_count = result.get("invalid_count")

        if (
            isinstance(invalid_count, bool)
            or not isinstance(invalid_count, int)
            or invalid_count < 0
        ):
            raise ValueError(
                f"{layer_name} evidence has an invalid invalid_count."
            )

        sample_values = result.get("sample_invalid_values")

        if not isinstance(sample_values, list):
            raise ValueError(
                f"{layer_name} evidence sample_invalid_values must be a list."
            )

        if len(sample_values) > invalid_count:
            raise ValueError(
                f"{layer_name} sample count exceeds invalid_count."
            )

        expected_status = "FAIL" if invalid_count > 0 else "PASS"

        if result["status"] != expected_status:
            raise ValueError(
                f"{layer_name} status conflicts with invalid_count."
            )

    return True

def validate_additional_mcp_evidence(
    dataset,
    column,
    minimum,
    maximum,
    invalid_records,
    remediation_audit
):
    """Validate invalid-record and remediation-audit MCP responses."""

    if not isinstance(invalid_records, dict):
        raise ValueError("Invalid-record MCP response must be a JSON object.")

    if not isinstance(remediation_audit, dict):
        raise ValueError("Remediation audit MCP response must be a JSON object.")

    if invalid_records.get("dataset") != dataset:
        raise ValueError("Invalid-record dataset mismatch.")

    if invalid_records.get("layer") != "bronze":
        raise ValueError("Invalid-record layer must be bronze.")

    if invalid_records.get("column") != column:
        raise ValueError("Invalid-record column mismatch.")

    if invalid_records.get("minimum") != minimum:
        raise ValueError("Invalid-record minimum boundary mismatch.")

    if invalid_records.get("maximum") != maximum:
        raise ValueError("Invalid-record maximum boundary mismatch.")

    if invalid_records.get("status") not in {"PASS", "FAIL"}:
        raise ValueError("Invalid-record response has an unsupported status.")

    invalid_count = invalid_records.get("invalid_count")
    records_returned = invalid_records.get("records_returned")
    records = invalid_records.get("records")

    if (
        isinstance(invalid_count, bool)
        or not isinstance(invalid_count, int)
        or invalid_count < 0
    ):
        raise ValueError("Invalid-record response has an invalid count.")

    if (
        isinstance(records_returned, bool)
        or not isinstance(records_returned, int)
        or records_returned < 0
    ):
        raise ValueError("Invalid-record response has an invalid records_returned.")

    if not isinstance(records, list):
        raise ValueError("Invalid-record response records must be a list.")

    if records_returned != len(records):
        raise ValueError("Invalid-record records_returned does not match records length.")

    if records_returned > invalid_count:
        raise ValueError("Returned invalid records exceed invalid_count.")

    if any(not isinstance(record, dict) for record in records):
        raise ValueError("Invalid-record response contains a non-object record.")

    expected_status = "FAIL" if invalid_count > 0 else "PASS"
    if invalid_records["status"] != expected_status:
        raise ValueError("Invalid-record status conflicts with invalid_count.")

    if remediation_audit.get("read_only") is not True:
        raise ValueError("Remediation audit response is not marked read-only.")

    audit_count = remediation_audit.get("count")
    audit_records = remediation_audit.get("records")

    if (
        isinstance(audit_count, bool)
        or not isinstance(audit_count, int)
        or audit_count < 0
    ):
        raise ValueError("Remediation audit has an invalid count.")

    if not isinstance(audit_records, list):
        raise ValueError("Remediation audit records must be a list.")

    if audit_count != len(audit_records):
        raise ValueError("Remediation audit count does not match records length.")

    if any(not isinstance(record, dict) for record in audit_records):
        raise ValueError("Remediation audit contains a non-object record.")

    if any(record.get("dataset") != dataset for record in audit_records):
        raise ValueError("Remediation audit contains a different dataset.")

    return True



# =========================================================
# LangSmith Trace Privacy Filters
# =========================================================

def _trace_rca_inputs(inputs):
    """Allowlist metadata and evidence summaries for LangSmith inputs."""

    if not isinstance(inputs, dict):
        return {"input_type": type(inputs).__name__}

    safe = {}

    for key in ("dataset", "column", "minimum", "maximum"):
        value = inputs.get(key)
        if isinstance(value, (str, int, float)) and not isinstance(value, bool):
            safe[key] = value

    dq = inputs.get("dq_result")
    if isinstance(dq, dict):
        safe["dq_summary"] = {
            "dataset": dq.get("dataset"),
            "status": dq.get("status"),
            "check_count": len(dq.get("checks", []))
                if isinstance(dq.get("checks"), list) else None,
        }

    evidence = inputs.get("rca_evidence")
    if isinstance(evidence, dict):
        summary = {}

        for layer in ("bronze", "silver"):
            item = evidence.get(layer)
            if isinstance(item, dict):
                summary[layer] = {
                    "status": item.get("status"),
                    "invalid_count": item.get("invalid_count"),
                }

        invalid = evidence.get("invalid_records")
        if isinstance(invalid, dict):
            summary["invalid_records"] = {
                "invalid_count": invalid.get("invalid_count"),
                "records_returned": invalid.get("records_returned"),
            }

        audit = evidence.get("remediation_audit")
        if isinstance(audit, dict):
            summary["remediation_audit"] = {
                "read_only": audit.get("read_only"),
                "count": audit.get("count"),
            }

        safe["evidence_summary"] = summary

    return safe


def _trace_rca_outputs(output):
    """Allowlist non-sensitive RCA result metadata for LangSmith outputs."""

    if isinstance(output, str):
        return {
            "response_type": "text",
            "response_length": len(output),
        }

    if not isinstance(output, dict):
        return {"result_type": type(output).__name__}

    remediation = output.get("remediation")
    if not isinstance(remediation, dict):
        remediation = {}

    return {
        "dataset": output.get("dataset"),
        "dq_status": output.get("dq_status"),
        "classification": output.get("classification"),
        "confidence": output.get("confidence"),
        "remediation_action": remediation.get("action"),
        "requires_approval": remediation.get("requires_approval"),
        "evidence_count": len(output.get("evidence", []))
            if isinstance(output.get("evidence"), list) else None,
        "next_action_count": len(output.get("next_actions", []))
            if isinstance(output.get("next_actions"), list) else None,
    }




def _trace_evidence_outputs(output):
    """Expose evidence summaries only, never raw MCP records."""
    if not isinstance(output, dict):
        return {"result_type": type(output).__name__}

    summary = {}

    for key in ("bronze", "silver"):
        item = output.get(key)
        if isinstance(item, dict):
            summary[key] = {
                "status": item.get("status"),
                "invalid_count": item.get("invalid_count"),
            }

    invalid = output.get("invalid_records")
    if isinstance(invalid, dict):
        summary["invalid_records"] = {
            "status": invalid.get("status"),
            "invalid_count": invalid.get("invalid_count"),
            "records_returned": invalid.get("records_returned"),
        }

    audit = output.get("remediation_audit")
    if isinstance(audit, dict):
        summary["remediation_audit"] = {
            "read_only": audit.get("read_only"),
            "count": audit.get("count"),
        }

    scope = output.get("investigation_scope")
    if isinstance(scope, dict):
        summary["investigation_scope"] = {
            "column": scope.get("column"),
            "minimum": scope.get("minimum"),
            "maximum": scope.get("maximum"),
            "rule": scope.get("rule"),
        }

    dq = output.get("dq_result")
    if isinstance(dq, dict):
        summary["dq_summary"] = {
            "dataset": dq.get("dataset"),
            "status": dq.get("status"),
        }

    return summary


# =========================================================
def validate_dq_result(dq_result: dict, expected_dataset: str) -> bool:
    """Validate the DQ response before using its checks."""
    if not isinstance(dq_result, dict):
        raise ValueError("DQ response must be a dictionary.")

    if dq_result.get("dataset") != expected_dataset:
        raise ValueError("DQ response dataset does not match the requested dataset.")

    status = dq_result.get("status")
    if status not in {"PASS", "FAIL"}:
        raise ValueError("DQ response status must be PASS or FAIL.")

    checks = dq_result.get("checks")
    if not isinstance(checks, list):
        raise ValueError("DQ response checks must be a list.")

    for index, check in enumerate(checks):
        if not isinstance(check, dict):
            raise ValueError(f"DQ check at index {index} must be a dictionary.")

        check_status = check.get("status")
        if check_status not in {"PASS", "FAIL"}:
            raise ValueError(
                f"DQ check at index {index} has an invalid status."
            )

        check_name = check.get("check")
        if not isinstance(check_name, str) or not check_name.strip():
            raise ValueError(
                f"DQ check at index {index} has an invalid check name."
            )

        if check_name.startswith("range_check:"):
            if not check_name.split(":", 1)[1].strip():
                raise ValueError(
                    f"DQ range check at index {index} has no column name."
                )

            if check_status == "FAIL":
                range_text = check.get("range")
                if not isinstance(range_text, str) or not range_text.strip():
                    raise ValueError(
                        f"Failed range check at index {index} has no valid range."
                    )

    return True


# MCP Evidence Collection
# =========================================================

@traceable(
    name="DataPilot RCA - MCP Evidence Collection",
    run_type="chain",
    process_inputs=_trace_rca_inputs,
    process_outputs=_trace_evidence_outputs,
    tags=["datapilot-ai", "rca-agent", "mcp"],
    metadata={"agent": "rca", "stage": "evidence_collection"}
)
def collect_mcp_evidence(dataset: str, column: str, minimum: float, maximum: float) -> dict:
    """
    Collect DQ, Bronze, Silver, invalid-record and
    remediation audit evidence through MCP.

    Investigation parameters are derived from the first
    failed range check. CLI parameters are the fallback.
    """
    print("\nCollecting evidence through MCP...")
    print("  → get_dq_result")
    dq_result = call_tool(
        "get_dq_result",
        {"dataset": dataset}
    )

    validate_dq_result(dq_result, dataset)
    print("DQ response validation: PASSED")

    # -----------------------------------------------------
    # Derive investigation scope from failed DQ rule
    # -----------------------------------------------------
    investigation_column = column
    investigation_minimum = minimum
    investigation_maximum = maximum
    investigation_rule = "CLI_FALLBACK"

    checks = dq_result.get("checks", [])

    for check in checks:
        check_name = check.get("check", "")

        if (
            check.get("status") == "FAIL"
            and check_name.startswith("range_check:")
        ):
            failed_column = check_name.split(":", 1)[1]
            range_text = check.get("range", "")

            match = re.fullmatch(
                r"\s*(-?\d+(?:\.\d+)?)\s*<=\s*value\s*<=\s*(-?\d+(?:\.\d+)?)\s*",
                range_text
            )

            if not match:
                raise ValueError(
                    f"Failed DQ range check has an invalid range: {check_name}"
                )

            parsed_minimum = float(match.group(1))
            parsed_maximum = float(match.group(2))

            if not math.isfinite(parsed_minimum) or not math.isfinite(parsed_maximum):
                raise ValueError(
                    f"Failed DQ range check has non-finite boundaries: {check_name}"
                )

            if parsed_minimum > parsed_maximum:
                raise ValueError(
                    f"Failed DQ range check has minimum greater than maximum: {check_name}"
                )

            investigation_column = failed_column
            investigation_minimum = parsed_minimum
            investigation_maximum = parsed_maximum
            investigation_rule = check_name
            break

    print("\nInvestigation scope:")
    print(f"  Column  : {investigation_column}")
    print(f"  Minimum : {investigation_minimum}")
    print(f"  Maximum : {investigation_maximum}")
    print(f"  Rule    : {investigation_rule}")
    # -----------------------------------------------------
    # Bronze inspection
    # -----------------------------------------------------
    print("\n  → inspect_bronze")

    bronze_result = call_tool(
        "inspect_bronze",
        {
            "dataset": dataset,
            "column": investigation_column,
            "minimum": investigation_minimum,
            "maximum": investigation_maximum
        }
    )
    # -----------------------------------------------------
    # Silver inspection
    # -----------------------------------------------------
    print("  → inspect_silver")

    silver_result = call_tool(
        "inspect_silver",
        {
            "dataset": dataset,
            "column": investigation_column,
            "minimum": investigation_minimum,
            "maximum": investigation_maximum
        }
    )
    # -----------------------------------------------------
    # Invalid records
    # -----------------------------------------------------
    print("  → get_invalid_records")

    invalid_records = call_tool(
        "get_invalid_records",
        {
            "dataset": dataset,
            "column": investigation_column,
            "minimum": investigation_minimum,
            "maximum": investigation_maximum
        }
    )
    # -----------------------------------------------------
    # Historical remediation audit
    # -----------------------------------------------------
    print("  → get_remediation_audit")

    remediation_audit = call_tool(
        "get_remediation_audit",
        {
            "dataset": dataset,
            "limit": 5
        }
    )
    # -----------------------------------------------------
    # Evidence validation
    # -----------------------------------------------------
    validate_mcp_evidence(
        dq_result=dq_result,
        bronze_result=bronze_result,
        silver_result=silver_result,
        expected_column=investigation_column,
        expected_minimum=investigation_minimum,
        expected_maximum=investigation_maximum
    )

    validate_additional_mcp_evidence(
        dataset=dataset,
        column=investigation_column,
        minimum=investigation_minimum,
        maximum=investigation_maximum,
        invalid_records=invalid_records,
        remediation_audit=remediation_audit
    )

    print("MCP evidence validation: PASSED")

    evidence_bundle = {
        "dq_result": dq_result,
        "bronze": bronze_result,
        "silver": silver_result,
        "invalid_records": invalid_records,
        "remediation_audit": remediation_audit,
        "investigation_scope": {
            "column": investigation_column,
            "minimum": investigation_minimum,
            "maximum": investigation_maximum,
            "rule": investigation_rule
        }
    }

    print("\nMCP evidence collection completed.")

    return evidence_bundle


# =========================================================
# OpenAI RCA Analysis
# =========================================================

@traceable(
    name="DataPilot RCA - OpenAI Analysis",
    run_type="llm",
    process_inputs=_trace_rca_inputs,
    process_outputs=_trace_rca_outputs,
    tags=["datapilot-ai", "rca-agent", "llm"],
    metadata={"agent": "rca", "stage": "openai_analysis"}
)
def analyze_with_openai(dq_result, rca_evidence):
    """
    Send the MCP evidence bundle to OpenAI
    for structured RCA reasoning.
    """

    evidence = {
        "dq_result": dq_result,
        "rca_evidence": rca_evidence
    }

    response = client.responses.create(
        model=MODEL,
        instructions=SYSTEM_PROMPT,
        input=(
            "Analyze the following DataPilot AI "
            "data-quality incident.\n\n"
            "The evidence was collected through "
            "DataPilot AI MCP tools.\n\n"
            "Evidence:\n"
            +
            json.dumps(
                evidence,
                indent=2,
                default=str
            )
        ),
        text={
            "format": {
                "type": "json_schema",
                "name": "datapilot_rca",
                "description": (
                    "Structured RCA result for a "
                    "DataPilot AI data-quality incident."
                ),
                "schema": RCA_SCHEMA,
                "strict": True
            }
        }
    )

    return response.output_text


# =========================================================
# Parse Structured Response
# =========================================================

def parse_llm_response(response_text):
    """
    Convert the structured OpenAI response into
    a Python dictionary.
    """

    try:
        result = json.loads(response_text)

    except json.JSONDecodeError as exc:

        raise RuntimeError(
            "OpenAI returned invalid JSON.\n"
            f"Response:\n{response_text}"
        ) from exc

    return result


# =========================================================
# RCA Orchestration
# =========================================================

@traceable(
    name="DataPilot RCA Agent",
    run_type="chain",
    process_inputs=_trace_rca_inputs,
    process_outputs=_trace_rca_outputs,
    tags=["datapilot-ai", "rca-agent"],
    metadata={"agent": "rca", "stage": "orchestration"}
)
def run_rca(
    dq_result,
    rca_evidence
):
    """
    Execute the RCA reasoning workflow.
    """

    response_text = analyze_with_openai(
        dq_result=dq_result,
        rca_evidence=rca_evidence
    )

    result = parse_llm_response(
        response_text
    )

    result = validate_rca_result(
    result=result,
    dq_result=dq_result,
    rca_evidence=rca_evidence
    )

    print("RCA output guardrail validation: PASSED")

    return result


# =========================================================
# Save RCA Result
# =========================================================

def save_result(
    path,
    result
):
    """
    Save the structured RCA result to JSON.
    """

    output_directory = os.path.dirname(path)

    if output_directory:
        os.makedirs(
            output_directory,
            exist_ok=True
        )

    with open(
        path,
        "w"
    ) as file:

        json.dump(
            result,
            file,
            indent=2,
            default=str
        )

    print(
        f"\nRCA result saved to: {path}"
    )


# =========================================================
# Print RCA Result
# =========================================================

def print_rca_result(result):

    print("\n")
    print("========================================")
    print("DATAPILOT AI RCA RESULT")
    print("========================================")

    print(f"Dataset        : " f"{result['dataset']}")

    print(f"DQ Status      : "f"{result['dq_status']}")

    print(f"Classification : " f"{result['classification']}")

    print(f"Confidence     : " f"{result['confidence']}")

    print("\nRoot Cause:")

    print(result["root_cause"])

    print("\nEvidence:")

    for evidence in result["evidence"]:
        print(f"  - {evidence}")

    print("\nRemediation:")

    print(f"  Action            : " f"{result['remediation']['action']}")

    print(f"  Requires Approval : " f"{result['remediation']['requires_approval']}")

    print(f"  Reason            : " f"{result['remediation']['reason']}")

    print("\nNext Actions:")

    for action in result["next_actions"]:
        print(f"  - {action}")

    print(
        "========================================"
    )


# =========================================================
# Main
# =========================================================

def main():

    parser = argparse.ArgumentParser(
        description=(
            "DataPilot AI MCP-powered "
            "OpenAI RCA Agent"
        )
    )

    # -----------------------------------------------------
    # Dataset
    # -----------------------------------------------------

    parser.add_argument(
        "--dataset",
        required=True,
        help="Dataset to investigate."
    )

    # -----------------------------------------------------
    # Column
    # -----------------------------------------------------

    parser.add_argument(
        "--column",
        required=True,
        help="Column containing the suspected anomaly."
    )

    # -----------------------------------------------------
    # Expected minimum
    # -----------------------------------------------------

    parser.add_argument(
        "--minimum",
        required=True,
        type=float,
        help="Minimum valid value."
    )

    # -----------------------------------------------------
    # Expected maximum
    # -----------------------------------------------------

    parser.add_argument(
        "--maximum",
        required=True,
        type=float,
        help="Maximum valid value."
    )

    # -----------------------------------------------------
    # Output
    # -----------------------------------------------------

    parser.add_argument(
        "--output",
        required=True,
        help="Path for the RCA JSON result."
    )

    args = parser.parse_args()

    # =====================================================
    # MCP Evidence Collection
    # =====================================================

    with tracing_context(
        tags=[
            "datapilot-ai",
            "rca-agent",
            "data-quality",
            "mcp",
            "production-pattern"
        ],
        metadata={
            "dataset": args.dataset,
            "column": args.column,
            "environment": "dev",
            "agent": "rca",
            "project": "datapilot-ai"
        }
    ):

        evidence_bundle = collect_mcp_evidence(
            dataset=args.dataset,
            column=args.column,
            minimum=args.minimum,
            maximum=args.maximum
        )

        # -------------------------------------------------
        # Separate DQ and investigation evidence
        # -------------------------------------------------

        dq_result = evidence_bundle["dq_result"]

        rca_evidence = {
            "bronze": evidence_bundle["bronze"],
            "silver": evidence_bundle["silver"],
            "invalid_records": evidence_bundle["invalid_records"],
            "remediation_audit": evidence_bundle["remediation_audit"]
        }

        # -------------------------------------------------
        # RCA reasoning
        # -------------------------------------------------

        result = run_rca(
            dq_result=dq_result,
            rca_evidence=rca_evidence
        )

    # =====================================================
    # Save Result
    # =====================================================

    save_result(
        path=args.output,
        result=result
    )

    # =====================================================
    # Display Result
    # =====================================================

    print_rca_result(
        result
    )


# =========================================================
# Entry Point
# =========================================================

if __name__ == "__main__":
    main()