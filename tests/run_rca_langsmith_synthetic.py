import json
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from langsmith import tracing_context
from agents.rca import rca_agent


DATASET = "synthetic_rca_langsmith_test"
COLUMN = "payment_installments"

DQ_RESULT = {
    "dataset": DATASET,
    "status": "FAIL",
    "checks": [
        {
            "check": "range_check:payment_installments",
            "status": "FAIL",
            "range": "1 <= value <= 100",
        }
    ],
}

BRONZE_RESULT = {
    "dataset": DATASET,
    "layer": "bronze",
    "column": COLUMN,
    "path": "synthetic/bronze/data",
    "status": "FAIL",
    "minimum": 1.0,
    "maximum": 100.0,
    "invalid_count": 2,
    "sample_invalid_values": [0, 101],
}

SILVER_RESULT = {
    "dataset": DATASET,
    "layer": "silver",
    "column": COLUMN,
    "path": "synthetic/silver/data",
    "status": "PASS",
    "minimum": 1.0,
    "maximum": 100.0,
    "invalid_count": 0,
    "sample_invalid_values": [],
}

INVALID_RECORDS = {
    "dataset": DATASET,
    "layer": "bronze",
    "column": COLUMN,
    "minimum": 1.0,
    "maximum": 100.0,
    "status": "FAIL",
    "invalid_count": 2,
    "records_returned": 2,
    "records": [
        {"synthetic_id": "record_001"},
        {"synthetic_id": "record_002"},
    ],
}

REMEDIATION_AUDIT = {
    "read_only": True,
    "count": 1,
    "records": [
        {
            "dataset": DATASET,
            "action": "QUARANTINE_RECORDS",
            "execution_mode": "DRY_RUN",
        }
    ],
}

SYNTHETIC_RCA_RESULT = {
    "dataset": DATASET,
    "dq_status": "FAIL",
    "classification": "UPSTREAM_DATA_QUALITY",
    "root_cause": (
        "Synthetic Bronze evidence contains two invalid values "
        "outside the configured range."
    ),
    "evidence": [
        "Bronze evidence reports two invalid records.",
        "Silver evidence reports zero invalid records.",
    ],
    "recommendation": "Review the invalid source records.",
    "remediation": {
        "action": "MANUAL_REVIEW",
        "requires_approval": True,
        "reason": "Synthetic trace test; no remediation is executed.",
    },
    "next_actions": [
        "Review the source data.",
        "Validate the proposed remediation before approval.",
    ],
    "confidence": "MEDIUM",
}


def synthetic_mcp_tool(tool_name, arguments):
    """Return synthetic responses without invoking an MCP server."""

    responses = {
        "get_dq_result": DQ_RESULT,
        "inspect_bronze": BRONZE_RESULT,
        "inspect_silver": SILVER_RESULT,
        "get_invalid_records": INVALID_RECORDS,
        "get_remediation_audit": REMEDIATION_AUDIT,
    }

    if tool_name not in responses:
        raise ValueError(f"Unexpected MCP tool requested: {tool_name}")

    return responses[tool_name]


def main():
    print("Starting synthetic RCA LangSmith trace test.")

    fake_openai_response = SimpleNamespace(
        output_text=json.dumps(SYNTHETIC_RCA_RESULT)
    )

    with (
        patch.object(
            rca_agent,
            "call_tool",
            side_effect=synthetic_mcp_tool,
        ) as mocked_mcp,
        patch.object(
            rca_agent.client.responses,
            "create",
            return_value=fake_openai_response,
        ) as mocked_openai,
    ):
        with tracing_context(
            tags=[
                "datapilot-ai",
                "rca-agent",
                "synthetic-test",
            ],
            metadata={
                "dataset": DATASET,
                "environment": "synthetic",
                "test_type": "langsmith_trace_verification",
            },
        ):
            # First trace: MCP evidence collection.
            evidence_bundle = rca_agent.collect_mcp_evidence(
                dataset=DATASET,
                column=COLUMN,
                minimum=1.0,
                maximum=100.0,
            )

            dq_result = evidence_bundle["dq_result"]

            rca_evidence = {
                "bronze": evidence_bundle["bronze"],
                "silver": evidence_bundle["silver"],
                "invalid_records": evidence_bundle["invalid_records"],
                "remediation_audit": evidence_bundle["remediation_audit"],
            }

            # Second trace: RCA orchestration with nested OpenAI analysis.
            result = rca_agent.run_rca(
                dq_result=dq_result,
                rca_evidence=rca_evidence,
            )

        assert result["dataset"] == DATASET
        assert result["classification"] == "UPSTREAM_DATA_QUALITY"
        assert result["remediation"]["requires_approval"] is True
        assert mocked_mcp.call_count == 5
        assert mocked_openai.call_count == 1

    print("\nSynthetic RCA trace test: PASSED")
    print("MCP calls mocked:", mocked_mcp.call_count)
    print("OpenAI calls mocked:", mocked_openai.call_count)
    print("Dataset:", result["dataset"])
    print("Classification:", result["classification"])
    print("Confidence:", result["confidence"])
    print("Approval required:", result["remediation"]["requires_approval"])
    print("\nExpected LangSmith traces:")
    print("1. DataPilot RCA - MCP Evidence Collection")
    print("2. DataPilot RCA Agent")
    print("   └── DataPilot RCA - OpenAI Analysis")
    print("\nNo real MCP or OpenAI requests were made.")


if __name__ == "__main__":
    main()
