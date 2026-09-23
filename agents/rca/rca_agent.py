import argparse
import json
import os

from dotenv import load_dotenv
from langsmith import traceable, tracing_context
from openai import OpenAI

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

- If the anomaly exists in Bronze and Silver,
  classify as UPSTREAM_DATA_QUALITY.

- If the anomaly does not exist in Bronze but appears
  in Silver, classify as TRANSFORMATION_DATA_QUALITY.

- If the available evidence is insufficient,
  classify as RCA_INCONCLUSIVE.

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
        "next_actions",
        "confidence"
    ],
    "additionalProperties": False
}


# =========================================================
# MCP Evidence Validation
# =========================================================

def validate_mcp_evidence(
    dq_result,
    bronze_result,
    silver_result
):
    """
    Validate the evidence returned by MCP tools
    before sending it to the LLM.
    """

    if not isinstance(dq_result, dict):
        raise ValueError(
            "DQ result returned by MCP is not a JSON object."
        )

    if not isinstance(bronze_result, dict):
        raise ValueError(
            "Bronze result returned by MCP is not a JSON object."
        )

    if not isinstance(silver_result, dict):
        raise ValueError(
            "Silver result returned by MCP is not a JSON object."
        )

    if "dataset" not in dq_result:
        raise ValueError(
            "MCP DQ result does not contain dataset."
        )

    if bronze_result.get("dataset") != dq_result.get("dataset"):
        raise ValueError(
            "Bronze evidence dataset does not match DQ dataset."
        )

    if silver_result.get("dataset") != dq_result.get("dataset"):
        raise ValueError(
            "Silver evidence dataset does not match DQ dataset."
        )


# =========================================================
# MCP Evidence Collection
# =========================================================

@traceable(
    name="DataPilot RCA - MCP Evidence Collection",
    run_type="chain"
)
def collect_mcp_evidence(dataset: str,column: str,minimum: float,maximum: float) -> dict:
    """
    Collect DQ, Bronze, Silver and invalid-record
    evidence through MCP.
    """

    print("\nCollecting evidence through MCP...")

    print("  → get_dq_result")

    dq_result = call_tool(
        "get_dq_result",
        {
            "dataset": dataset
        }
    )

    print("  → inspect_bronze")

    bronze_result = call_tool(
        "inspect_bronze",
        {
            "dataset": dataset,
            "column": column,
            "minimum": minimum,
            "maximum": maximum
        }
    )

    print("  → inspect_silver")

    silver_result = call_tool(
        "inspect_silver",
        {
            "dataset": dataset,
            "column": column,
            "minimum": minimum,
            "maximum": maximum
        }
    )

    print("  → get_invalid_records")

    invalid_records = call_tool(
        "get_invalid_records",
        {
            "dataset": dataset,
            "column": column,
            "minimum": minimum,
            "maximum": maximum
        }
    )

    validate_mcp_evidence(
        dq_result=dq_result,
        bronze_result=bronze_result,
        silver_result=silver_result
    )

    evidence_bundle = {
        "dq_result": dq_result,
        "bronze": bronze_result,
        "silver": silver_result,
        "invalid_records": invalid_records
    }

    print("MCP evidence collection completed.")

    return evidence_bundle


# =========================================================
# OpenAI RCA Analysis
# =========================================================

@traceable(
    name="DataPilot RCA - OpenAI Analysis",
    run_type="llm"
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
    run_type="chain"
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

    print(
        f"Dataset        : "
        f"{result['dataset']}"
    )

    print(
        f"DQ Status      : "
        f"{result['dq_status']}"
    )

    print(
        f"Classification : "
        f"{result['classification']}"
    )

    print(
        f"Confidence     : "
        f"{result['confidence']}"
    )

    print(
        "\nRoot Cause:"
    )

    print(
        result["root_cause"]
    )

    print(
        "\nEvidence:"
    )

    for evidence in result["evidence"]:
        print(
            f"  - {evidence}"
        )

    print(
        "\nRecommendation:"
    )

    print(
        result["recommendation"]
    )

    print(
        "\nNext Actions:"
    )

    for action in result["next_actions"]:
        print(
            f"  - {action}"
        )

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
            "invalid_records": evidence_bundle["invalid_records"]

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