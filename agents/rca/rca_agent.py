import argparse
import json
import os
from langsmith import traceable, tracing_context
from dotenv import load_dotenv
from openai import OpenAI


# ---------------------------------------------------------
# Environment
# ---------------------------------------------------------

load_dotenv()

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

if not OPENAI_API_KEY:
    raise RuntimeError(
        "OPENAI_API_KEY is not configured. "
        "Add it to the .env file."
    )

client = OpenAI(api_key=OPENAI_API_KEY)

MODEL = "gpt-5.6"


# ---------------------------------------------------------
# System instructions
# ---------------------------------------------------------

SYSTEM_PROMPT = """
You are the RCA Agent for DataPilot AI.

You analyze data-quality incidents using evidence produced
by the DataPilot AI data engineering pipeline.

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

The evidence array must contain only facts supported
by the supplied input.

Do not invent source records, business rules, timestamps,
or system behavior.
"""


# ---------------------------------------------------------
# Structured output schema
# ---------------------------------------------------------

RCA_SCHEMA = {
    "type": "object",
    "properties": {
        "dataset": {
            "type": "string"
        },
        "dq_status": {
            "type": "string",
            "enum": ["PASS", "FAIL"]
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
            "enum": ["HIGH", "MEDIUM", "LOW"]
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


# ---------------------------------------------------------
# Utility functions
# ---------------------------------------------------------

def load_json(path):
    with open(path, "r") as file:
        return json.load(file)


def validate_evidence(dq_result, rca_evidence):
    if not isinstance(dq_result, dict):
        raise ValueError("DQ result must be a JSON object.")

    if not isinstance(rca_evidence, dict):
        raise ValueError("RCA evidence must be a JSON object.")

    if "dataset" not in dq_result:
        raise ValueError(
            "DQ result does not contain dataset."
        )

    if "investigations" not in rca_evidence:
        raise ValueError(
            "RCA evidence does not contain investigations."
        )


# ---------------------------------------------------------
# OpenAI RCA analysis
# ---------------------------------------------------------
@traceable(
    name="DataPilot RCA - OpenAI Analysis",
    run_type="llm"
)
def analyze_with_openai(dq_result, rca_evidence):

    evidence = {
        "dq_result": dq_result,
        "rca_evidence": rca_evidence,
    }

    response = client.responses.create(
        model=MODEL,
        instructions=SYSTEM_PROMPT,
        input=(
            "Analyze the following DataPilot AI "
            "data-quality incident.\n\n"
            "Evidence:\n"
            + json.dumps(
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
                "strict": True,
            }
        },
    )

    return response.output_text


# ---------------------------------------------------------
# Parse structured response
# ---------------------------------------------------------

def parse_llm_response(response_text):

    try:
        result = json.loads(response_text)
    except json.JSONDecodeError as exc:
        raise RuntimeError(
            "OpenAI returned invalid JSON.\n"
            f"Response:\n{response_text}"
        ) from exc

    return result


# ---------------------------------------------------------
# Save result
# ---------------------------------------------------------
@traceable(
    name="DataPilot RCA Agent",
    run_type="chain"
)
def run_rca(dq_result, rca_evidence):

    response_text = analyze_with_openai(
        dq_result=dq_result,
        rca_evidence=rca_evidence
    )

    result = parse_llm_response(
        response_text
    )

    return result

def save_result(path, result):

    output_directory = os.path.dirname(path)

    if output_directory:
        os.makedirs(
            output_directory,
            exist_ok=True
        )

    with open(path, "w") as file:
        json.dump(
            result,
            file,
            indent=2,
            default=str
        )

    print(
        f"LLM RCA result saved to: {path}"
    )


# ---------------------------------------------------------
# Print RCA
# ---------------------------------------------------------

def print_rca_result(result):

    print("\n========================================")
    print("OPENAI RCA RESULT")
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
        f"\nRoot Cause:\n"
        f"{result['root_cause']}"
    )

    print("\nEvidence:")

    for evidence in result["evidence"]:
        print(f"  - {evidence}")

    print(
        f"\nRecommendation:\n"
        f"{result['recommendation']}"
    )

    print("\nNext Actions:")

    for action in result["next_actions"]:
        print(f"  - {action}")

    print("========================================")


# ---------------------------------------------------------
# Main
# ---------------------------------------------------------

def main():

    parser = argparse.ArgumentParser(
        description="DataPilot AI OpenAI RCA Agent"
    )

    parser.add_argument(
        "--dq-result",
        required=True
    )

    parser.add_argument(
        "--rca-evidence",
        required=True
    )

    parser.add_argument(
        "--output",
        required=True
    )

    args = parser.parse_args()

    dq_result = load_json(
        args.dq_result
    )

    rca_evidence = load_json(
        args.rca_evidence
    )

    validate_evidence(
        dq_result=dq_result,
        rca_evidence=rca_evidence
    )

    response_text = analyze_with_openai(
        dq_result=dq_result,
        rca_evidence=rca_evidence
    )

    with tracing_context(
    tags=[
        "datapilot-ai",
        "rca-agent",
        "data-quality",
        "production-pattern"
    ],
    metadata={
        "dataset": dq_result.get("dataset"),
        "dq_status": dq_result.get("dq_status"),
        "environment": "dev",
        "agent": "rca",
        "project": "datapilot-ai"
    }
   ):
        result = run_rca(
            dq_result=dq_result,
            rca_evidence=rca_evidence
        )

    save_result(
        path=args.output,
        result=result
    )

    print_rca_result(result)


if __name__ == "__main__":
    main()