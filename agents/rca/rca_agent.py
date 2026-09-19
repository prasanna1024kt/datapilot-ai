import argparse
import json
import os

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


# ---------------------------------------------------------
# OpenAI Client
# ---------------------------------------------------------

client = OpenAI(
    api_key=OPENAI_API_KEY
)


# ---------------------------------------------------------
# Configuration
# ---------------------------------------------------------

MODEL = "gpt-5.6"


SYSTEM_PROMPT = """
You are the RCA Agent for DataPilot AI.

You analyze data-quality incidents using the evidence
provided by the DataPilot AI data engineering pipeline.

Your responsibility is to:

1. Identify the failed data-quality check.
2. Identify the affected dataset and column.
3. Identify invalid values and their counts.
4. Compare DQ evidence with Bronze and Silver evidence.
5. Determine the most likely origin of the problem.
6. Distinguish observed evidence from inference.
7. Recommend appropriate next actions.
8. Never invent evidence that is not present in the input.

For Bronze and Silver comparison:

- If the anomaly exists in Bronze and Silver, classify it as
  UPSTREAM_DATA_QUALITY unless there is evidence suggesting
  otherwise.

- If the anomaly does not exist in Bronze but appears in Silver,
  classify it as TRANSFORMATION_DATA_QUALITY.

- If the evidence is insufficient, classify it as
  RCA_INCONCLUSIVE.

Return the RCA as valid JSON with this structure:

{
  "dataset": "string",
  "dq_status": "PASS|FAIL",
  "classification": "string",
  "root_cause": "string",
  "evidence": [
    "string"
  ],
  "recommendation": "string",
  "next_actions": [
    "string"
  ],
  "confidence": "HIGH|MEDIUM|LOW"
}

Do not include Markdown code fences around the JSON.
"""


# ---------------------------------------------------------
# JSON Loader
# ---------------------------------------------------------

def load_json(path):
    """
    Load a JSON file from disk.
    """

    with open(path, "r") as file:
        return json.load(file)


# ---------------------------------------------------------
# Evidence Validation
# ---------------------------------------------------------

def validate_evidence(dq_result, rca_evidence):
    """
    Perform basic validation before sending evidence
    to the LLM.
    """

    if not isinstance(dq_result, dict):
        raise ValueError(
            "DQ result must be a JSON object."
        )

    if not isinstance(rca_evidence, dict):
        raise ValueError(
            "RCA evidence must be a JSON object."
        )

    if "dataset" not in dq_result:
        raise ValueError(
            "DQ result does not contain dataset."
        )

    if "investigations" not in rca_evidence:
        raise ValueError(
            "RCA evidence does not contain investigations."
        )


# ---------------------------------------------------------
# OpenAI RCA Analysis
# ---------------------------------------------------------

def analyze_with_openai(
    dq_result,
    rca_evidence,
):
    """
    Send DQ and Bronze/Silver evidence to OpenAI
    for RCA reasoning.
    """

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
                default=str,
            )
        ),
    )

    return response.output_text


# ---------------------------------------------------------
# Parse LLM Response
# ---------------------------------------------------------

def parse_llm_response(response_text):
    """
    Convert the LLM JSON response into a Python object.
    """

    try:
        return json.loads(response_text)

    except json.JSONDecodeError as exc:
        raise RuntimeError(
            "OpenAI returned an invalid JSON response.\n"
            f"Response:\n{response_text}"
        ) from exc


# ---------------------------------------------------------
# Save RCA Result
# ---------------------------------------------------------

def save_result(path, result):
    """
    Persist the structured RCA report.
    """

    output_directory = os.path.dirname(path)

    if output_directory:
        os.makedirs(
            output_directory,
            exist_ok=True,
        )

    with open(path, "w") as file:
        json.dump(
            result,
            file,
            indent=2,
            default=str,
        )

    print(
        f"LLM RCA result saved to: {path}"
    )


# ---------------------------------------------------------
# Console Output
# ---------------------------------------------------------

def print_rca_result(result):
    """
    Display a readable RCA summary.
    """

    print("\n========================================")
    print("OPENAI RCA RESULT")
    print("========================================")

    print(
        f"Dataset       : "
        f"{result.get('dataset')}"
    )

    print(
        f"DQ Status     : "
        f"{result.get('dq_status')}"
    )

    print(
        f"Classification : "
        f"{result.get('classification')}"
    )

    print(
        f"Confidence    : "
        f"{result.get('confidence')}"
    )

    print(
        f"\nRoot Cause:\n"
        f"{result.get('root_cause')}"
    )

    print("\nEvidence:")

    for evidence in result.get(
        "evidence",
        [],
    ):
        print(
            f"  - {evidence}"
        )

    print(
        "\nRecommendation:\n"
        f"{result.get('recommendation')}"
    )

    print("\nNext Actions:")

    for action in result.get(
        "next_actions",
        [],
    ):
        print(
            f"  - {action}"
        )

    print("========================================")


# ---------------------------------------------------------
# Main
# ---------------------------------------------------------

def main():

    parser = argparse.ArgumentParser(
        description=(
            "DataPilot AI OpenAI RCA Agent"
        )
    )

    parser.add_argument(
        "--dq-result",
        required=True,
        help=(
            "Path to DQ result JSON"
        ),
    )

    parser.add_argument(
        "--rca-evidence",
        required=True,
        help=(
            "Path to Bronze/Silver RCA evidence JSON"
        ),
    )

    parser.add_argument(
        "--output",
        required=True,
        help=(
            "Path for final LLM RCA JSON"
        ),
    )

    args = parser.parse_args()

    # ---------------------------------------------
    # Load evidence
    # ---------------------------------------------

    dq_result = load_json(
        args.dq_result
    )

    rca_evidence = load_json(
        args.rca_evidence
    )

    # ---------------------------------------------
    # Validate evidence
    # ---------------------------------------------

    validate_evidence(
        dq_result=dq_result,
        rca_evidence=rca_evidence,
    )

    # ---------------------------------------------
    # LLM reasoning
    # ---------------------------------------------

    response_text = analyze_with_openai(
        dq_result=dq_result,
        rca_evidence=rca_evidence,
    )

    # ---------------------------------------------
    # Parse response
    # ---------------------------------------------

    result = parse_llm_response(
        response_text
    )

    # ---------------------------------------------
    # Save result
    # ---------------------------------------------

    save_result(
        path=args.output,
        result=result,
    )

    # ---------------------------------------------
    # Display result
    # ---------------------------------------------

    print_rca_result(
        result
    )


if __name__ == "__main__":
    main()