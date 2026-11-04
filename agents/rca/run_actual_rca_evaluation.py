from dotenv import load_dotenv
from langsmith import Client

from agents.rca.rca_evaluators import EVALUATORS
from agents.rca.rca_eval_actual_target import rca_actual_target


load_dotenv(override=True)

DATASET_ID = "f5afb6ec-707a-4c8d-bc5b-0ebaf1507873"


def main():
    client = Client()

    dataset = client.read_dataset(dataset_id=DATASET_ID)

    print(f"Dataset: {dataset.name}")
    print(f"Dataset ID: {DATASET_ID}")
    print(f"Evaluators: {len(EVALUATORS)}")

    if len(EVALUATORS) != 4:
        raise RuntimeError("Expected exactly four RCA evaluators.")

    if __import__("os").getenv("DATAPILOT_ALLOW_LIVE_RCA_EVAL") != "true":
        raise RuntimeError(
            "Live RCA evaluation is disabled. "
            "Explicit opt-in is required."
        )

    results = client.evaluate(
        rca_actual_target,
        data=DATASET_ID,
        evaluators=EVALUATORS,
        experiment_prefix="datapilot-rca-actual-llm",
        description=(
            "Actual LLM-powered RCA evaluation against the "
            "five-scenario synthetic evaluation dataset."
        ),
        max_concurrency=1,
        num_repetitions=1,
        blocking=True,
        upload_results=True,
    )

    print("\nActual RCA evaluation completed.")
    print(results)


if __name__ == "__main__":
    main()