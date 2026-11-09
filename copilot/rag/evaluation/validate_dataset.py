
from collections import Counter

from copilot.rag.evaluation.evaluation_dataset import (
    EVALUATION_DATASET,
)



REQUIRED_FIELDS = [
    "id",
    "category",
    "question",
    "expected_sources",
    "reference_answer",
]


def validate_dataset():

    errors = []
    warnings = []

    if not EVALUATION_DATASET:
        errors.append("Evaluation dataset is empty.")

    ids = [
        item.get("id")
        for item in EVALUATION_DATASET
    ]

    duplicates = [
        item_id
        for item_id, count in Counter(ids).items()
        if count > 1
    ]

    if duplicates:
        errors.append(
            f"Duplicate IDs: {duplicates}"
        )

    for index, item in enumerate(
        EVALUATION_DATASET,
        start=1,
    ):

        item_id = item.get("id", f"ROW-{index}")

        for field in REQUIRED_FIELDS:

            if field not in item:
                errors.append(
                    f"{item_id}: Missing field '{field}'"
                )

        if not isinstance(item.get("question"), str) or not item.get("question", "").strip():
            errors.append(
                f"{item_id}: Invalid question"
            )

        if not isinstance(item.get("expected_sources"), list) or not item.get("expected_sources"):
            errors.append(
                f"{item_id}: expected_sources must be a non-empty list"
            )

        reference = item.get("reference_answer")

        if not isinstance(reference, str) or not reference.strip():
            warnings.append(
                f"{item_id}: Missing reference_answer"
            )

    print("\nDATAPILOT AI DATASET VALIDATION")
    print("=" * 55)

    print("Total questions:", len(EVALUATION_DATASET))
    print("Unique IDs:", len(set(ids)))

    print(
        "Questions with reference answers:",
        sum(
            1
            for item in EVALUATION_DATASET
            if isinstance(item.get("reference_answer"), str)
            and item["reference_answer"].strip()
        ),
    )

    print("\nERRORS:")

    for error in errors:
        print("ERROR:", error)

    print("\nWARNINGS:")

    for warning in warnings:
        print("WARNING:", warning)

    if errors:
        raise ValueError(
            "Dataset validation failed."
        )

    if warnings:
        print("\nDataset structure is valid, but RAGAS evaluation is not ready.")

    else:
        print("\nDataset validation passed.")

    return not errors and not warnings


if __name__ == "__main__":
    validate_dataset()
