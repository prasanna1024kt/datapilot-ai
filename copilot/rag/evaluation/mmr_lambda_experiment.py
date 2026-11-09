
import json
from datetime import datetime, timezone
from pathlib import Path

from copilot.rag.retrieval.retriever import RAGRetriever
from copilot.rag.evaluation.evaluation_dataset import (
    EVALUATION_DATASET,
)
from copilot.rag.evaluation.mmr_limit_experiment import (
    calculate_metrics,
    aggregate,
)

LAMBDA_VALUES = [0.5, 0.6, 0.7, 0.8, 0.9]

CANDIDATE_K = 20
DOCUMENT_K = 5
THRESHOLD = 0.40

REPORT = Path(
    "/app/copilot/rag/evaluation/reports/"
    "mmr_lambda_experiment.json"
)


def run_experiment():

    results_by_lambda = {
        str(value): []
        for value in LAMBDA_VALUES
    }

    print("\nDataPilot AI - MMR Lambda Experiment")
    print("=" * 65)

    for test_case in EVALUATION_DATASET:

        print(f"\n{test_case['id']}")

        for lambda_value in LAMBDA_VALUES:

            retriever = RAGRetriever(
                top_k=DOCUMENT_K,
                candidate_k=CANDIDATE_K,
                similarity_threshold=THRESHOLD,
                strategy="mmr",
                mmr_lambda=lambda_value,
                max_chunks_per_document=1,
            )

            retrieved = retriever.retrieve(
                test_case["question"]
            )

            metrics = calculate_metrics(
                retrieved,
                test_case["expected_sources"],
            )

            results_by_lambda[str(lambda_value)].append({
                "id": test_case["id"],
                "metrics": metrics,
                "chunks": retrieved,
            })

            print(
                f"lambda={lambda_value:.1f} | "
                f"MRR={metrics['mrr']:.3f} | "
                f"Recall={metrics['recall_at_k']:.3f} | "
                f"Unique docs={metrics['unique_documents']} | "
                f"Similarity={metrics['average_similarity']:.3f}"
            )

    summary = {
        value: aggregate([
            item["metrics"] for item in results
        ])
        for value, results in results_by_lambda.items()
    }

    report = {
        "experiment": "MMR lambda comparison",
        "generated_at": datetime.now(
            timezone.utc
        ).isoformat(),
        "configuration": {
            "candidate_k": CANDIDATE_K,
            "document_k": DOCUMENT_K,
            "threshold": THRESHOLD,
            "max_chunks_per_document": 1,
            "lambda_values": LAMBDA_VALUES,
        },
        "summary": summary,
        "results": results_by_lambda,
    }

    REPORT.parent.mkdir(parents=True, exist_ok=True)

    with REPORT.open("w") as file:
        json.dump(report, file, indent=2)

    print("\nAGGREGATE RESULTS")
    print("=" * 65)

    for value, metrics in summary.items():
        print(f"\nLambda: {value}")

        for metric, score in metrics.items():
            print(f"{metric}: {score:.4f}")

    print(f"\nReport saved: {REPORT}")


if __name__ == "__main__":
    run_experiment()