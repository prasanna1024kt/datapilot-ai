
import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

from copilot.rag.retrieval.retriever import RAGRetriever
from copilot.rag.evaluation.evaluation_dataset import (
    EVALUATION_DATASET,
)


# ============================================================
# EXPERIMENT CONFIGURATION
# ============================================================

CANDIDATE_K = 20
DOCUMENT_K = 5
SIMILARITY_THRESHOLD = 0.40
MMR_LAMBDA = 0.70

DOCUMENT_LIMITS = [1, 2, 3]

REPORT_DIRECTORY = Path(
    "/app/copilot/rag/evaluation/reports"
)

REPORT_FILE = (
    REPORT_DIRECTORY / "mmr_limit_experiment.json"
)


# ============================================================
# METRICS
# ============================================================

def calculate_metrics(results, expected_sources):
    expected = set(expected_sources)

    unique_results = []
    seen_documents = set()

    for result in results:
        document_id = result.get("document_id")

        if not document_id:
            continue

        if document_id not in seen_documents:
            seen_documents.add(document_id)
            unique_results.append(result)

    top_documents = unique_results[:DOCUMENT_K]

    retrieved_documents = [
        result["document_id"]
        for result in top_documents
    ]

    relevant_documents = [
        document
        for document in retrieved_documents
        if document in expected
    ]

    reciprocal_rank = 0.0

    for rank, document in enumerate(
        retrieved_documents, start=1
    ):
        if document in expected:
            reciprocal_rank = 1.0 / rank
            break

    valid_document_ids = [
        result["document_id"]
        for result in results
        if result.get("document_id")
    ]

    unique_document_count = len(set(valid_document_ids))

    duplicate_rate = (
        1 - unique_document_count / len(valid_document_ids)
        if valid_document_ids else 0.0
    )

    document_counts = Counter(valid_document_ids)

    return {
        "hit_at_k": int(bool(relevant_documents)),
        "mrr": reciprocal_rank,
        "precision_at_k": (
            len(relevant_documents) / DOCUMENT_K
        ),
        "recall_at_k": (
            len(relevant_documents) / len(expected)
            if expected else 0.0
        ),
        "unique_documents": unique_document_count,
        "evaluated_documents": len(top_documents),
        "retrieved_chunks": len(results),
        "duplicate_document_rate": duplicate_rate,
        "average_similarity": (
            sum(r["score"] for r in top_documents)
            / len(top_documents)
            if top_documents else 0.0
        ),
        "retrieved_documents": retrieved_documents,
        "relevant_documents": relevant_documents,
        "chunks_per_document": dict(document_counts),
        "questions_with_no_results": int(not bool(results)),
    }


# ============================================================
# AGGREGATION
# ============================================================

def aggregate(results):
    metric_names = [
        "hit_at_k",
        "mrr",
        "precision_at_k",
        "recall_at_k",
        "unique_documents",
        "evaluated_documents",
        "retrieved_chunks",
        "duplicate_document_rate",
        "average_similarity",
        "questions_with_no_results",
    ]

    total = len(results)

    if not total:
        return {}

    return {
        metric: sum(
            item[metric] for item in results
        ) / total
        for metric in metric_names
    }


# ============================================================
# EXPERIMENT
# ============================================================

def run_experiment():

    print("\nDataPilot AI - MMR Document Limit Experiment")
    print("=" * 65)

    experiment_results = {
        str(limit): []
        for limit in DOCUMENT_LIMITS
    }

    for test_case in EVALUATION_DATASET:

        print(
            f"\n{test_case['id']} | "
            f"{test_case['category']}"
        )

        for limit in DOCUMENT_LIMITS:

            retriever = RAGRetriever(
                top_k=DOCUMENT_K,
                candidate_k=CANDIDATE_K,
                similarity_threshold=SIMILARITY_THRESHOLD,
                strategy="mmr",
                mmr_lambda=MMR_LAMBDA,
                max_chunks_per_document=limit,
            )

            results = retriever.retrieve(
                test_case["question"]
            )

            metrics = calculate_metrics(
                results,
                test_case["expected_sources"],
            )

            experiment_results[str(limit)].append({
                "id": test_case["id"],
                "category": test_case["category"],
                "question": test_case["question"],
                "expected_sources": test_case["expected_sources"],
                "metrics": metrics,
                "chunks": results,
            })

            print(
                f"MMR limit={limit} | "
                f"Hit={metrics['hit_at_k']} | "
                f"MRR={metrics['mrr']:.3f} | "
                f"Recall={metrics['recall_at_k']:.3f} | "
                f"Unique docs={metrics['unique_documents']} | "
                f"Duplicate rate="
                f"{metrics['duplicate_document_rate']:.3f}"
            )

    summary = {
        limit: aggregate(
            [
                item["metrics"]
                for item in results
            ]
        )
        for limit, results in experiment_results.items()
    }

    report = {
        "experiment": "MMR document limit comparison",
        "generated_at": datetime.now(
            timezone.utc
        ).isoformat(),
        "configuration": {
            "candidate_k": CANDIDATE_K,
            "document_k": DOCUMENT_K,
            "similarity_threshold": SIMILARITY_THRESHOLD,
            "mmr_lambda": MMR_LAMBDA,
            "document_limits": DOCUMENT_LIMITS,
        },
        "summary": summary,
        "results": experiment_results,
    }

    REPORT_DIRECTORY.mkdir(
        parents=True,
        exist_ok=True,
    )

    with REPORT_FILE.open("w") as file:
        json.dump(report, file, indent=2)

    print("\nEXPERIMENT SUMMARY")
    print("=" * 65)

    for limit, metrics in summary.items():
        print(f"\nMMR limit: {limit}")

        for metric, value in metrics.items():
            print(f"{metric}: {value:.4f}")

    print(f"\nReport saved: {REPORT_FILE}")


if __name__ == "__main__":
    run_experiment()
