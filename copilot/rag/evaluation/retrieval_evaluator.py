
import json
from datetime import datetime, timezone
from pathlib import Path

from copilot.rag.retrieval.retriever import RAGRetriever
from copilot.rag.evaluation.evaluation_dataset import (
    EVALUATION_DATASET,
)


# ============================================================
# CONFIGURATION
# ============================================================

THRESHOLD_VALUES = [
    0.30,
    0.40,
    0.50,
    0.60,
    0.70,
]

CANDIDATE_K = 20
DOCUMENT_K = 5

EXPERIMENT_THRESHOLD = 0.40
PRODUCTION_THRESHOLD = 0.70

MMR_LAMBDA = 0.70

REPORT_DIRECTORY = Path(
    "/app/copilot/rag/evaluation/reports"
)

REPORT_FILE = (
    REPORT_DIRECTORY / "retrieval_evaluation.json"
)


# ============================================================
# RETRIEVER INITIALIZATION
# ============================================================

# Baseline candidate retriever.
# Retrieves top 20 chunks without applying a threshold.
candidate_retriever = RAGRetriever(
    top_k=CANDIDATE_K,
    candidate_k=CANDIDATE_K,
    similarity_threshold=0.0,
    strategy="similarity",
)


# Similarity strategy.
# Retrieves top 5 chunks using similarity ranking.
similarity_retriever = RAGRetriever(
    top_k=DOCUMENT_K,
    candidate_k=CANDIDATE_K,
    similarity_threshold=EXPERIMENT_THRESHOLD,
    strategy="similarity",
)


# MMR strategy.
# Retrieves diverse chunks with document-level diversity.
mmr_retriever = RAGRetriever(
    top_k=DOCUMENT_K,
    candidate_k=CANDIDATE_K,
    similarity_threshold=EXPERIMENT_THRESHOLD,
    strategy="mmr",
    mmr_lambda=MMR_LAMBDA,
)

# Production-equivalent retriever.
# Must match the live Copilot API configuration.
production_retriever = RAGRetriever(
    top_k=DOCUMENT_K,
    candidate_k=CANDIDATE_K,
    similarity_threshold=PRODUCTION_THRESHOLD,
    strategy="similarity",
)

# Multi-Query strategy.
# Uses deterministic query expansion and Reciprocal Rank Fusion.
multi_query_retriever = RAGRetriever(
    top_k=DOCUMENT_K,
    candidate_k=CANDIDATE_K,
    similarity_threshold=EXPERIMENT_THRESHOLD,
    strategy="multi_query",
    max_chunks_per_document=1,
)

# ============================================================
# METRIC CALCULATION
# ============================================================

def calculate_metrics(
    results,
    expected_sources,
    document_k=DOCUMENT_K,
):
    """
    Evaluate retrieval at document level.

    Steps:
    1. Receive ranked chunks.
    2. Deduplicate by document_id.
    3. Preserve the original ranking.
    4. Evaluate the top K unique documents.
    """

    expected = set(expected_sources)

    # --------------------------------------------------------
    # Document-level deduplication
    # --------------------------------------------------------

    unique_results = []
    seen_documents = set()

    for result in results:

        document_id = result.get("document_id")

        if not document_id:
            continue

        if document_id in seen_documents:
            continue

        seen_documents.add(document_id)
        unique_results.append(result)

    top_documents = unique_results[:document_k]

    retrieved_documents = [
        result["document_id"]
        for result in top_documents
    ]

    relevant_documents = [
        document
        for document in retrieved_documents
        if document in expected
    ]

    # --------------------------------------------------------
    # Hit@K
    # --------------------------------------------------------

    hit_at_k = int(
        bool(relevant_documents)
    )

    # --------------------------------------------------------
    # Mean Reciprocal Rank
    # --------------------------------------------------------

    reciprocal_rank = 0.0

    for rank, document in enumerate(
        retrieved_documents,
        start=1,
    ):

        if document in expected:
            reciprocal_rank = 1.0 / rank
            break

    # --------------------------------------------------------
    # Precision@K
    # Fixed denominator
    # --------------------------------------------------------

    precision_at_k = (
        len(relevant_documents) / document_k
        if document_k > 0 else 0.0
    )

    # --------------------------------------------------------
    # Recall@K
    # --------------------------------------------------------

    recall_at_k = (
        len(relevant_documents) / len(expected)
        if expected else 0.0
    )

    # --------------------------------------------------------
    # Duplicate document rate in final result set
    # --------------------------------------------------------

    valid_document_results = [
        result
        for result in results
        if result.get("document_id")
    ]

    valid_document_count = len(
        valid_document_results
    )

    unique_document_count = len(
        set(
            result["document_id"]
            for result in valid_document_results
        )
    )

    final_duplicate_document_rate = (
        1 - unique_document_count / valid_document_count
        if valid_document_count else 0.0
    )

    # --------------------------------------------------------
    # Average similarity
    # --------------------------------------------------------

    average_similarity = (
        sum(
            result["score"]
            for result in top_documents
        ) / len(top_documents)
        if top_documents else 0.0
    )

    # --------------------------------------------------------
    # Return metrics
    # --------------------------------------------------------

    return {
        "hit_at_k": hit_at_k,
        "mrr": reciprocal_rank,
        "precision_at_k": precision_at_k,
        "recall_at_k": recall_at_k,
        "average_similarity": average_similarity,
        "final_duplicate_document_rate": (
            final_duplicate_document_rate
        ),
        "retrieved_documents": retrieved_documents,
        "expected_documents": sorted(expected),
        "relevant_documents": relevant_documents,
        "retrieved_chunks": len(results),
        "unique_documents": unique_document_count,
        "evaluated_documents": len(top_documents),
    }


# ============================================================
# METRIC AGGREGATION
# ============================================================

def aggregate_metrics(
    query_results,
    result_key,
):
    """
    Calculate average metrics across all evaluation questions.
    """

    metric_names = [
        "hit_at_k",
        "mrr",
        "precision_at_k",
        "recall_at_k",
        "average_similarity",
        "final_duplicate_document_rate",
    ]

    total = len(query_results)

    if total == 0:
        return {
            metric: 0.0
            for metric in metric_names
        }

    averages = {
        metric: (
            sum(
                item[result_key][metric]
                for item in query_results
            ) / total
        )
        for metric in metric_names
    }

    averages["questions_with_no_results"] = sum(
        1
        for item in query_results
        if item[result_key]["evaluated_documents"] == 0
    )

    averages["average_evaluated_documents"] = (
        sum(
            item[result_key]["evaluated_documents"]
            for item in query_results
        ) / total
    )

    return averages


# ============================================================
# THRESHOLD SWEEP
# ============================================================

def run_threshold_sweep(query_results):
    """
    Evaluate threshold values against the same top-20
    candidate chunks.

    Candidates are filtered first, then deduplicated
    and evaluated at document level.
    """

    sweep_results = {}

    for threshold in THRESHOLD_VALUES:

        threshold_query_results = []

        for item in query_results:

            candidates = item["candidate_chunks"]

            filtered_results = [
                result
                for result in candidates
                if result["score"] >= threshold
            ]

            metrics = calculate_metrics(
                filtered_results,
                item["expected_sources"],
                DOCUMENT_K,
            )

            threshold_query_results.append({
                "id": item["id"],
                "metrics": metrics,
            })

        metric_names = [
            "hit_at_k",
            "mrr",
            "precision_at_k",
            "recall_at_k",
            "average_similarity",
            "final_duplicate_document_rate",
        ]

        total = len(threshold_query_results)

        if total == 0:
            continue

        averages = {
            metric: (
                sum(
                    item["metrics"][metric]
                    for item in threshold_query_results
                ) / total
            )
            for metric in metric_names
        }

        averages["questions_with_no_results"] = sum(
            1
            for item in threshold_query_results
            if item["metrics"]["evaluated_documents"] == 0
        )

        averages["average_evaluated_documents"] = (
            sum(
                item["metrics"]["evaluated_documents"]
                for item in threshold_query_results
            ) / total
        )

        sweep_results[str(threshold)] = {
            "summary": averages,
            "per_question": threshold_query_results,
        }

    return sweep_results


# ============================================================
# SIMILARITY VS MMR COMPARISON
# ============================================================

def run_strategy_comparison():
    """
    Compare similarity, MMR and Multi-Query retrieval
    using the same evaluation dataset and configuration.
    """

    strategies = {
    "similarity": similarity_retriever,
    "mmr": mmr_retriever,
    "multi_query": multi_query_retriever,
    }

    comparison_results = []

    print("\nSIMILARITY VS MMR COMPARISON")
    print("=" * 70)

    for test_case in EVALUATION_DATASET:

        question = test_case["question"]
        expected_sources = test_case["expected_sources"]

        query_result = {
            "id": test_case["id"],
            "category": test_case["category"],
            "question": question,
            "expected_sources": expected_sources,
        }

        for strategy_name, retriever in strategies.items():

            results = retriever.retrieve(question)

            metrics = calculate_metrics(
                results,
                expected_sources,
                DOCUMENT_K,
            )

            query_result[strategy_name] = {
                "metrics": metrics,
                "retrieved_chunks": results,
            }

        comparison_results.append(query_result)

        similarity_metrics = (
            query_result["similarity"]["metrics"]
        )

        mmr_metrics = (
            query_result["mmr"]["metrics"]
        )
        multi_query_metrics = (
            query_result["multi_query"]["metrics"]
        )

        print(
            f"\n{test_case['id']} | "
            f"{test_case['category']}"
        )

        print(
            "Similarity:",
            f"Hit@5={similarity_metrics['hit_at_k']}",
            f"MRR={similarity_metrics['mrr']:.4f}",
            f"Unique docs={similarity_metrics['unique_documents']}",
        )

        print(
            "MMR:",
            f"Hit@5={mmr_metrics['hit_at_k']}",
            f"MRR={mmr_metrics['mrr']:.4f}",
            f"Unique docs={mmr_metrics['unique_documents']}",
        )
        print(
            "Multi-Query:",
            f"Hit@5={multi_query_metrics['hit_at_k']}",
            f"MRR={multi_query_metrics['mrr']:.4f}",
            f"Unique docs={multi_query_metrics['unique_documents']}",
        )

    summary = {}

    for strategy_name in strategies:

        summary[strategy_name] = {}

        metric_names = [
            "hit_at_k",
            "mrr",
            "precision_at_k",
            "recall_at_k",
            "average_similarity",
            "final_duplicate_document_rate",
        ]

        total = len(comparison_results)

        for metric in metric_names:

            summary[strategy_name][metric] = (
                sum(
                    item[strategy_name]["metrics"][metric]
                    for item in comparison_results
                ) / total
                if total else 0.0
            )

        summary[strategy_name][
            "questions_with_no_results"
        ] = sum(
            1
            for item in comparison_results
            if item[strategy_name]["metrics"][
                "evaluated_documents"
            ] == 0
        )

        summary[strategy_name][
            "average_evaluated_documents"
        ] = (
            sum(
                item[strategy_name]["metrics"][
                    "evaluated_documents"
                ]
                for item in comparison_results
            ) / total
            if total else 0.0
        )

    return {
        "configuration": {
            "candidate_k": CANDIDATE_K,
            "document_k": DOCUMENT_K,
            "similarity_threshold": EXPERIMENT_THRESHOLD,
            "mmr_lambda": MMR_LAMBDA,
            "strategies": [
            "similarity",
            "mmr",
            "multi_query",
        ],
        },
        "summary": summary,
        "results": comparison_results,
    }


# ============================================================
# MAIN EVALUATION
# ============================================================

def run_evaluation():

    query_results = []

    print("\nDataPilot AI - RAG Retrieval Evaluation")
    print("=" * 70)

    # --------------------------------------------------------
    # 1. Baseline and production simulation
    # --------------------------------------------------------

    for test_case in EVALUATION_DATASET:

        question = test_case["question"]
        expected_sources = test_case["expected_sources"]

        # Retrieve top 20 candidate chunks once.
        candidate_results = candidate_retriever.retrieve(
            question
        )

        # Baseline: top 5 unique documents from top 20.
        baseline = calculate_metrics(
            candidate_results,
            expected_sources,
            DOCUMENT_K,
        )

        production_candidates = production_retriever.retrieve(
            question
        )

        production = calculate_metrics(
            production_candidates,
            expected_sources,
            DOCUMENT_K,
        )

        query_result = {
            "id": test_case["id"],
            "category": test_case["category"],
            "question": question,
            "expected_sources": expected_sources,
            "candidate_chunks": candidate_results,
            "baseline": baseline,
            "production_threshold": production,
        }

        query_results.append(query_result)

        print(
            f"\n{test_case['id']} | "
            f"{test_case['category']}"
        )

        print("Question:", question)

        print(
            "Baseline Hit@5:",
            baseline["hit_at_k"],
        )

        print(
            "Baseline MRR:",
            round(baseline["mrr"], 4),
        )

        print(
            "Production Hit@5:",
            production["hit_at_k"],
        )

        print(
            "Baseline documents:",
            baseline["retrieved_documents"],
        )

        print(
            "Production documents:",
            production["retrieved_documents"],
        )

    # --------------------------------------------------------
    # 2. Aggregate baseline and production metrics
    # --------------------------------------------------------

    summary = {
        "baseline": aggregate_metrics(
            query_results,
            "baseline",
        ),
        "production_threshold": aggregate_metrics(
            query_results,
            "production_threshold",
        ),
    }

    # --------------------------------------------------------
    # 3. Threshold sweep
    # --------------------------------------------------------

    threshold_sweep = run_threshold_sweep(
        query_results
    )

    # --------------------------------------------------------
    # 4. Similarity vs MMR strategy comparison
    # --------------------------------------------------------

    strategy_comparison = run_strategy_comparison()

    # --------------------------------------------------------
    # 5. Build report
    # --------------------------------------------------------

    report = {
        "project": "DataPilot AI",
        "evaluation_timestamp": datetime.now(
            timezone.utc
        ).isoformat(),
        "total_questions": len(EVALUATION_DATASET),
        "configuration": {
            "candidate_k": CANDIDATE_K,
            "document_k": DOCUMENT_K,
            "experiment_threshold": EXPERIMENT_THRESHOLD,
            "production_threshold": PRODUCTION_THRESHOLD,
            "mmr_lambda": MMR_LAMBDA,
        },
        "production_simulation": (
            "Top 20 candidate chunks filtered at threshold 0.70, "
            "returning up to 5 chunks"
        ),
        "summary": summary,
        "results": query_results,
        "threshold_sweep": threshold_sweep,
        "strategy_comparison": strategy_comparison,
    }

    # --------------------------------------------------------
    # 6. Save JSON report
    # --------------------------------------------------------

    REPORT_DIRECTORY.mkdir(
        parents=True,
        exist_ok=True,
    )

    with REPORT_FILE.open(
        "w",
        encoding="utf-8",
    ) as file:

        json.dump(
            report,
            file,
            indent=2,
            ensure_ascii=False,
        )

    # --------------------------------------------------------
    # 7. Print threshold sweep
    # --------------------------------------------------------

    print("\nTHRESHOLD SWEEP")
    print("=" * 70)

    for threshold, result in threshold_sweep.items():

        metrics = result["summary"]

        print(f"\nThreshold: {threshold}")
        print(f"Hit@5: {metrics['hit_at_k']:.4f}")
        print(f"MRR: {metrics['mrr']:.4f}")

        print(
            f"Precision@5: "
            f"{metrics['precision_at_k']:.4f}"
        )

        print(
            f"Recall@5: "
            f"{metrics['recall_at_k']:.4f}"
        )

        print(
            "Average similarity:",
            f"{metrics['average_similarity']:.4f}",
        )

        print(
            "Questions with no results:",
            metrics["questions_with_no_results"],
        )

    # --------------------------------------------------------
    # 8. Print strategy comparison
    # --------------------------------------------------------

    print("\nSTRATEGY COMPARISON")
    print("=" * 70)

    for strategy, metrics in (
        strategy_comparison["summary"].items()
    ):

        print(f"\n{strategy.upper()}")

        for metric, value in metrics.items():

            print(
                f"{metric}: {value:.4f}"
                if isinstance(value, float)
                else f"{metric}: {value}"
            )

    # --------------------------------------------------------
    # 9. Print final summary
    # --------------------------------------------------------

    print("\nBASELINE AND PRODUCTION SUMMARY")
    print("=" * 70)

    for mode, metrics in summary.items():

        print(f"\n{mode.upper()}")

        for metric, value in metrics.items():

            print(
                f"{metric}: {value:.4f}"
                if isinstance(value, float)
                else f"{metric}: {value}"
            )

    print("\nEvaluation completed.")
    print("Total questions:", len(EVALUATION_DATASET))
    print("Report saved:", REPORT_FILE)


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":
    run_evaluation()
