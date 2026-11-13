
import asyncio
import json
import logging
from datetime import datetime, timezone
from pathlib import Path

from openai import AsyncOpenAI
from copilot.rag.llm_response_generator import LLMResponseGenerator
from ragas.llms import llm_factory
from ragas.embeddings import embedding_factory
from ragas.metrics.collections import (
    Faithfulness,
    AnswerRelevancy,
    ContextPrecision,
    ContextRecall,
)

from copilot.config.settings import get_settings
from copilot.rag.retrieval.retriever import RAGRetriever
from copilot.rag.evaluation.evaluation_dataset import (
    EVALUATION_DATASET,
)
from copilot.rag.evaluation.validate_dataset import (
    validate_dataset,
)

# ============================================================
# CONFIGURATION
# ============================================================

CANDIDATE_K = 20
DOCUMENT_K = 5

SIMILARITY_THRESHOLD = 0.40
MMR_LAMBDA = 0.70 

#GENERATION_MODEL = "gpt-4.1"
EVALUATION_MODEL = "gpt-4o-mini"

EVALUATION_NAMESPACE = "datapilot-ai-v3-1-evaluation"

REPORT_DIRECTORY = Path(
    "/app/results/rag-evaluation"
)

REPORT_FILE = (
    REPORT_DIRECTORY / "ragas_evaluation.json"
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger(__name__)

settings = get_settings()


# ============================================================
# CLIENTS AND METRICS
# ============================================================

client = AsyncOpenAI(
    api_key=settings.openai_api_key,
)
response_generator = LLMResponseGenerator()


# Standard evaluator LLM for other metrics
evaluator_llm = llm_factory(
    EVALUATION_MODEL,
    client=client,
)

# Dedicated Faithfulness evaluator with higher output allowance
faithfulness_llm = llm_factory(
    EVALUATION_MODEL,
    client=client,
    max_tokens=8192,
)

evaluator_embeddings = embedding_factory(
    "openai",
    model=settings.openai_embedding_model,
    client=client,
)

faithfulness_metric = Faithfulness(
    llm=faithfulness_llm,
)

answer_relevancy_metric = AnswerRelevancy(
    llm=evaluator_llm,
    embeddings=evaluator_embeddings,
)

context_precision_metric = ContextPrecision(
    llm=evaluator_llm,
)

context_recall_metric = ContextRecall(
    llm=evaluator_llm,
)

METRICS = {
    "faithfulness": faithfulness_metric,
    "answer_relevancy": answer_relevancy_metric,
    "context_precision": context_precision_metric,
    "context_recall": context_recall_metric,
}

# ============================================================
# RETRIEVERS
# ============================================================

similarity_retriever = RAGRetriever(
    top_k=DOCUMENT_K,
    candidate_k=CANDIDATE_K,
    similarity_threshold=SIMILARITY_THRESHOLD,
    strategy="similarity",
    namespace=EVALUATION_NAMESPACE,
)

mmr_retriever = RAGRetriever(
    top_k=DOCUMENT_K,
    candidate_k=CANDIDATE_K,
    similarity_threshold=SIMILARITY_THRESHOLD,
    strategy="mmr",
    mmr_lambda=MMR_LAMBDA,
    max_chunks_per_document=1,
    namespace=EVALUATION_NAMESPACE,
)

experimental_mmr_retriever = RAGRetriever(
    top_k=8,
    candidate_k=CANDIDATE_K,
    similarity_threshold=SIMILARITY_THRESHOLD,
    strategy="mmr",
    mmr_lambda=MMR_LAMBDA,
    max_chunks_per_document=2,
    namespace=EVALUATION_NAMESPACE,
)

RETRIEVERS = {
    "similarity": similarity_retriever,
    "mmr": mmr_retriever,
    "experimental_mmr": experimental_mmr_retriever,
}

# ============================================================
# ANSWER GENERATION
# ============================================================

# ============================================================
# PRODUCTION ANSWER GENERATION
# ============================================================

async def generate_answer(question: str, retrieved_chunks: list[dict]) -> dict:
    """
    Generate answers using the actual production
    LLMResponseGenerator and rag_system_prompt.txt.

    Run the synchronous production generator in a worker
    thread to avoid blocking the async evaluation loop.
    """

    response = await asyncio.to_thread(
        response_generator.generate_response,
        question,
        retrieved_chunks,
    )

    if not response.get("answer"):
        raise RuntimeError(
            "Production LLMResponseGenerator returned an empty answer."
        )

    return response


# ============================================================
# RAGAS METRIC EVALUATION
# ============================================================

# ============================================================
# RAGAS METRIC EVALUATION
# ============================================================

async def evaluate_answer(
    question: str,
    answer: str,
    reference_answer: str,
    retrieved_contexts: list[str],
) -> dict:
    """
    Evaluate one generated answer using metric-specific
    RAGAS inputs.
    """

    metric_inputs = {
        "faithfulness": {
    "user_input": question,
    "response": answer[:6000],
    "retrieved_contexts": [
        context[:4000]
        for context in retrieved_contexts[:3]
    ],
 },
        "answer_relevancy": {
            "user_input": question,
            "response": answer,
        },

        "context_precision": {
            "user_input": question,
            "reference": reference_answer,
            "retrieved_contexts": retrieved_contexts,
        },

        "context_recall": {
            "user_input": question,
            "reference": reference_answer,
            "retrieved_contexts": retrieved_contexts,
        },
    }

    scores = {}

    for metric_name, metric in METRICS.items():

        try:

            result = await metric.ascore(
                **metric_inputs[metric_name]
            )

            scores[metric_name] = (
                float(result.value)
                if result.value is not None
                else None
            )

        except Exception as exc:

            logger.exception(
                "RAGAS metric failed: %s",
                metric_name,
            )

            # Capture structured-output failure details
            last_completion = getattr(
                exc,
                "last_completion",
                None,
            )

            if last_completion:
                for choice in last_completion.choices:
                    logger.error(
                        "Metric: %s | Finish reason: %s",
                        metric_name,
                        choice.finish_reason,
                    )

                    logger.error(
                        "LLM output: %s",
                        choice.message.content,
                    )

                    logger.error(
                        "LLM usage: %s",
                        last_completion.usage,
                    )

            scores[metric_name] = None
            scores[f"{metric_name}_error"] = str(exc)
        
    return scores
# ============================================================
# EVALUATE ONE QUESTION AND STRATEGY
# ============================================================

async def evaluate_strategy(
    test_case: dict,
    strategy_name: str,
    retriever: RAGRetriever,
) -> dict:

    question = test_case["question"]
    reference_answer = test_case["reference_answer"]

    logger.info(
        "Evaluating %s using %s",
        test_case["id"],
        strategy_name,
    )

    # --------------------------------------------------------
    # 1. Retrieve documents
    # --------------------------------------------------------

    retrieved_results = retriever.retrieve(
        question
    )

    retrieved_contexts = [
        item["content"]
        for item in retrieved_results
        if item.get("content")
    ]

    # --------------------------------------------------------
    # 2. Generate answer
    # --------------------------------------------------------

    generation_result = await generate_answer(
    question,
    retrieved_results,
)
    answer = generation_result["answer"]

    # --------------------------------------------------------
    # 3. Evaluate answer using RAGAS
    # --------------------------------------------------------

    scores = await evaluate_answer(
        question=question,
        answer=answer,
        reference_answer=reference_answer,
        retrieved_contexts=retrieved_contexts,
    )

    return {
        "strategy": strategy_name,
        "question_id": test_case["id"],
        "category": test_case["category"],
        "question": question,
        "reference_answer": reference_answer,
        "generated_answer": answer,
        "generation_model": generation_result.get("model"),
        "generation_usage": generation_result.get("usage"),
        "grounded": generation_result.get("grounded"),
        "retrieved_documents": [
            item.get("document_id")
            for item in retrieved_results
        ],
        "retrieved_contexts": retrieved_contexts,
        "retrieved_chunks": len(retrieved_contexts),
        "scores": scores,
    }


# ============================================================
# AGGREGATE METRICS
# ============================================================

def aggregate_scores(results: list[dict]) -> dict:

    metric_names = list(METRICS.keys())

    summary = {}

    for metric_name in metric_names:

        values = [
            item["scores"].get(metric_name)
            for item in results
            if item["scores"].get(metric_name) is not None
        ]

        summary[metric_name] = (
            sum(values) / len(values)
            if values else None
        )

        summary[f"{metric_name}_evaluated"] = len(values)

    return summary


# ============================================================
# MAIN EVALUATION
# ============================================================

async def run_evaluation():

    validate_dataset()

    logger.info(
        "Starting DataPilot AI RAGAS evaluation"
    )

    all_results = []

    strategy_results = {
        strategy_name: []
        for strategy_name in RETRIEVERS
    }

    # --------------------------------------------------------
    # Run both retrieval strategies
    # --------------------------------------------------------

    for test_case in EVALUATION_DATASET:

        for strategy_name, retriever in RETRIEVERS.items():

            result = await evaluate_strategy(
                test_case=test_case,
                strategy_name=strategy_name,
                retriever=retriever,
            )

            all_results.append(result)

            strategy_results[strategy_name].append(
                result
            )

            print(
                f"\n{test_case['id']} | "
                f"{strategy_name.upper()}"
            )

            for metric_name, value in (
                result["scores"].items()
            ):

                if metric_name.endswith("_error"):
                    continue

                print(
                    f"{metric_name}: "
                    f"{value if value is not None else 'FAILED'}"
                )

    # --------------------------------------------------------
    # Aggregate strategy results
    # --------------------------------------------------------

    summary = {}

    for strategy_name, results in (
        strategy_results.items()
    ):

        summary[strategy_name] = aggregate_scores(
            results
        )
    
    # --------------------------------------------------------
    # Determine evaluation status
    # --------------------------------------------------------

    total_metric_attempts = (
        len(all_results) * len(METRICS)
    )

    successful_metric_attempts = sum(
        1
        for result in all_results
        for metric_name in METRICS
        if result["scores"].get(metric_name) is not None
    )

    failed_metric_attempts = (
        total_metric_attempts - successful_metric_attempts
    )

    evaluation_status = (
        "SUCCESS"
        if failed_metric_attempts == 0
        else (
            "PARTIAL_SUCCESS"
            if successful_metric_attempts > 0
            else "FAILED"
        )
    )

    # --------------------------------------------------------
    # Build report
    # --------------------------------------------------------

    report = {
    "project": "DataPilot AI",
    "evaluation_type": "RAGAS answer evaluation",
    "evaluation_namespace": EVALUATION_NAMESPACE,
    "evaluation_timestamp": datetime.now(
        timezone.utc
    ).isoformat(),

    "evaluation_status": evaluation_status,
    "configuration": {
        "total_questions": len(EVALUATION_DATASET),
        "candidate_k": CANDIDATE_K,
        "document_k": DOCUMENT_K,
        "similarity_threshold": SIMILARITY_THRESHOLD,
        "mmr_lambda": MMR_LAMBDA,
        "generation_model": settings.openai_chat_model,
        "generation_prompt": "copilot/prompts/rag_system_prompt.txt",
        "generation_temperature": 0.2,
        "generation_mode": "production_llm_response_generator",
        "evaluation_model": EVALUATION_MODEL,
        "strategies": list(RETRIEVERS.keys()),
        "metrics": list(METRICS.keys()),
    },

    "execution_summary": {
        "total_metric_attempts": total_metric_attempts,
        "successful_metric_attempts": successful_metric_attempts,
        "failed_metric_attempts": failed_metric_attempts,
    },

    "summary": summary,
    "results": all_results,
}

    # --------------------------------------------------------
    # Save report
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
    # Print summary
    # --------------------------------------------------------

    print("\nRAGAS EVALUATION SUMMARY")
    print("=" * 70)

    for strategy_name, metrics in summary.items():

        print(f"\n{strategy_name.upper()}")

        for metric_name, value in metrics.items():

            print(
                f"{metric_name}: "
                f"{value:.4f}"
                if isinstance(value, (int, float))
                else f"{metric_name}: {value}"
            )

    print("\nEvaluation status:", evaluation_status)
    print("Successful metric attempts:", successful_metric_attempts)
    print("Failed metric attempts:", failed_metric_attempts)
    print("Questions:", len(EVALUATION_DATASET))
    print("Strategies:", len(RETRIEVERS))
    print("Total evaluations:", len(all_results))
    print("Report:", REPORT_FILE)
    if evaluation_status == "FAILED":
        raise RuntimeError(
            "RAGAS evaluation failed: "
            "all metric attempts failed. "
            f"Check report: {REPORT_FILE}"
        )
# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":

    asyncio.run(
        run_evaluation()
    )
