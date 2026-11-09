
import asyncio

from openai import AsyncOpenAI

from ragas.llms import llm_factory
from ragas.embeddings import OpenAIEmbeddings

from ragas.metrics.collections import (
    Faithfulness,
    AnswerRelevancy,
    ContextPrecision,
    ContextRecall,
)

from copilot.config.settings import get_settings


async def main():

    # --------------------------------------------------------
    # 1. Load configuration
    # --------------------------------------------------------

    settings = get_settings()

    if not settings.openai_api_key:
        raise ValueError(
            "OPENAI_API_KEY is not configured."
        )

    client = AsyncOpenAI(
        api_key=settings.openai_api_key,
    )

    # --------------------------------------------------------
    # 2. Initialize RAGAS LLM
    # --------------------------------------------------------

    llm = llm_factory(
        "gpt-4o-mini",
        client=client,
    )

    # --------------------------------------------------------
    # 3. Initialize RAGAS embeddings
    # --------------------------------------------------------

    embeddings = OpenAIEmbeddings(
        client=client,
        model=settings.openai_embedding_model,
    )

    # --------------------------------------------------------
    # 4. Initialize evaluation metrics
    # --------------------------------------------------------

    metrics = {
        "faithfulness": Faithfulness(
            llm=llm,
        ),

        "answer_relevancy": AnswerRelevancy(
            llm=llm,
            embeddings=embeddings,
        ),

        "context_precision": ContextPrecision(
            llm=llm,
        ),

        "context_recall": ContextRecall(
            llm=llm,
        ),
    }

    # --------------------------------------------------------
    # 5. Evaluation sample
    # --------------------------------------------------------

    question = (
        "What is the purpose of DataPilot AI?"
    )

    answer = (
        "DataPilot AI is an AI-powered data engineering "
        "copilot that helps automate data engineering "
        "and DataOps workflows."
    )

    reference = (
        "DataPilot AI is an AI data engineering and DataOps "
        "copilot designed to demonstrate RAG, MCP, "
        "multi-agent orchestration and automation."
    )

    contexts = [
        (
            "DataPilot AI is an AI data engineering and "
            "DataOps copilot. It demonstrates RAG, MCP, "
            "multi-agent orchestration and automated "
            "data engineering workflows."
        ),
        (
            "The project uses Pinecone as its managed "
            "vector database for knowledge retrieval."
        ),
    ]

    # --------------------------------------------------------
    # 6. Metric-specific inputs
    # --------------------------------------------------------

    metric_inputs_by_name = {

        "faithfulness": {
            "user_input": question,
            "response": answer,
            "retrieved_contexts": contexts,
        },

        "answer_relevancy": {
            "user_input": question,
            "response": answer,
        },

        "context_precision": {
            "user_input": question,
            "reference": reference,
            "retrieved_contexts": contexts,
        },

        "context_recall": {
            "user_input": question,
            "reference": reference,
            "retrieved_contexts": contexts,
        },
    }

    # --------------------------------------------------------
    # 7. Execute RAGAS evaluation
    # --------------------------------------------------------

    print("\nDATAPILOT AI - RAGAS SMOKE TEST")
    print("=" * 60)

    results = {}

    for name, metric in metrics.items():

        print(f"\nEvaluating: {name}")

        result = await metric.ascore(
            **metric_inputs_by_name[name]
        )

        score = (
            float(result.value)
            if result.value is not None
            else None
        )

        results[name] = {
            "score": score,
            "reason": getattr(result, "reason", None),
        }

        print(f"Score: {score}")
        print(f"Reason: {results[name]['reason']}")

    # --------------------------------------------------------
    # 8. Final summary
    # --------------------------------------------------------

    print("\nRAGAS EVALUATION SUMMARY")
    print("=" * 60)

    for name, result in results.items():

        print(
            f"{name}: {result['score']}"
        )

    print("\nRAGAS smoke test completed successfully.")


if __name__ == "__main__":

    asyncio.run(main())
