
from unittest.mock import Mock

import pytest

from copilot.rag.retrieval.retriever import RAGRetriever


@pytest.fixture
def mock_dependencies():
    embedding_service = Mock()
    embedding_service.embed_texts.return_value = [[0.1] * 1536]

    vector_store = Mock()
    vector_store.search.return_value = [
        {
            "chunk_id": "chunk-001",
            "score": 0.95,
            "metadata": {
                "content": "Payment pipeline failed due to null values.",
                "source_file": "incident.md",
                "document_id": "doc-001",
                "document_type": "md",
            },
        },
        {
            "chunk_id": "chunk-002",
            "score": 0.55,
            "metadata": {
                "content": "Unrelated documentation.",
                "source_file": "architecture.md",
            },
        },
    ]

    return embedding_service, vector_store


def test_successful_retrieval(mock_dependencies):
    embedding, store = mock_dependencies
    retriever = RAGRetriever(embedding, store)

    results = retriever.retrieve("Why did payments fail?")

    assert len(results) == 1
    assert results[0]["chunk_id"] == "chunk-001"
    assert results[0]["source_file"] == "incident.md"
    embedding.embed_texts.assert_called_once()
    store.search.assert_called_once()


def test_similarity_threshold_filtering(mock_dependencies):
    embedding, store = mock_dependencies
    retriever = RAGRetriever(
        embedding,
        store,
        similarity_threshold=0.90,
    )

    results = retriever.retrieve("Payment failure")

    assert len(results) == 1
    assert results[0]["score"] == 0.95


def test_top_k_configuration(mock_dependencies):
    embedding, store = mock_dependencies
    retriever = RAGRetriever(
        embedding,
        store,
        top_k=3,
    )

    retriever.retrieve("Payment failure")

    assert store.search.call_args.kwargs["top_k"] == 3


def test_empty_question_rejected(mock_dependencies):
    embedding, store = mock_dependencies
    retriever = RAGRetriever(embedding, store)

    with pytest.raises(ValueError, match="cannot be empty"):
        retriever.retrieve(" ")

    embedding.embed_texts.assert_not_called()
    store.search.assert_not_called()


def test_missing_metadata_handling(mock_dependencies):
    embedding, store = mock_dependencies
    store.search.return_value = [
        {
            "chunk_id": "chunk-003",
            "score": 0.95,
            "metadata": {},
        }
    ]

    retriever = RAGRetriever(embedding, store)
    results = retriever.retrieve("Payment failure")

    assert results[0]["source_file"] == "unknown"
    assert results[0]["content"] == ""


def test_empty_search_results(mock_dependencies):
    embedding, store = mock_dependencies
    store.search.return_value = []

    retriever = RAGRetriever(embedding, store)

    assert retriever.retrieve("Payment failure") == []


@pytest.mark.parametrize("top_k", [0, -1])
def test_invalid_top_k_rejected(top_k, mock_dependencies):
    embedding, store = mock_dependencies

    with pytest.raises(ValueError, match="top_k"):
        RAGRetriever(embedding, store, top_k=top_k)


def test_invalid_similarity_threshold(mock_dependencies):
    embedding, store = mock_dependencies

    with pytest.raises(ValueError, match="similarity_threshold"):
        RAGRetriever(
            embedding,
            store,
            similarity_threshold=1.5,
        )
