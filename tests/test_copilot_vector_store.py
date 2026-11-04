
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from copilot.rag.vector_store import PineconeVectorStore


@pytest.fixture
def mock_pinecone():
    client = Mock()
    index = Mock()

    client.Index.return_value = index
    client.describe_index.return_value = SimpleNamespace(
        dimension=1536
    )

    index.query.return_value = SimpleNamespace(
        matches=[
            SimpleNamespace(
                id="chunk-001",
                score=0.95,
                metadata={
                    "source_file": "incident.md",
                    "content": "Payment pipeline failed",
                },
            )
        ]
    )

    return client, index


def test_index_dimension_validation(mock_pinecone):
    client, _ = mock_pinecone
    store = PineconeVectorStore(client=client)

    assert store.verify_index_dimension() is True


def test_index_dimension_mismatch(mock_pinecone):
    client, _ = mock_pinecone
    client.describe_index.return_value.dimension = 768

    store = PineconeVectorStore(client=client)

    with pytest.raises(ValueError, match="dimension mismatch"):
        store.verify_index_dimension()


def test_upsert_chunks(mock_pinecone):
    client, index = mock_pinecone
    store = PineconeVectorStore(client=client)

    chunks = [{
        "chunk_id": "chunk-001",
        "document_id": "doc-001",
        "source_file": "incident.md",
        "document_type": "md",
        "chunk_index": 0,
        "content": "Payment pipeline failed",
        "embedding": [0.1] * 1536,
    }]

    result = store.upsert_chunks(chunks)

    assert result == 1
    index.upsert.assert_called_once()

    payload = index.upsert.call_args.kwargs["vectors"]
    assert payload[0]["id"] == "chunk-001"
    assert payload[0]["metadata"]["source_file"] == "incident.md"


def test_empty_upsert_does_not_call_pinecone(mock_pinecone):
    client, index = mock_pinecone
    store = PineconeVectorStore(client=client)

    assert store.upsert_chunks([]) is None
    client.Index.assert_not_called()
    index.upsert.assert_not_called()


def test_similarity_search(mock_pinecone):
    client, index = mock_pinecone
    store = PineconeVectorStore(client=client)

    results = store.search([0.1] * 1536, top_k=5)

    assert len(results) == 1
    assert results[0]["chunk_id"] == "chunk-001"
    assert results[0]["score"] == 0.95
    index.query.assert_called_once()


def test_invalid_query_dimension(mock_pinecone):
    client, index = mock_pinecone
    store = PineconeVectorStore(client=client)

    with pytest.raises(ValueError, match="dimension mismatch"):
        store.search([0.1] * 768)

    index.query.assert_not_called()


def test_empty_query_rejected(mock_pinecone):
    client, index = mock_pinecone
    store = PineconeVectorStore(client=client)

    with pytest.raises(ValueError, match="cannot be empty"):
        store.search([])

    index.query.assert_not_called()


def test_missing_client_rejected(monkeypatch):
    from copilot.rag import vector_store

    class FakeSettings:
        pinecone_api_key = ""
        pinecone_index_name = "datapilot-ai-knowledge"
        pinecone_embedding_dimension = 1536

    monkeypatch.setattr(
        vector_store,
        "get_settings",
        lambda: FakeSettings(),
    )

    store = PineconeVectorStore()

    with pytest.raises(RuntimeError, match="not configured"):
        store.search([0.1] * 1536)
