
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from copilot.rag.ingestion.embedding_service import EmbeddingService


@pytest.fixture
def mock_client():
    client = Mock()
    client.embeddings.create.return_value = SimpleNamespace(
        data=[
            SimpleNamespace(embedding=[0.1] * 1536),
            SimpleNamespace(embedding=[0.2] * 1536),
        ]
    )
    return client


def test_embed_multiple_texts(mock_client):
    service = EmbeddingService(client=mock_client)

    result = service.embed_texts(["pipeline failure", "DQ validation"])

    assert len(result) == 2
    assert len(result[0]) == 1536
    assert len(result[1]) == 1536

    mock_client.embeddings.create.assert_called_once()


def test_empty_input_does_not_call_api(mock_client):
    service = EmbeddingService(client=mock_client)

    assert service.embed_texts([]) == []

    mock_client.embeddings.create.assert_not_called()


def test_invalid_text_rejected(mock_client):
    service = EmbeddingService(client=mock_client)

    with pytest.raises(ValueError):
        service.embed_texts(["valid text", ""])

    mock_client.embeddings.create.assert_not_called()


def test_dimension_mismatch_detected(mock_client):
    mock_client.embeddings.create.return_value = SimpleNamespace(
        data=[
            SimpleNamespace(embedding=[0.1] * 768),
        ]
    )

    service = EmbeddingService(client=mock_client)

    with pytest.raises(ValueError, match="Expected 1536 dimensions"):
        service.embed_texts(["pipeline failure"])


def test_embed_chunks_preserves_metadata(mock_client):
    service = EmbeddingService(client=mock_client)

    chunks = [
        {
            "chunk_id": "chunk-001",
            "document_id": "doc-001",
            "source_file": "incident.md",
            "content": "Payment pipeline failed",
        },
        {
            "chunk_id": "chunk-002",
            "document_id": "doc-001",
            "source_file": "incident.md",
            "content": "DQ validation failed",
        },
    ]

    result = service.embed_chunks(chunks)

    assert len(result) == 2
    assert result[0]["chunk_id"] == "chunk-001"
    assert result[0]["source_file"] == "incident.md"
    assert len(result[0]["embedding"]) == 1536


def test_missing_client_rejected(monkeypatch):
    from copilot.rag.ingestion import embedding_service

    class FakeSettings:
        openai_api_key = ""
        openai_embedding_model = "text-embedding-3-small"
        pinecone_embedding_dimension = 1536

    monkeypatch.setattr(
        embedding_service,
        "get_settings",
        lambda: FakeSettings(),
    )

    service = EmbeddingService()

    with pytest.raises(RuntimeError, match="OpenAI client is not configured"):
        service.embed_texts(["pipeline failure"])
