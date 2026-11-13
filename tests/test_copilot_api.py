from fastapi.testclient import TestClient

from copilot.api.main import app
from copilot.rag.retrieval.retriever import RAGRetriever
from copilot.rag.llm_response_generator import LLMResponseGenerator

client = TestClient(app)


def test_health_endpoint():
    response = client.get("/health")

    assert response.status_code == 200
    assert response.json()["status"] == "healthy"


def test_chat_endpoint_with_context(monkeypatch):
    def mock_retrieve(self, question):
        return [{
            "chunk_id": "chunk-001",
            "score": 0.92,
            "content": "Airflow orchestrates DataPilot pipelines.",
            "source_file": "pipeline-orchestration.md",
            "document_id": "doc-001",
            "document_type": "markdown",
        }]

    def mock_generate(self, question, retrieved_chunks):
        return {
            "answer": "Airflow orchestrates DataPilot pipelines.",
            "sources": [{
                "source_file": "pipeline-orchestration.md",
                "chunk_id": "chunk-001",
                "score": 0.92,
            }],
            "grounded": True,
        }

    monkeypatch.setattr(RAGRetriever, "retrieve", mock_retrieve)
    monkeypatch.setattr(
        LLMResponseGenerator,
        "generate_response",
        mock_generate,
    )

    response = client.post(
        "/api/v1/chat",
        json={"question": "How are pipelines orchestrated?"},
    )

    assert response.status_code == 200
    assert response.json()["grounded"] is True
    assert len(response.json()["sources"]) == 1


def test_chat_endpoint_rejects_empty_question():
    response = client.post(
        "/api/v1/chat",
        json={"question": ""},
    )

    assert response.status_code == 422


def test_chat_endpoint_handles_missing_credentials(monkeypatch):
    def mock_retrieve(self, question):
        raise RuntimeError("OpenAI client is not configured")

    monkeypatch.setattr(RAGRetriever, "retrieve", mock_retrieve)

    response = client.post(
        "/api/v1/chat",
        json={"question": "Explain the pipeline"},
    )

    assert response.status_code == 503

    error_detail = response.json()["detail"]

    assert error_detail["error"] == "SERVICE_ERROR"
    assert "not configured" in error_detail["message"]
    assert "request_id" in error_detail