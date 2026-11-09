from types import SimpleNamespace

import pytest

from copilot.rag.llm_response_generator import LLMResponseGenerator


class MockOpenAIClient:
    def __init__(self, answer="Mock grounded answer"):
        self.answer = answer
        self.chat = SimpleNamespace(
            completions=SimpleNamespace(create=self.create)
        )
        self.last_request = None

    def create(self, **kwargs):
        self.last_request = kwargs

        return SimpleNamespace(
            choices=[
                SimpleNamespace(
                    message=SimpleNamespace(content=self.answer)
                )
            ]
        )


def test_generate_grounded_response():
    client = MockOpenAIClient()

    generator = LLMResponseGenerator(client=client)

    chunks = [
        {
            "chunk_id": "chunk-001",
            "score": 0.91,
            "content": "Airflow orchestrates DataPilot pipelines.",
            "source_file": "pipeline-orchestration.md",
        }
    ]

    result = generator.generate_response(
        "How does Airflow orchestrate pipelines?",
        chunks,
    )

    assert result["answer"] == "Mock grounded answer"
    assert result["grounded"] is True
    assert len(result["sources"]) == 1
    assert result["sources"][0]["source_file"] == "pipeline-orchestration.md"

    assert client.last_request["model"] == generator.settings.openai_chat_model
    assert client.last_request["temperature"] == 0.2


def test_empty_retrieval_returns_no_context_answer():
    generator = LLMResponseGenerator(client=None)

    result = generator.generate_response(
        "Explain the pipeline",
        [],
    )

    assert result["grounded"] is False
    assert result["sources"] == []
    assert "could not find sufficient information" in result["answer"]


def test_empty_question_raises_error():
    generator = LLMResponseGenerator(client=MockOpenAIClient())

    with pytest.raises(ValueError, match="Question cannot be empty"):
        generator.generate_response(" ", [])


def test_empty_chunk_content_does_not_call_llm():
    client = MockOpenAIClient()
    generator = LLMResponseGenerator(client=client)

    result = generator.generate_response(
        "Explain the pipeline",
        [{"content": "   ", "source_file": "empty.md"}],
    )

    assert result["grounded"] is False
    assert result["sources"] == []
    assert client.last_request is None


def test_missing_openai_client_raises_error(monkeypatch):
    from types import SimpleNamespace
    import copilot.rag.llm_response_generator as module

    monkeypatch.setattr(
        module,
        "get_settings",
        lambda: SimpleNamespace(
            openai_api_key="",
            openai_chat_model="gpt-4.1",
        ),
    )

    generator = LLMResponseGenerator(client=None)

    with pytest.raises(
        RuntimeError,
        match="OpenAI client is not configured",
    ):
        generator.generate_response(
            "Explain the pipeline",
            [{
                "content": "Pipeline context",
                "source_file": "pipeline.md",
            }],
        )