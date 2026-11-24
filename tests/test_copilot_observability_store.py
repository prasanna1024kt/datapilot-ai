
from copilot.observability.sqlite_store import (
    SQLiteObservabilityStore,
)


def test_database_initialization(tmp_path):
    db_path = tmp_path / "test_observability.db"

    store = SQLiteObservabilityStore(db_path)

    assert store.db_path.exists()


def test_record_and_retrieve_request(tmp_path):
    db_path = tmp_path / "test_observability.db"
    store = SQLiteObservabilityStore(db_path)

    metrics = {
        "request_id": "req_test001",
        "timestamp": "2026-11-23T10:00:00Z",
        "status": "success",
        "llm_model": "gpt-4.1",
        "embedding_model": "text-embedding-3-small",
        "embedding_latency_ms": 120.5,
        "pinecone_query_latency_ms": 85.2,
        "retrieval_latency_ms": 210.3,
        "llm_generation_ms": 950.4,
        "total_request_latency_ms": 1250.0,
        "prompt_tokens": 1000,
        "completion_tokens": 250,
        "total_tokens": 1250,
    }

    store.record_request(metrics)

    result = store.get_request("req_test001")

    assert result is not None
    assert result["llm_model"] == "gpt-4.1"
    assert result["total_tokens"] == 1250
    assert result["retrieval_latency_ms"] == 210.3


def test_missing_required_fields_rejected(tmp_path):
    store = SQLiteObservabilityStore(
        tmp_path / "test_observability.db"
    )

    try:
        store.record_request({"status": "success"})
        assert False, "Expected ValueError"
    except ValueError:
        pass


def test_recent_requests(tmp_path):
    store = SQLiteObservabilityStore(
        tmp_path / "test_observability.db"
    )

    store.record_request({
        "request_id": "req_test002",
        "timestamp": "2026-11-23T10:00:00Z",
        "status": "success",
    })

    results = store.get_recent_requests(limit=5)

    assert len(results) == 1
    assert results[0]["request_id"] == "req_test002"
