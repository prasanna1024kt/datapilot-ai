
from unittest.mock import Mock

import pytest

from copilot.rag.ingestion.ingestion_pipeline import RAGIngestionPipeline


@pytest.fixture
def sample_directory(tmp_path):
    document = tmp_path / "sample_incident.md"
    document.write_text(
        "# Synthetic Incident\n"
        "Dataset: olist_order_payments\n"
        "Issue: Data quality validation failed.\n",
        encoding="utf-8",
    )
    return tmp_path


def test_dry_run_loads_and_chunks_documents(sample_directory):
    pipeline = RAGIngestionPipeline(
        chunk_size=50,
        chunk_overlap=10,
    )

    result = pipeline.ingest(str(sample_directory), dry_run=True)

    assert result["documents_loaded"] == 1
    assert result["chunks_created"] > 0
    assert result["embeddings_generated"] == 0
    assert result["vectors_upserted"] == 0
    assert result["dry_run"] is True


def test_dry_run_does_not_generate_embeddings(sample_directory):
    embedding = Mock()
    store = Mock()

    pipeline = RAGIngestionPipeline(
        embedding_service=embedding,
        vector_store=store,
    )

    pipeline.ingest(str(sample_directory), dry_run=True)

    embedding.embed_chunks.assert_not_called()


def test_dry_run_does_not_write_to_pinecone(sample_directory):
    store = Mock()

    pipeline = RAGIngestionPipeline(vector_store=store)

    pipeline.ingest(str(sample_directory), dry_run=True)

    store.upsert_chunks.assert_not_called()


def test_live_ingestion_requires_explicit_permission(sample_directory):
    pipeline = RAGIngestionPipeline()

    with pytest.raises(PermissionError, match="explicit external-operation approval"):
        pipeline.ingest(
            str(sample_directory),
            dry_run=False,
            allow_external_operations=False,
        )


def test_mocked_live_ingestion(sample_directory):
    embedding = Mock()
    embedding.embed_chunks.side_effect = lambda chunks: [
        {**chunk, "embedding": [0.1] * 1536}
        for chunk in chunks
    ]

    store = Mock()
    store.upsert_chunks.return_value = 1

    pipeline = RAGIngestionPipeline(
        embedding_service=embedding,
        vector_store=store,
    )

    result = pipeline.ingest(
        str(sample_directory),
        dry_run=False,
        allow_external_operations=True,
    )

    assert result["documents_loaded"] == 1
    assert result["chunks_created"] > 0
    assert result["embeddings_generated"] == result["chunks_created"]
    assert result["vectors_upserted"] == 1

    embedding.embed_chunks.assert_called_once()
    store.upsert_chunks.assert_called_once()


def test_empty_directory(sample_directory):
    empty_directory = sample_directory / "empty"
    empty_directory.mkdir()

    pipeline = RAGIngestionPipeline()

    result = pipeline.ingest(str(empty_directory), dry_run=True)

    assert result["documents_loaded"] == 0
    assert result["chunks_created"] == 0
    assert result["embeddings_generated"] == 0
    assert result["vectors_upserted"] == 0
