
import pytest

from copilot.rag.ingestion.chunker import chunk_document


@pytest.fixture
def sample_document():
    return {
        "document_id": "test-document-001",
        "source_file": "sample_incident.md",
        "document_type": "md",
        "content": "A" * 250,
    }


def test_chunk_count(sample_document):
    chunks = chunk_document(
        sample_document,
        chunk_size=100,
        chunk_overlap=20,
    )

    assert len(chunks) == 3


def test_chunk_overlap(sample_document):
    chunks = chunk_document(
        sample_document,
        chunk_size=100,
        chunk_overlap=20,
    )

    assert chunks[0]["content"][-20:] == chunks[1]["content"][:20]


def test_unique_chunk_ids(sample_document):
    chunks = chunk_document(
        sample_document,
        chunk_size=100,
        chunk_overlap=20,
    )

    chunk_ids = [chunk["chunk_id"] for chunk in chunks]

    assert len(chunk_ids) == len(set(chunk_ids))


def test_metadata_preservation(sample_document):
    chunks = chunk_document(sample_document)

    assert chunks[0]["document_id"] == "test-document-001"
    assert chunks[0]["source_file"] == "sample_incident.md"
    assert chunks[0]["document_type"] == "md"


def test_empty_document():
    document = {
        "document_id": "empty-001",
        "source_file": "empty.md",
        "document_type": "md",
        "content": "",
    }

    assert chunk_document(document) == []


def test_invalid_chunk_size(sample_document):
    with pytest.raises(ValueError):
        chunk_document(sample_document, chunk_size=0)


def test_invalid_overlap(sample_document):
    with pytest.raises(ValueError):
        chunk_document(
            sample_document,
            chunk_size=100,
            chunk_overlap=100,
        )
