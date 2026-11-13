
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
        chunk_size=150,
        chunk_overlap=20,
    )
    assert len(chunks) >= 1


def test_unique_chunk_ids(sample_document):
    chunks = chunk_document(
        sample_document,
        chunk_size=150,
        chunk_overlap=20,
    )
    ids = [chunk["chunk_id"] for chunk in chunks]
    assert len(ids) == len(set(ids))


def test_metadata_preservation(sample_document):
    chunks = chunk_document(sample_document)
    assert chunks[0]["document_id"] == "test-document-001"
    assert chunks[0]["source_file"] == "sample_incident.md"
    assert chunks[0]["document_type"] == "md"
    assert chunks[0]["chunking_version"] == "v3.1"


def test_markdown_heading_preserved():
    document = {
        "document_id": "architecture",
        "source_file": "architecture.md",
        "document_type": "md",
        "content": (
            "# Architecture\n"
            "Platform overview.\n\n"
            "## Storage\n"
            "Amazon S3 stores raw and processed data.\n\n"
            "## Processing\n"
            "Spark processes the datasets."
        ),
    }

    chunks = chunk_document(document, chunk_size=500)

    assert any(
        chunk["section_heading"] == "Storage"
        and "Amazon S3" in chunk["content"]
        for chunk in chunks
    )

    assert any(
        chunk["section_heading"] == "Processing"
        and "Spark" in chunk["content"]
        for chunk in chunks
    )


def test_markdown_heading_hierarchy():
    document = {
        "document_id": "architecture",
        "source_file": "architecture.md",
        "document_type": "md",
        "content": (
            "# Architecture\n"
            "Overview.\n"
            "## Agents\n"
            "Agent overview.\n"
            "### Supervisor\n"
            "Supervisor responsibilities."
        ),
    }

    chunks = chunk_document(document)

    supervisor = next(
        chunk for chunk in chunks
        if chunk["section_heading"] == "Supervisor"
    )

    assert supervisor["parent_heading"] == "Agents"
    assert supervisor["heading_level"] == 3


def test_markdown_table_preserved():
    document = {
        "document_id": "table-doc",
        "source_file": "table.md",
        "document_type": "md",
        "content": (
            "# Configuration\n\n"
            "| Layer | Format |\n"
            "|---|---|\n"
            "| Bronze | Parquet |\n"
            "| Silver | Parquet |"
        ),
    }

    chunks = chunk_document(document, chunk_size=500)

    table_chunk = next(
        chunk for chunk in chunks if "Bronze" in chunk["content"]
    )

    assert "| Layer | Format |" in table_chunk["content"]
    assert "| Silver | Parquet |" in table_chunk["content"]


def test_yaml_top_level_sections():
    document = {
        "document_id": "config",
        "source_file": "variables.yaml",
        "document_type": "yaml",
        "content": (
            "project:\n"
            "  name: datapilot-ai\n"
            "  environment: dev\n"
            "aws:\n"
            "  region: us-east-1\n"
        ),
    }

    chunks = chunk_document(document)

    assert any(
        chunk["config_path"] == "project"
        and "datapilot-ai" in chunk["content"]
        for chunk in chunks
    )

    assert any(
        chunk["config_path"] == "aws"
        and "us-east-1" in chunk["content"]
        for chunk in chunks
    )


def test_json_top_level_sections():
    document = {
        "document_id": "config",
        "source_file": "config.json",
        "document_type": "json",
        "content": (
            '{\n'
            '  "project": {\n'
            '    "name": "datapilot-ai"\n'
            '  },\n'
            '  "aws": {\n'
            '    "region": "us-east-1"\n'
            '  }\n'
            '}'
        ),
    }

    chunks = chunk_document(document)

    assert any(
        chunk["config_path"] == "project"
        and "datapilot-ai" in chunk["content"]
        for chunk in chunks
    )

    assert any(
        chunk["config_path"] == "aws"
        and "us-east-1" in chunk["content"]
        for chunk in chunks
    )


def test_empty_document():
    document = {
        "document_id": "empty",
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
            chunk_size=150,
            chunk_overlap=150,
        )


def test_non_string_content():
    document = {
        "document_id": "invalid",
        "source_file": "invalid.md",
        "document_type": "md",
        "content": None,
    }

    with pytest.raises(TypeError):
        chunk_document(document)


def test_heading_only_section_is_not_a_separate_chunk():
    document = {
        "document_id": "heading-test",
        "source_file": "config.md",
        "document_type": "md",
        "content": (
            "# Configuration\n"
            "## variables.yaml\n"
            "### Project metadata\n"
            "Project name is datapilot-ai.\n"
        ),
    }

    chunks = chunk_document(document)

    assert not any(
        chunk["content"].strip() in {
            "## variables.yaml",
            "### Project metadata",
        }
        for chunk in chunks
    )

    assert any(
        "Project name is datapilot-ai" in chunk["content"]
        for chunk in chunks
    )


def test_table_rows_are_not_split():
    rows = [
        "| Dataset | Target | Business key |",
        "|---|---|---|",
        "| customers | customers | customer_id |",
        "| orders | orders | order_id |",
        "| payments | payments | order_id,payment_sequential |",
    ]

    document = {
        "document_id": "mapping-test",
        "source_file": "gold_tables.md",
        "document_type": "md",
        "content": "# Gold mappings\n\n" + "\n".join(rows),
    }

    chunks = chunk_document(
        document,
        chunk_size=150,
        chunk_overlap=20,
    )

    table_chunks = [
        chunk for chunk in chunks
        if "| Dataset | Target | Business key |" in chunk["content"]
    ]

    assert table_chunks, "Table header must be present"

    combined_content = "\n".join(
        chunk["content"] for chunk in chunks
    )

    # Verify every original table row is preserved.
    for row in rows:
        assert row in combined_content, f"Missing table row: {row}"

    # Every chunk containing table data must include the header
    # and separator.
    for chunk in chunks:
        if any(row in chunk["content"] for row in rows[2:]):
            assert rows[0] in chunk["content"]
            assert rows[1] in chunk["content"]

    # No table row should be partially split.
    for row in rows:
        assert sum(row in chunk["content"] for chunk in chunks) >= 1


def test_chunk_size_limit_with_overlap():
    document = {
        "document_id": "size-test",
        "source_file": "large.md",
        "document_type": "md",
        "content": "# Details\n" + ("A" * 2500),
    }

    chunks = chunk_document(
        document,
        chunk_size=500,
        chunk_overlap=100,
    )

    assert all(len(chunk["content"]) <= 500 for chunk in chunks)


def test_chunk_metadata_has_version_and_heading():
    document = {
        "document_id": "metadata-test",
        "source_file": "architecture.md",
        "document_type": "md",
        "content": "# Architecture\nSystem overview.",
    }

    chunks = chunk_document(document)

    assert chunks
    assert all(chunk["chunking_version"] == "v3.1" for chunk in chunks)
    assert all("section_heading" in chunk for chunk in chunks)
    assert all("parent_heading" in chunk for chunk in chunks)


def test_v31_includes_parent_and_subsection_context():
    document = {
        "document_id": "v31-heading-context",
        "source_file": "architecture.md",
        "document_type": "md",
        "content": (
            "# DataPilot AI\n\n"
            "## Deployment\n\n"
            "### Airflow\n\n"
            "Airflow orchestrates the pipeline."
        ),
    }

    chunks = chunk_document(document)

    airflow = next(
        chunk for chunk in chunks
        if "Airflow orchestrates" in chunk["content"]
    )

    assert "DataPilot AI" in airflow["content"]
    assert "Deployment" in airflow["content"]
    assert "Airflow" in airflow["content"]
    assert airflow["heading_path"] == [
        "DataPilot AI", "Deployment", "Airflow"
    ]
    assert airflow["chunking_version"] == "v3.1"


def test_v31_heading_only_parent_is_carried_into_child():
    document = {
        "document_id": "v31-parent-heading",
        "source_file": "deployment.md",
        "document_type": "md",
        "content": (
            "## Docker Compose services\n\n"
            "### Airflow scheduler\n\n"
            "The scheduler triggers DAG execution."
        ),
    }

    chunks = chunk_document(document)

    assert len(chunks) == 1
    assert "Docker Compose services" in chunks[0]["content"]
    assert "Airflow scheduler" in chunks[0]["content"]
    assert "The scheduler triggers DAG execution" in chunks[0]["content"]


def test_v31_table_keeps_heading_context_and_isolation():
    document = {
        "document_id": "v31-table-context",
        "source_file": "deployment.md",
        "document_type": "md",
        "content": (
            "# Deployment\n\n"
            "## Services\n\n"
            "| Service | Purpose |\n"
            "|---|---|\n"
            "| Airflow | Orchestration |\n\n"
            "Ordinary paragraph after the table."
        ),
    }

    chunks = chunk_document(document)

    table_chunk = next(
        chunk for chunk in chunks
        if "| Airflow | Orchestration |" in chunk["content"]
    )

    assert "Deployment" in table_chunk["content"]
    assert "Services" in table_chunk["content"]
    assert table_chunk["is_table"] is True
    assert "Ordinary paragraph after the table" not in table_chunk["content"]


def test_v31_code_block_keeps_heading_context_and_isolation():
    document = {
        "document_id": "v31-code-context",
        "source_file": "deployment.md",
        "document_type": "md",
        "content": (
            "# Deployment\n\n"
            "## Airflow\n\n"
            "```yaml\n"
            "services:\n"
            "  scheduler:\n"
            "    image: airflow\n"
            "```\n\n"
            "Separate explanatory paragraph."
        ),
    }

    chunks = chunk_document(document)

    code_chunk = next(
        chunk for chunk in chunks
        if "image: airflow" in chunk["content"]
    )

    assert "Deployment" in code_chunk["content"]
    assert "Airflow" in code_chunk["content"]
    assert "Separate explanatory paragraph" not in code_chunk["content"]


def test_v31_sibling_sections_do_not_leak_context():
    document = {
        "document_id": "v31-service-boundary",
        "source_file": "deployment.md",
        "document_type": "md",
        "content": (
            "# Services\n\n"
            "## Scheduler\n\n"
            "Scheduler runs DAGs.\n\n"
            "## Worker\n\n"
            "Worker executes tasks."
        ),
    }

    chunks = chunk_document(document)

    scheduler = next(
        chunk for chunk in chunks
        if "Scheduler runs DAGs" in chunk["content"]
    )
    worker = next(
        chunk for chunk in chunks
        if "Worker executes tasks" in chunk["content"]
    )

    assert "Scheduler" in scheduler["content"]
    assert "Worker" not in scheduler["content"]
    assert "Worker" in worker["content"]
    assert "Scheduler" not in worker["content"]


def test_v31_context_counts_toward_chunk_size():
    document = {
        "document_id": "v31-size",
        "source_file": "large.md",
        "document_type": "md",
        "content": (
            "# A long parent heading\n\n"
            "## A long subsection heading\n\n"
            + ("A" * 2500)
        ),
    }

    chunks = chunk_document(
        document,
        chunk_size=500,
        chunk_overlap=100,
    )

    assert chunks
    assert all(len(chunk["content"]) <= 500 for chunk in chunks)


def test_v31_source_spans_are_valid():
    content = (
        "# Deployment\n\n"
        "## Scheduler\n\n"
        "The scheduler triggers DAG execution."
    )
    document = {
        "document_id": "v31-source-spans",
        "source_file": "deployment.md",
        "document_type": "md",
        "content": content,
    }

    chunks = chunk_document(document)

    assert chunks
    for chunk in chunks:
        for span in chunk["source_spans"]:
            assert 0 <= span["start_char"] < span["end_char"] <= len(content)
            assert content[span["start_char"]:span["end_char"]].strip()
