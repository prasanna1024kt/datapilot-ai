
import argparse
from pathlib import Path

from copilot.rag.ingestion.document_loader import load_documents
from copilot.rag.ingestion.ingestion_pipeline import RAGIngestionPipeline
from copilot.rag.vector_store import PineconeVectorStore


DOCS_DIRECTORY = "/app/docs"
NAMESPACE = "datapilot-ai"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Perform the migration. Without this flag, preview only.",
    )
    args = parser.parse_args()

    documents = load_documents(DOCS_DIRECTORY)

    old_document_ids = [
        str(Path(DOCS_DIRECTORY) / document["document_id"])
        for document in documents
    ]

    print(f"Documents discovered: {len(documents)}")
    print(f"Target namespace: {NAMESPACE}")

    for document in documents:
        print(
            f"{DOCS_DIRECTORY}/{document['document_id']}"
            f" -> {document['document_id']}"
        )

    if not args.execute:
        print("\nDRY RUN: No Pinecone changes performed.")
        return

    # First ingest new vectors.
    pipeline = RAGIngestionPipeline()

    result = pipeline.ingest(
        directory=DOCS_DIRECTORY,
        dry_run=False,
        allow_external_operations=True,
    )

    print("Ingestion result:", result)

    # Verify that all expected vectors were submitted.
    if result["vectors_upserted"] != result["chunks_created"]:
        raise RuntimeError(
            "Vector upsert count does not match chunk count"
        )
    
    index = store._get_index()

    stats = index.describe_index_stats()

    namespace_stats = stats.namespaces.get(NAMESPACE)

    actual_count = (
        namespace_stats.vector_count
        if namespace_stats else 0
    )

    if actual_count < result["vectors_upserted"]:
        raise RuntimeError(
            f"Vector verification failed. "
            f"Expected at least {result['vectors_upserted']}, "
            f"found {actual_count}"
        )
    # Delete old absolute-path document vectors.
    store = PineconeVectorStore()

    deleted_documents = store.delete_documents(
        document_ids=old_document_ids,
        namespace=NAMESPACE,
    )

    print("Old document IDs submitted for deletion:", deleted_documents)
    print("Migration operations submitted.")


if __name__ == "__main__":
    main()
