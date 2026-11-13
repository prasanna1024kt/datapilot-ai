
from pathlib import Path

from copilot.rag.ingestion.document_loader import load_documents
from copilot.rag.ingestion.chunker import chunk_document
from copilot.rag.ingestion.embedding_service import EmbeddingService
from copilot.rag.vector_store import PineconeVectorStore


class RAGIngestionPipeline:
    def __init__(
        self,
        embedding_service=None,
        vector_store=None,
        chunk_size: int = 1000,
        chunk_overlap: int = 200,
        namespace: str = "datapilot-ai",
    ):
        self.embedding_service = embedding_service
        self.vector_store = vector_store
        self.chunk_size = chunk_size
        self.chunk_overlap = chunk_overlap
        self.namespace = namespace

    def ingest(
        self,
        directory: str,
        dry_run: bool = True,
        allow_external_operations: bool = False,
    ) -> dict:
        if not dry_run and not allow_external_operations:
            raise PermissionError(
                "Live ingestion requires explicit external-operation approval"
            )

        documents = load_documents(directory)

        chunks = []

        for document in documents:
            chunks.extend(
                chunk_document(
                    document,
                    chunk_size=self.chunk_size,
                    chunk_overlap=self.chunk_overlap,
                )
            )

        result = {
            "documents_loaded": len(documents),
            "chunks_created": len(chunks),
            "embeddings_generated": 0,
            "vectors_upserted": 0,
            "dry_run": dry_run,
        }

        if dry_run:
            return result

        if self.embedding_service is None:
            self.embedding_service = EmbeddingService()

        if self.vector_store is None:
            self.vector_store = PineconeVectorStore()

        embedded_chunks = self.embedding_service.embed_chunks(chunks)

        result["embeddings_generated"] = len(embedded_chunks)

        upserted = self.vector_store.upsert_chunks(embedded_chunks, namespace=self.namespace)

        result["vectors_upserted"] = upserted or 0

        return result
