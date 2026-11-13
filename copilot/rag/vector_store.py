import time
from pinecone import Pinecone, ServerlessSpec
from copilot.config.settings import get_settings
import json 

class PineconeVectorStore:
    def __init__(self, client=None):
        self.settings = get_settings()
        self.client = client

        if self.client is None and self.settings.pinecone_api_key:
            self.client = Pinecone(
                api_key=self.settings.pinecone_api_key
            )

    def _get_index(self):
        if self.client is None:
            raise RuntimeError("Pinecone client is not configured")

        return self.client.Index(self.settings.pinecone_index_name)

    def verify_index_dimension(self) -> bool:
        index = self._get_index()
        description = self.client.describe_index(
            self.settings.pinecone_index_name
        )

        actual_dimension = description.dimension
        expected_dimension = self.settings.pinecone_embedding_dimension

        if actual_dimension != expected_dimension:
            raise ValueError(
                f"Pinecone dimension mismatch: expected "
                f"{expected_dimension}, received {actual_dimension}"
            )

        return True

    def upsert_chunks(
        self,
        chunks: list[dict],
        namespace: str = "datapilot-ai",
    ):
        if not chunks:
            return

        index = self._get_index()
        vectors = []

        for chunk in chunks:
            metadata = {
                "chunk_id": chunk["chunk_id"],
                "document_id": chunk["document_id"],
                "source_file": chunk["source_file"],
                "document_type": chunk["document_type"],
                "chunk_index": chunk["chunk_index"],
                "content": chunk["content"],
                "chunking_version": chunk.get("chunking_version", "unknown"),
                "is_table": bool(chunk.get("is_table", False)),
            }

            # Preserve exact source provenance as a JSON string.
            source_spans = chunk.get("source_spans")
            if source_spans is not None:
                metadata["source_spans_json"] = json.dumps(
                    source_spans,
                    separators=(",", ":"),
                )

            # Add optional structural metadata only when available.
            optional_fields = [
                "start_char",
                "end_char",
                "section_heading",
                "parent_heading",
                "heading_level",
                "config_path",
            ]

            for field in optional_fields:
                value = chunk.get(field)
                if value is not None:
                    metadata[field] = value

            vectors.append({
                "id": chunk["chunk_id"],
                "values": chunk["embedding"],
                "metadata": metadata,
            })

        batch_size = 50
        total_upserted = 0

        for start in range(0, len(vectors), batch_size):
            batch = vectors[start:start + batch_size]

            index.upsert(
                vectors=batch,
                namespace=namespace,
            )

            total_upserted += len(batch)

        return total_upserted

    def search(
        self,
        query_embedding: list[float],
        top_k: int = 5,
        namespace: str = "datapilot-ai",
        include_values: bool = False,
    ) -> list[dict]:

        if not query_embedding:
            raise ValueError("Query embedding cannot be empty")

        if len(query_embedding) != self.settings.pinecone_embedding_dimension:
            raise ValueError("Query embedding dimension mismatch")

        if top_k < 1:
            raise ValueError("top_k must be at least 1")

        index = self._get_index()

        response = index.query(
            vector=query_embedding,
            top_k=top_k,
            include_metadata=True,
            include_values=include_values,
            namespace=namespace,
        )

        results = []

        for match in response.matches:
            result = {
                "chunk_id": match.id,
                "score": match.score,
                "metadata": match.metadata or {},
            }

            if include_values:
                result["embedding"] = match.values

            results.append(result)

        return results

    
    def ensure_index(self) -> dict:
        """
        Create the Pinecone index if it does not exist.
        Validate dimension if it already exists.
        """

        if self.client is None:
            raise RuntimeError("Pinecone client is not configured")

        index_name = self.settings.pinecone_index_name

        existing_indexes = [
            index.name for index in self.client.list_indexes()
        ]

        if index_name not in existing_indexes:
            self.client.create_index(
                name=index_name,
                dimension=self.settings.pinecone_embedding_dimension,
                metric="cosine",
                spec=ServerlessSpec(
                    cloud=self.settings.pinecone_cloud,
                    region=self.settings.pinecone_region,
                ),
            )

            print(f"Creating Pinecone index: {index_name}")

        # Wait until the index is ready
        for _ in range(60):
            description = self.client.describe_index(index_name)

            if description.status["ready"]:
                break

            time.sleep(2)
        else:
            raise TimeoutError(
                f"Pinecone index {index_name} did not become ready"
            )

        self.verify_index_dimension()

        return {
            "index_name": index_name,
            "dimension": self.settings.pinecone_embedding_dimension,
            "metric": "cosine",
            "status": "ready",
        }

    def delete_documents(self, document_ids: list[str], namespace: str = "datapilot-ai") -> int:
        """
        Delete all vectors belonging to the specified documents.

        Uses the document_id metadata field.
        """
        if not document_ids:
            return 0
        index = self._get_index()
        deleted_documents = 0

        for document_id in document_ids:
            if not document_id or not isinstance(document_id, str):
                raise ValueError("document_id must be a non-empty string")

            index.delete(
                filter={
                    "document_id": {"$eq": document_id}
                },
                namespace=namespace,
            )

            deleted_documents += 1

        return deleted_documents
