
from pinecone import Pinecone

from copilot.config.settings import get_settings


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

    def upsert_chunks(self, chunks: list[dict], namespace: str = "datapilot-ai"):
        if not chunks:
            return

        index = self._get_index()

        vectors = []

        for chunk in chunks:
            vectors.append({
                "id": chunk["chunk_id"],
                "values": chunk["embedding"],
                "metadata": {
                    "document_id": chunk["document_id"],
                    "source_file": chunk["source_file"],
                    "document_type": chunk["document_type"],
                    "chunk_index": chunk["chunk_index"],
                    "content": chunk["content"],
                },
            })

        index.upsert(
            vectors=vectors,
            namespace=namespace,
        )

        return len(vectors)

    def search(
        self,
        query_embedding: list[float],
        top_k: int = 5,
        namespace: str = "datapilot-ai",
    ) -> list[dict]:
        if not query_embedding:
            raise ValueError("Query embedding cannot be empty")

        if len(query_embedding) != self.settings.pinecone_embedding_dimension:
            raise ValueError("Query embedding dimension mismatch")

        index = self._get_index()

        response = index.query(
            vector=query_embedding,
            top_k=top_k,
            include_metadata=True,
            namespace=namespace,
        )

        return [
            {
                "chunk_id": match.id,
                "score": match.score,
                "metadata": match.metadata,
            }
            for match in response.matches
        ]
