
from copilot.rag.ingestion.embedding_service import EmbeddingService
from copilot.rag.vector_store import PineconeVectorStore


class RAGRetriever:
    def __init__(
        self,
        embedding_service=None,
        vector_store=None,
        top_k: int = 5,
        similarity_threshold: float = 0.70,
    ):
        if top_k < 1:
            raise ValueError("top_k must be at least 1")

        if not 0.0 <= similarity_threshold <= 1.0:
            raise ValueError("similarity_threshold must be between 0 and 1")

        self.embedding_service = embedding_service or EmbeddingService()
        self.vector_store = vector_store or PineconeVectorStore()
        self.top_k = top_k
        self.similarity_threshold = similarity_threshold

    def retrieve(self, question: str) -> list[dict]:
        if not isinstance(question, str) or not question.strip():
            raise ValueError("Question cannot be empty")

        query_embedding = self.embedding_service.embed_texts(
            [question.strip()]
        )[0]

        matches = self.vector_store.search(
            query_embedding=query_embedding,
            top_k=self.top_k,
        )

        results = []

        for match in matches:
            score = match.get("score")

            if score is None or score < self.similarity_threshold:
                continue

            metadata = match.get("metadata") or {}

            results.append({
                "chunk_id": match["chunk_id"],
                "score": score,
                "content": metadata.get("content", ""),
                "source_file": metadata.get("source_file", "unknown"),
                "document_id": metadata.get("document_id"),
                "document_type": metadata.get("document_type"),
            })

        return results
