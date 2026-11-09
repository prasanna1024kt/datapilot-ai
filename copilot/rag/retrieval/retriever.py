
import math

from copilot.rag.ingestion.embedding_service import EmbeddingService
from copilot.rag.vector_store import PineconeVectorStore


class RAGRetriever:

    def __init__(
        self,
        embedding_service=None,
        vector_store=None,
        top_k: int = 5,
        candidate_k: int = 20,
        similarity_threshold: float = 0.70,
        strategy: str = "similarity",
        mmr_lambda: float = 0.70,
        max_chunks_per_document: int = 1,
    ):

        if top_k < 1:
            raise ValueError("top_k must be at least 1")

        if candidate_k < top_k:
            raise ValueError(
                "candidate_k must be greater than or equal to top_k"
            )

        if not 0.0 <= similarity_threshold <= 1.0:
            raise ValueError(
                "similarity_threshold must be between 0 and 1"
            )

        if strategy not in ("similarity", "mmr", "multi_query"):
            raise ValueError(
                "strategy must be 'similarity', 'mmr', or 'multi_query'"
            )

        if not 0.0 <= mmr_lambda <= 1.0:
            raise ValueError(
                "mmr_lambda must be between 0 and 1"
            )

        self.embedding_service = (
            embedding_service or EmbeddingService()
        )

        self.vector_store = (
            vector_store or PineconeVectorStore()
        )
        if (
        not isinstance(max_chunks_per_document, int)
        or isinstance(max_chunks_per_document, bool)
        or max_chunks_per_document < 1
):
            raise ValueError(
                "max_chunks_per_document must be a positive integer"
            )

        self.max_chunks_per_document = max_chunks_per_document

        self.top_k = top_k
        self.candidate_k = candidate_k
        self.similarity_threshold = similarity_threshold
        self.strategy = strategy
        self.mmr_lambda = mmr_lambda

    @staticmethod
    def cosine_similarity(vector_a, vector_b):

        if len(vector_a) != len(vector_b):
            raise ValueError(
                "Embedding dimensions do not match"
            )

        dot_product = sum(
            a * b for a, b in zip(vector_a, vector_b)
        )

        norm_a = math.sqrt(sum(a * a for a in vector_a))
        norm_b = math.sqrt(sum(b * b for b in vector_b))

        if norm_a == 0 or norm_b == 0:
            return 0.0

        return dot_product / (norm_a * norm_b)

    
    def _select_mmr(self, candidates):
        """
        Select relevant and diverse chunks using MMR.

        Enforces a configurable maximum number of chunks
        per document.
        """

        selected = []
        remaining = list(candidates)
        selected_document_counts = {}

        while remaining and len(selected) < self.top_k:

            best_candidate = None
            best_mmr_score = float("-inf")

            for candidate in remaining:

                document_id = candidate.get("document_id")

                document_key = (
                    document_id
                    if document_id
                    else candidate["chunk_id"]
                )

                selected_count = selected_document_counts.get(
                    document_key, 0
                )

                if selected_count >= self.max_chunks_per_document:
                    continue

                relevance = candidate["score"]

                if selected:

                    max_redundancy = max(
                        self.cosine_similarity(
                            candidate["embedding"],
                            chosen["embedding"],
                        )
                        for chosen in selected
                    )

                else:
                    max_redundancy = 0.0

                mmr_score = (
                    self.mmr_lambda * relevance
                    - (1 - self.mmr_lambda) * max_redundancy
                )

                if mmr_score > best_mmr_score:
                    best_mmr_score = mmr_score
                    best_candidate = candidate

            if best_candidate is None:
                break

            selected.append(best_candidate)

            document_id = best_candidate.get("document_id")

            document_key = (
                document_id
                if document_id
                else best_candidate["chunk_id"]
            )

            selected_document_counts[document_key] = (
                selected_document_counts.get(document_key, 0) + 1
            )

            remaining.remove(best_candidate)

        return selected

    @staticmethod
    def _build_queries(question: str) -> list[str]:
        """
        Build deterministic complementary queries.

        V1 focuses on agent responsibilities and
        remediation approval/execution workflows.
        """

        queries = [question.strip()]
        normalized = question.lower()

        agent_terms = (
            "agent",
            "supervisor",
            "rca",
            "root cause",
        )

        remediation_terms = (
            "remediation",
            "approval",
            "quarantine",
            "execution",
        )

        if any(term in normalized for term in agent_terms):
            queries.append(
                "RCA Agent, Remediation Agent and Supervisor Agent "
                "responsibilities, interactions, workflow boundaries "
                "and separation of duties"
            )

        if any(term in normalized for term in remediation_terms):
            queries.append(
                "Remediation approval workflow, human authorization, "
                "plan fingerprint validation, execution scope, "
                "DRY_RUN, APPLY, quarantine operation and audit evidence"
            )

        # Remove duplicate queries while preserving order.
        return list(dict.fromkeys(queries))

    def _retrieve_multi_query(self, question: str) -> list[dict]:
        """
        Retrieve using multiple queries and merge candidates
        with Reciprocal Rank Fusion (RRF).
        """

        queries = self._build_queries(question)

        embeddings = self.embedding_service.embed_texts(queries)

        if len(embeddings) != len(queries):
            raise RuntimeError(
                "Embedding count does not match query count"
            )

        rrf_k = 60
        merged = {}

        for query_embedding in embeddings:

            matches = self.vector_store.search(
                query_embedding=query_embedding,
                top_k=self.candidate_k,
                include_values=False,
            )

            for rank, match in enumerate(matches, start=1):

                score = match.get("score")

                if (
                    score is None
                    or score < self.similarity_threshold
                ):
                    continue

                chunk_id = match.get("chunk_id")

                if not chunk_id:
                    continue

                metadata = match.get("metadata") or {}

                if chunk_id not in merged:
                    merged[chunk_id] = {
                        "chunk_id": chunk_id,
                        "score": score,
                        "content": metadata.get("content", ""),
                        "source_file": metadata.get(
                            "source_file", "unknown"
                        ),
                        "document_id": metadata.get("document_id"),
                        "document_type": metadata.get("document_type"),
                        "retrieval_score": 0.0,
                        "query_matches": 0,
                    }

                candidate = merged[chunk_id]

                candidate["score"] = max(
                    candidate["score"],
                    score,
                )

                candidate["retrieval_score"] += (
                    1.0 / (rrf_k + rank)
                )

                candidate["query_matches"] += 1

        ranked = sorted(
            merged.values(),
            key=lambda item: (
                item["retrieval_score"],
                item["score"],
            ),
            reverse=True,
        )

        # Enforce document diversity.
        selected = []
        document_counts = {}

        for candidate in ranked:

            document_id = candidate.get("document_id")
            document_key = (
                document_id
                if document_id
                else candidate["chunk_id"]
            )

            count = document_counts.get(document_key, 0)

            if count >= self.max_chunks_per_document:
                continue

            selected.append(candidate)

            document_counts[document_key] = count + 1

            if len(selected) >= self.top_k:
                break

        return selected

    
    def retrieve(self, question: str) -> list[dict]:

        if not isinstance(question, str) or not question.strip():
            raise ValueError("Question cannot be empty")

        # Dispatch Multi-Query before single-query retrieval.
        if self.strategy == "multi_query":
            return self._retrieve_multi_query(question.strip())

        query_embedding = self.embedding_service.embed_texts(
            [question.strip()]
        )[0]

        matches = self.vector_store.search(
            query_embedding=query_embedding,
            top_k=self.candidate_k,
            include_values=(self.strategy == "mmr"),
        )

        candidates = []

        for match in matches:

            score = match.get("score")

            if (
                score is None
                or score < self.similarity_threshold
            ):
                continue

            metadata = match.get("metadata") or {}

            candidate = {
                "chunk_id": match["chunk_id"],
                "score": score,
                "content": metadata.get("content", ""),
                "source_file": metadata.get(
                    "source_file", "unknown"
                ),
                "document_id": metadata.get("document_id"),
                "document_type": metadata.get("document_type"),
            }

            if self.strategy == "mmr":

                embedding = match.get("embedding")

                if embedding is None:
                    raise RuntimeError(
                        "MMR requires candidate embeddings. "
                        "Enable include_values in vector search."
                    )

                candidate["embedding"] = embedding

            candidates.append(candidate)

        if self.strategy == "mmr":
            selected = self._select_mmr(candidates)
        else:
            selected = candidates[:self.top_k]

        # Never expose embedding vectors to the LLM or API.
        return [
            {
                key: value
                for key, value in result.items()
                if key != "embedding"
            }
            for result in selected
        ]

