
from openai import OpenAI

from copilot.config.settings import get_settings


class EmbeddingService:
    def __init__(self, client=None):
        self.settings = get_settings()
        self.client = client

        if self.client is None and self.settings.openai_api_key:
            self.client = OpenAI(
                api_key=self.settings.openai_api_key
            )

    def embed_texts(self, texts: list[str]) -> list[list[float]]:
        if not texts:
            return []

        if not all(isinstance(text, str) and text.strip() for text in texts):
            raise ValueError("All input texts must be non-empty strings")

        if self.client is None:
            raise RuntimeError("OpenAI client is not configured")

        response = self.client.embeddings.create(
            model=self.settings.openai_embedding_model,
            input=texts,
            dimensions=self.settings.pinecone_embedding_dimension,
        )

        embeddings = [
            item.embedding for item in response.data
        ]

        if len(embeddings) != len(texts):
            raise ValueError("Embedding response count does not match input count")

        expected_dimension = self.settings.pinecone_embedding_dimension

        for embedding in embeddings:
            if len(embedding) != expected_dimension:
                raise ValueError(
                    f"Expected {expected_dimension} dimensions, "
                    f"received {len(embedding)}"
                )

        return embeddings

    def embed_chunks(self, chunks: list[dict]) -> list[dict]:
        if not chunks:
            return []

        texts = [chunk["content"] for chunk in chunks]
        embeddings = self.embed_texts(texts)

        return [
            {
                **chunk,
                "embedding": embedding,
            }
            for chunk, embedding in zip(chunks, embeddings)
        ]
