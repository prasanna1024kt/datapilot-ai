
import hashlib


def chunk_document(
    document: dict,
    chunk_size: int = 1000,
    chunk_overlap: int = 200,
) -> list[dict]:
    content = document.get("content", "")

    if not isinstance(content, str):
        raise TypeError("Document content must be a string")

    if chunk_size <= 0:
        raise ValueError("chunk_size must be greater than zero")

    if chunk_overlap < 0 or chunk_overlap >= chunk_size:
        raise ValueError(
            "chunk_overlap must be non-negative and smaller than chunk_size"
        )

    if not content.strip():
        return []

    chunks = []
    step = chunk_size - chunk_overlap

    for start in range(0, len(content), step):
        end = min(start + chunk_size, len(content))
        chunk_text = content[start:end]

        if not chunk_text.strip():
            continue

        chunk_id = hashlib.sha256(
            f"{document['document_id']}:{start}:{end}".encode("utf-8")
        ).hexdigest()

        chunks.append({
            "chunk_id": chunk_id,
            "document_id": document["document_id"],
            "source_file": document["source_file"],
            "document_type": document["document_type"],
            "chunk_index": len(chunks),
            "start_char": start,
            "end_char": end,
            "content": chunk_text,
        })

        if end == len(content):
            break

    return chunks
