import importlib.machinery
import importlib.util
import json
import re
from pathlib import Path
from statistics import mean

from copilot.rag.ingestion.document_loader import load_documents
from copilot.rag.ingestion.chunker import chunk_document as chunk_v31

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

V3_BACKUP = ROOT / "copilot/rag/ingestion/chunker.py.v3.backup"
OUTPUT = ROOT / "results/chunker_evaluation/v3_vs_v3_1.json"


def load_v3_chunker():
    loader = importlib.machinery.SourceFileLoader(
        "chunker_v3_backup",
        str(V3_BACKUP),
    )
    spec = importlib.util.spec_from_loader(loader.name, loader)

    module = importlib.util.module_from_spec(spec)
    loader.exec_module(module)

    return module.chunk_document


def merge_intervals(intervals):
    if not intervals:
        return []

    intervals = sorted(intervals)
    merged = [list(intervals[0])]

    for start, end in intervals[1:]:
        last = merged[-1]

        if start <= last[1]:
            last[1] = max(last[1], end)
        else:
            merged.append([start, end])

    return merged


def evaluate_version(documents, chunker, version):
    all_chunks = []
    document_results = []

    for document in documents:
        chunks = chunker(
            document,
            chunk_size=1000,
            chunk_overlap=200,
        )

        all_chunks.extend(chunks)

        intervals = []
        invalid_spans = 0

        for chunk in chunks:
            for span in chunk.get("source_spans", []):
                if span.get("synthetic", False):
                    continue

                start = span.get("start_char")
                end = span.get("end_char")

                if (
                    not isinstance(start, int)
                    or not isinstance(end, int)
                    or start < 0
                    or end <= start
                    or end > len(document["content"])
                ):
                    invalid_spans += 1
                    continue

                intervals.append((start, end))

        covered = sum(
            end - start
            for start, end in merge_intervals(intervals)
        )

        content_length = len(document["content"])
        coverage = (
            round(covered / content_length * 100, 2)
            if content_length else 100.0
        )

        document_results.append({
            "document": document["source_file"],
            "document_type": document["document_type"],
            "characters": content_length,
            "chunks": len(chunks),
            "source_coverage_percent": coverage,
            "invalid_source_spans": invalid_spans,
        })

    sizes = [len(c["content"]) for c in all_chunks]

    heading_context = sum(
        c["content"].startswith("[Section:")
        for c in all_chunks
    )

    heading_only = sum(
        bool(re.fullmatch(
            r"#{1,6}[ \t]+.+",
            re.sub(
                r"^\[Section:.*?\]\n",
                "",
                c["content"],
                count=1,
            ).strip(),
        ))
        for c in all_chunks
    )

    table_chunks = [
        c for c in all_chunks if c.get("is_table", False)
    ]

    code_chunks = [
        c for c in all_chunks
        if "```" in c["content"] or "~~~" in c["content"]
    ]

    mixed_table_code = sum(
        c.get("is_table", False)
        and ("```" in c["content"] or "~~~" in c["content"])
        for c in all_chunks
    )

    return {
        "version": version,
        "documents": len(documents),
        "total_chunks": len(all_chunks),
        "average_chunk_chars": round(mean(sizes), 2) if sizes else 0,
        "maximum_chunk_chars": max(sizes, default=0),
        "minimum_chunk_chars": min(sizes, default=0),
        "heading_context_chunks": heading_context,
        "heading_only_chunks": heading_only,
        "table_chunks": len(table_chunks),
        "code_chunks": len(code_chunks),
        "mixed_table_code_chunks": mixed_table_code,
        "invalid_source_spans": sum(
            d["invalid_source_spans"] for d in document_results
        ),
        "average_source_coverage_percent": round(
            mean(d["source_coverage_percent"] for d in document_results),
            2,
        ) if document_results else 0,
        "per_document": document_results,
    }


def main():
    if not V3_BACKUP.is_file():
        raise FileNotFoundError(
            f"V3 backup not found: {V3_BACKUP}"
        )

    documents = load_documents(str(ROOT / "docs"))

    print(f"Documents loaded: {len(documents)}")

    if not documents:
        raise RuntimeError("No documents found in docs/")

    chunk_v3 = load_v3_chunker()

    print("Evaluating V3...")
    v3_results = evaluate_version(documents, chunk_v3, "v3")

    print("Evaluating V3.1...")
    v31_results = evaluate_version(documents, chunk_v31, "v3.1")

    report = {
        "evaluation": {
            "chunk_size": 1000,
            "chunk_overlap": 200,
            "documents_directory": "docs",
            "pinecone_accessed": False,
            "embeddings_generated": False,
        },
        "v3": v3_results,
        "v3_1": v31_results,
    }

    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT.write_text(
        json.dumps(report, indent=2),
        encoding="utf-8",
    )

    print("\n=== V3 vs V3.1 SUMMARY ===")

    metrics = [
        "total_chunks",
        "average_chunk_chars",
        "maximum_chunk_chars",
        "heading_context_chunks",
        "heading_only_chunks",
        "table_chunks",
        "code_chunks",
        "mixed_table_code_chunks",
        "invalid_source_spans",
        "average_source_coverage_percent",
    ]

    print(f"{'Metric':40} {'V3':>12} {'V3.1':>12}")
    print("-" * 66)

    for metric in metrics:
        print(
            f"{metric:40} "
            f"{str(v3_results[metric]):>12} "
            f"{str(v31_results[metric]):>12}"
        )

    print("\n=== PER-DOCUMENT CHUNK COUNTS ===")
    print(f"{'Document':48} {'V3':>8} {'V3.1':>8}")

    v3_docs = {
        d["document"]: d for d in v3_results["per_document"]
    }
    v31_docs = {
        d["document"]: d for d in v31_results["per_document"]
    }

    for name in sorted(v3_docs):
        print(
            f"{name:48} "
            f"{v3_docs[name]['chunks']:>8} "
            f"{v31_docs[name]['chunks']:>8}"
        )

    print(f"\nReport saved to: {OUTPUT.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
