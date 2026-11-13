import json
from collections import Counter

from copilot.rag.vector_store import PineconeVectorStore

PRODUCTION_NAMESPACE = "datapilot-ai"
BACKUP_NAMESPACE = "datapilot-ai-backup-2026-11-12"

EXPECTED_PRODUCTION_COUNT = 625
EXPECTED_BACKUP_COUNT = 316
EXPECTED_DOCUMENTS = 14
BATCH_SIZE = 100
V3_1_NAMESPACE = "datapilot-ai-v3-1-evaluation"

EXPECTED_V3_1_COUNT = 729
EXPECTED_V3_1_VERSION = "v3.1"

def list_all_ids(index, namespace):
    ids = []

    # Pinecone SDK 10.x returns a generator of ListResponse pages.
    pages = index.list(
        namespace=namespace,
        limit=BATCH_SIZE,
    )

    for page in pages:
        for vector in page.vectors or []:
            ids.append(vector.id)

    return ids

def fetch_all(index, ids, namespace):
    records = {}

    for start in range(0, len(ids), BATCH_SIZE):
        batch = ids[start:start + BATCH_SIZE]

        response = index.fetch(
            ids=batch,
            namespace=namespace,
        )

        records.update(response.vectors)

    return records

def validate_namespace(
    records,
    listed_ids,
    expected_count,
    expected_version,
    label,
):
    errors = []
    documents = set()
    source_files = set()
    chunking_versions = Counter()

    for vector_id, vector in records.items():
        metadata = vector.metadata or {}

        if metadata.get("chunk_id") != vector_id:
            errors.append(f"{vector_id}: chunk_id mismatch")

        document_id = metadata.get("document_id")
        if not document_id:
            errors.append(f"{vector_id}: missing document_id")
        else:
            documents.add(document_id)

        source_file = metadata.get("source_file")
        if not source_file:
            errors.append(f"{vector_id}: missing source_file")
        else:
            source_files.add(source_file)

        if not metadata.get("content"):
            errors.append(f"{vector_id}: missing content")

        version = metadata.get("chunking_version")
        chunking_versions[version] += 1

        if version != expected_version:
            errors.append(
                f"{vector_id}: expected {expected_version}, got {version}"
            )

        raw_spans = metadata.get("source_spans_json")

        try:
            spans = json.loads(raw_spans)

            if not isinstance(spans, list) or not spans:
                raise ValueError("empty or invalid span list")

            content_length = len(metadata.get("content", ""))

            for span in spans:
                if not isinstance(span, dict):
                    raise ValueError("span is not an object")

                start = span.get("start_char")
                end = span.get("end_char")

                if (
                    not isinstance(start, int)
                    or isinstance(start, bool)
                    or not isinstance(end, int)
                    or isinstance(end, bool)
                    or start < 0
                    or end <= start
                ):
                    raise ValueError("invalid source boundaries")

        except (TypeError, ValueError, json.JSONDecodeError) as exc:
            errors.append(
                f"{vector_id}: invalid source_spans_json: {exc}"
            )

    listed_set = set(listed_ids)
    fetched_set = set(records)

    if listed_set - fetched_set:
        errors.append(f"Missing fetched IDs: {len(listed_set - fetched_set)}")

    if fetched_set - listed_set:
        errors.append(f"Unexpected fetched IDs: {len(fetched_set - listed_set)}")

    print(f"\n{label} VALIDATION")
    print("-" * 40)
    print("Listed IDs:", len(listed_ids))
    print("Fetched records:", len(records))
    print("Distinct documents:", len(documents))
    print("Distinct source files:", len(source_files))
    print("Chunking versions:", dict(chunking_versions))
    print("Validation errors:", len(errors))

    if errors:
        for error in errors[:50]:
            print("ERROR:", error)

    passed = (
        len(listed_ids) == expected_count
        and len(records) == expected_count
        and len(documents) == EXPECTED_DOCUMENTS
        and len(source_files) == EXPECTED_DOCUMENTS
        and set(listed_ids) == set(records)
        and not errors
    )

    print("RESULT:", "PASS" if passed else "FAIL")

    return passed

def main():
    store = PineconeVectorStore()
    index = store._get_index()

    print("Verifying Pinecone index dimension...")
    store.verify_index_dimension()

    print("\nChecking production namespace...")
    production_ids = list_all_ids(
        index,
        PRODUCTION_NAMESPACE,
    )

    production_records = fetch_all(
        index,
        production_ids,
        PRODUCTION_NAMESPACE,
    )

    production_ok = validate_namespace(
        production_records,
        production_ids,
        expected_count=EXPECTED_PRODUCTION_COUNT,
        expected_version="v3",
        label="PRODUCTION V3",
    )

    print("\nChecking backup namespace...")
    backup_ids = list_all_ids(
        index,
        BACKUP_NAMESPACE,
    )

    backup_records = fetch_all(
        index,
        backup_ids,
        BACKUP_NAMESPACE,
    )

    backup_ok = (
        len(backup_ids) == EXPECTED_BACKUP_COUNT
        and len(backup_records) == EXPECTED_BACKUP_COUNT
        and set(backup_ids) == set(backup_records)
    )

    print("\nChecking V3.1 evaluation namespace...")

    v3_1_ids = list_all_ids(
        index,
        V3_1_NAMESPACE,
    )

    v3_1_records = fetch_all(
        index,
        v3_1_ids,
        V3_1_NAMESPACE,
    )

    v3_1_ok = validate_namespace(
        v3_1_records,
        v3_1_ids,
        expected_count=EXPECTED_V3_1_COUNT,
        expected_version=EXPECTED_V3_1_VERSION,
        label="V3.1 EVALUATION",
    )

    print("\nBACKUP VALIDATION")
    print("-----------------")
    print("Listed IDs:", len(backup_ids))
    print("Fetched records:", len(backup_records))
    print("Backup count correct:", backup_ok)

    print("\nFINAL RESULT")
    print("------------")

    if production_ok and backup_ok and v3_1_ok:
        print("PASS: All Pinecone namespaces validated.")
    else:
        print("FAIL: Validation incomplete or mismatches found.")
        raise SystemExit(1)

if __name__ == "__main__":
    main()
