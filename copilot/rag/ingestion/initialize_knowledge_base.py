from copilot.rag.vector_store import PineconeVectorStore


def main():
    vector_store = PineconeVectorStore()

    result = vector_store.ensure_index()

    print("\nPinecone Knowledge Base Initialized")
    print("-----------------------------------")
    print(f"Index: {result['index_name']}")
    print(f"Dimension: {result['dimension']}")
    print(f"Metric: {result['metric']}")
    print(f"Status: {result['status']}")


if __name__ == "__main__":
    main()