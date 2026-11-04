
from copilot.config.settings import get_settings


def validate_settings():
    settings = get_settings()

    print("DataPilot AI Copilot Configuration Validation")
    print("-" * 50)

    checks = {
        "Application name": bool(settings.app_name),
        "Embedding model": bool(settings.openai_embedding_model),
        "Pinecone index name": bool(settings.pinecone_index_name),
        "Pinecone dimension": settings.pinecone_embedding_dimension > 0,
        "MCP server URL": settings.mcp_server_url.startswith("http"),
        "OpenAI API key configured": bool(settings.openai_api_key),
        "Pinecone API key configured": bool(settings.pinecone_api_key),
    }

    for name, passed in checks.items():
        status = "PASS" if passed else "NOT CONFIGURED"
        print(f"{name}: {status}")

    print("-" * 50)

    return all(
        passed
        for name, passed in checks.items()
        if name not in {
            "OpenAI API key configured",
            "Pinecone API key configured",
        }
    )


if __name__ == "__main__":
    success = validate_settings()
    print(f"Configuration validation: {'SUCCESS' if success else 'FAILED'}")
