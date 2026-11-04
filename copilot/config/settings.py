
from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class CopilotSettings(BaseSettings):
    # Application
    app_name: str = "DataPilot AI Copilot"
    app_version: str = "0.1.0"
    environment: str = "development"

    # OpenAI
    openai_api_key: str = ""
    openai_embedding_model: str = "text-embedding-3-small"
    openai_chat_model: str = "gpt-4.1"

    # Pinecone
    pinecone_api_key: str = ""
    pinecone_index_name: str = "datapilot-ai-knowledge"
    pinecone_cloud: str = "aws"
    pinecone_region: str = "us-east-1"
    pinecone_embedding_dimension: int = 1536

    # LangSmith
    langsmith_tracing: bool = False
    langsmith_api_key: str = ""
    langsmith_project: str = "datapilot-ai-copilot"

    # MCP
    mcp_server_url: str = "http://mcp-server:8000/mcp"

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )


@lru_cache
def get_settings() -> CopilotSettings:
    return CopilotSettings()
