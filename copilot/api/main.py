
import time
from datetime import datetime, timezone
from uuid import uuid4
from typing import Optional, Literal

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from copilot.config.settings import get_settings
from copilot.rag.retrieval.retriever import RAGRetriever
from copilot.rag.llm_response_generator import LLMResponseGenerator


settings = get_settings()

app = FastAPI(
    title=settings.app_name,
    description="AI-powered Data Engineering and DataOps Copilot",
    version=settings.app_version,
)


# --------------------------------------------------
# REQUEST MODEL
# --------------------------------------------------

class ChatRequest(BaseModel):
    question: str = Field(min_length=1, max_length=10000)


# --------------------------------------------------
# RESPONSE MODELS
# --------------------------------------------------

class SourceInfo(BaseModel):
    source_file: str
    chunk_id: Optional[str] = None
    document_id: Optional[str] = None
    document_type: Optional[str] = None
    score: Optional[float] = None


class AgentInfo(BaseModel):
    orchestration_status: str = "not_integrated"
    supervisor: Optional[str] = None
    selected_agent: Optional[str] = None
    routing_reason: Optional[str] = None
    agents_invoked: list[str] = Field(default_factory=list)


class RAGInfo(BaseModel):
    enabled: bool
    retrieval_status: str
    vector_database: str = "pinecone"
    embedding_model: Optional[str] = None
    query: str
    top_k: int
    similarity_threshold: float
    results_found: int
    sources: list[SourceInfo] = Field(default_factory=list)


class UsageInfo(BaseModel):
    prompt_tokens: Optional[int] = None
    completion_tokens: Optional[int] = None
    total_tokens: Optional[int] = None


class ExecutionInfo(BaseModel):
    execution_id: str
    components_invoked: list[str]
    status: str
    duration_ms: int
    llm_model: Optional[str] = None
    llm_response_id: Optional[str] = None


class MetadataInfo(BaseModel):
    project: str = "datapilot-ai"
    environment: str
    api_version: str
    timestamp: datetime


class ChatResponse(BaseModel):
    request_id: str
    status: Literal["success", "partial", "failed"]

    # Backward-compatible fields
    answer: str
    sources: list[SourceInfo]
    grounded: bool

    # Enhanced fields
    agent: AgentInfo
    rag: RAGInfo
    execution: ExecutionInfo
    usage: UsageInfo
    metadata: MetadataInfo


# --------------------------------------------------
# HEALTH ENDPOINTS
# --------------------------------------------------

@app.get("/")
async def root():
    return {
        "application": settings.app_name,
        "version": settings.app_version,
        "status": "running",
    }


@app.get("/health")
async def health():
    return {
        "status": "healthy",
        "service": "copilot-api",
        "environment": settings.environment,
    }


# --------------------------------------------------
# CHAT ENDPOINT
# --------------------------------------------------

@app.post(
    "/api/v1/chat",
    response_model=ChatResponse,
    summary="DataPilot AI Agentic RAG Chat",
    description=(
        "Processes questions using Pinecone retrieval "
        "and OpenAI response generation."
    ),
)
def chat(request: ChatRequest):

    request_id = f"req_{uuid4().hex[:12]}"
    execution_id = f"exec_{uuid4().hex[:12]}"

    start_time = time.perf_counter()

    try:
        # STEP 1: RETRIEVAL

        retriever = RAGRetriever(
            top_k=5,
            candidate_k=20,
            similarity_threshold=0.40,
            strategy="similarity",
        )

        retrieved_chunks = retriever.retrieve(request.question)

        # STEP 2: LLM GENERATION

        generator = LLMResponseGenerator()

        result = generator.generate_response(
            question=request.question,
            retrieved_chunks=retrieved_chunks,
        )

        # STEP 3: RESPONSE METADATA

        duration_ms = int(
            (time.perf_counter() - start_time) * 1000
        )

        sources = result.get("sources", [])

        # STEP 4: BUILD RESPONSE

        return ChatResponse(
            request_id=request_id,
            status="success",

            # Existing response fields
            answer=result["answer"],
            sources=sources,
            grounded=result["grounded"],

            # Agent information
            agent=AgentInfo(
                orchestration_status="not_integrated",
            ),

            # RAG information
            rag=RAGInfo(
                enabled=True,
                retrieval_status=(
                    "success" if retrieved_chunks else "no_results"
                ),
                vector_database="pinecone",
                query=request.question,
                top_k=retriever.top_k,
                similarity_threshold=retriever.similarity_threshold,
                results_found=len(retrieved_chunks),
                sources=sources,
            ),

            # Execution information
            execution=ExecutionInfo(
                execution_id=execution_id,
                components_invoked=[
                    "EmbeddingService",
                    "PineconeVectorStore",
                    "RAGRetriever",
                    "LLMResponseGenerator",
                ],
                status="completed",
                duration_ms=duration_ms,
                llm_model=result.get("model"),
                llm_response_id=result.get("response_id"),
            ),

            # Actual LLM token usage
            usage=UsageInfo(
                **(result.get("usage") or {})
            ),

            # Application metadata
            metadata=MetadataInfo(
                environment=settings.environment,
                api_version=settings.app_version,
                timestamp=datetime.now(timezone.utc),
            ),
        )

    except RuntimeError as exc:
        raise HTTPException(
            status_code=503,
            detail={
                "request_id": request_id,
                "error": "SERVICE_ERROR",
                "message": str(exc),
            },
        ) from exc

    except ValueError as exc:
        raise HTTPException(
            status_code=400,
            detail={
                "request_id": request_id,
                "error": "VALIDATION_ERROR",
                "message": str(exc),
            },
        ) from exc
