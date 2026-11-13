
import json
import re
import time
from datetime import datetime, timezone
from uuid import uuid4
from typing import Optional, Literal, Any
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, Field
from copilot.config.settings import get_settings
from copilot.rag.retrieval.retriever import RAGRetriever
from copilot.rag.llm_response_generator import LLMResponseGenerator
from copilot.integrations.airflow_service import AirflowService
from copilot.integrations.mcp_service import MCPService
from types import SimpleNamespace
from agents.supervisor.supervisor_agent import run_dag_assessment

settings = get_settings()
DQ_TABLES = [
    "olist_customers",
    "olist_orders",
    "olist_order_items",
    "olist_order_payments",
    "olist_order_reviews",
    "olist_products",
    "olist_sellers",
    "olist_geolocation",
    "olist_product_category_translation",
]

GOLD_TABLES = DQ_TABLES.copy()

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
    answer: str
    sources: list[SourceInfo]
    grounded: bool
    agent: AgentInfo
    rag: RAGInfo
    execution: ExecutionInfo
    usage: UsageInfo
    metadata: MetadataInfo


# --------------------------------------------------
# SHARED RESPONSE BUILDER
# --------------------------------------------------

def build_chat_response(
    request_id: str,
    execution_id: str,
    question: str,
    answer: str,
    duration_ms: int,
    components: list[str],
    *,
    grounded: bool = True,
    sources: Optional[list] = None,
    rag_enabled: bool = False,
    retrieval_status: str = "not_used",
    results_found: int = 0,
    top_k: int = 0,
    similarity_threshold: float = 0.0,
    routing_reason: Optional[str] = None,
    llm_model: Optional[str] = None,
    llm_response_id: Optional[str] = None,
    usage: Optional[dict] = None,
    orchestration_status: str = "not_integrated",
    selected_agent: Optional[str] = None,
    agents_invoked: Optional[list[str]] = None,
    supervisor: Optional[str] = None,
) -> ChatResponse:

    sources = sources or []

    return ChatResponse(
        request_id=request_id,
        status="success",
        answer=answer,
        sources=sources,
        grounded=grounded,
        agent=AgentInfo(
            orchestration_status=orchestration_status,
            supervisor=supervisor,
            selected_agent=selected_agent,
            routing_reason=routing_reason,
            agents_invoked=agents_invoked or [],
        ),
        rag=RAGInfo(
            enabled=rag_enabled,
            retrieval_status=retrieval_status,
            vector_database="pinecone",
            query=question,
            top_k=top_k,
            similarity_threshold=similarity_threshold,
            results_found=results_found,
            sources=sources,
        ),
        execution=ExecutionInfo(
            execution_id=execution_id,
            components_invoked=components,
            status="completed",
            duration_ms=duration_ms,
            llm_model=llm_model,
            llm_response_id=llm_response_id,
        ),
        usage=UsageInfo(**(usage or {})),
        metadata=MetadataInfo(
            environment=settings.environment,
            api_version=settings.app_version,
            timestamp=datetime.now(timezone.utc),
        ),
    )


# --------------------------------------------------
# AIRFLOW INTENT ROUTING
# --------------------------------------------------

def detect_airflow_intent(question: str) -> Optional[str]:
    """
    Deterministic Stage 1 intent detection.
    Only read-only Airflow operations are supported.
    """

    q = question.lower()

    list_dag_phrases = [
        "list dags",
        "list all dags",
        "show dags",
        "show all dags",
        "available dags",
        "airflow dags",
    ]

    operational_phrases = [
        "dag run",
        "dag runs",
        "latest run",
        "recent runs",
        "run status",
        "task status",
        "task states",
        "execution status",
        "airflow status",
    ]

    if any(phrase in q for phrase in list_dag_phrases):
        return "list_dags"

    if any(phrase in q for phrase in operational_phrases):
        return "dag_runs"

    return None

def detect_supervisor_intent(question: str) -> bool:
    q = question.lower()

    phrases = [
        "assess",
        "assessment",
        "supervisor analysis",
        "supervisor assessment",
        "investigate pipeline",
    ]

    return any(phrase in q for phrase in phrases)

def extract_requested_run_id(question: str) -> str | None:
    """Extract an explicitly supplied Airflow DAG run ID."""

    patterns = [
        r"\b(?:dag\s+)?run\s+id\s*[:=]\s*([^\s,]+)",
        r"\brun_id\s*[:=]\s*([^\s,]+)",
    ]

    for pattern in patterns:
        match = re.search(pattern, question, flags=re.IGNORECASE)
        if match:
            return match.group(1).strip().strip("'\"`")

    return None

def execute_supervisor_assessment(airflow: AirflowService, dag_id: str, requested_run_id: str | None = None) -> dict[str, Any]:
    """Assess a specific DAG run or the latest DAG run."""

    if requested_run_id:
        run_id = requested_run_id
    else:
        runs_response = airflow.list_runs(dag_id=dag_id, limit=10)
        runs = runs_response.get("runs", [])

        if not runs:
            raise RuntimeError(f"No DAG runs found for {dag_id}")

        latest_run = max(runs, key=get_run_sort_timestamp)
        run_id = latest_run.get("dag_run_id")

        if not run_id:
            raise RuntimeError("Unable to identify the latest DAG run ID.")

    details = airflow.get_run_details(
        dag_id=dag_id,
        dag_run_id=run_id,
    )
    run_details = details.get("run", {})

    if not run_details:
        raise RuntimeError(
            f"Unable to retrieve DAG run details for run_id={run_id}"
        )

    dag_run = SimpleNamespace(
        dag_id=dag_id,
        run_id=run_id,
        state=run_details.get("state"),
    )

    assessment = run_dag_assessment(
        dag_run=dag_run,
        dq_tables=DQ_TABLES,
        gold_tables=GOLD_TABLES,
    )

    return {
        "assessment": assessment,
        "airflow_run": run_details,
    }


def format_supervisor_assessment(result: dict[str, Any]) -> str:
    """Format the structured Supervisor assessment for the chat response."""

    assessment = result["assessment"]
    airflow_run = result.get("airflow_run", {})
    pipeline = assessment["pipeline"]
    dq = assessment["dq_assessment"]
    rca = assessment["rca_assessment"]
    gold = assessment["gold_assessment"]
    decision = assessment["supervisor_decision"]

    lines = [
        f"Supervisor assessment: {pipeline['dag_id']}",
        f"DAG run ID: {pipeline['dag_run_id']}",
        f"Airflow run state: {airflow_run.get('state', 'unknown')}",
        f"Supervisor decision: {decision['decision']}",
        f"Tasks assessed: {pipeline['task_count']}",
        f"Task state counts: {pipeline['task_state_counts']}",
        "",
        "Data quality summary:",
        f"- Datasets assessed: {dq['dataset_count']}",
        f"- Status counts: {dq['status_counts']}",
    ]

    for dataset, item in dq.get("results", {}).items():
        status = item.get("dq_status") or item.get("status", "UNKNOWN")
        lines.append(f"- {dataset}: {status} (DQ task: {item.get('dq_task_state', 'unknown')})")

    lines.extend(["", "RCA summary:"])
    for dataset, item in rca.get("results", {}).items():
        status = item.get("status", "UNKNOWN")
        lines.append(f"- {dataset}: {status}")
        rca_report = item.get("result") or {}
        if rca_report:
            classification = rca_report.get("classification")
            root_cause = rca_report.get("root_cause")
            if classification:
                lines.append(f"  Classification: {classification}")
            if root_cause:
                lines.append(f"  Root cause: {root_cause}")

    lines.extend(["", "Gold execution summary:"])
    for dataset, status in gold.get("dataset_results", {}).items():
        lines.append(f"- {dataset}: {status}")
    lines.append(
        f"- Business summary: {gold.get('business_summary_status', 'unknown')}"
    )

    lines.extend(["", "Recommended next actions:"])
    for action in decision.get("recommended_next_actions", []):
        lines.append(f"- {action}")

    safety = assessment.get("execution_safety", {})
    lines.extend([
        "",
        "Execution safety:",
        f"- DAG triggered: {safety.get('dag_triggered', False)}",
        f"- RCA invoked by Supervisor: {safety.get('rca_invoked_by_supervisor', False)}",
        f"- Remediation executed: {safety.get('remediation_executed', False)}",
    ])

    return "\n".join(lines)

def find_dag_id(question: str, airflow: AirflowService) -> Optional[str]:
    """
    Match a DAG ID against actual IDs returned by Airflow.
    Avoid guessing or constructing DAG IDs.
    """

    dags = airflow.list_dags(limit=100).get("dags", [])
    question_lower = question.lower()

    matches = [
        dag["dag_id"]
        for dag in dags
        if dag.get("dag_id")
        and dag["dag_id"].lower() in question_lower
    ]

    if not matches:
        return None

    # Prefer the longest exact DAG ID match.
    return max(matches, key=len)

def get_run_sort_timestamp(run: dict) -> datetime:
    timestamp = run.get("start_date") or run.get("logical_date")

    if not timestamp:
        return datetime.min.replace(tzinfo=timezone.utc)

    try:
        return datetime.fromisoformat(
            timestamp.replace("Z", "+00:00")
        )
    except (ValueError, TypeError):
        return datetime.min.replace(tzinfo=timezone.utc)
# --------------------------------------------------
# AIRFLOW ANSWER GENERATION
# --------------------------------------------------

def format_dag_list(airflow: AirflowService) -> str:
    result = airflow.list_dags(limit=100)
    dags = result.get("dags", [])

    if not dags:
        return "Airflow did not return any DAGs."

    lines = [
        f"Found {len(dags)} Airflow DAG(s):",
        "",
    ]

    for dag in dags:
        paused = dag.get("is_paused")
        active = dag.get("is_active")

        lines.append(
            f"- {dag.get('dag_id')} "
            f"(paused={paused}, active={active})"
        )

    return "\n".join(lines)

@app.get(
    "/api/v1/remediation/dq/{dataset}",
    summary="Get dataset data quality results",
    description="Read-only access to dataset DQ results through MCP."
)
async def get_remediation_dq_result(dataset: str):
    dataset = dataset.strip()

    if not dataset or len(dataset) > 128:
        raise HTTPException(
            status_code=400,
            detail="A valid dataset name is required."
        )

    try:
        mcp = MCPService()

        result = await mcp.call_tool(
            "get_dq_result",
            {"dataset": dataset}
        )

        return _parse_mcp_tool_result(result)

    except RuntimeError as exc:
        raise HTTPException(
            status_code=502,
            detail={
                "error": "MCP_TOOL_ERROR",
                "message": str(exc)
            }
        ) from exc

    except Exception as exc:
        raise HTTPException(
            status_code=503,
            detail={
                "error": "MCP_SERVICE_UNAVAILABLE",
                "message": str(exc)
            }
        ) from exc


def format_dag_runs(question: str, airflow: AirflowService, dag_id: str,) -> str:

    result = airflow.list_runs(dag_id=dag_id, limit=10)
    runs = result.get("runs", [])

    if not runs:
        return f"No DAG runs were returned for '{dag_id}'."

    # Sort runs newest-first
    runs = sorted(
        runs,
        key=get_run_sort_timestamp,
        reverse=True
    )

    q = question.lower()

    latest_requested = any(
        phrase in q
        for phrase in [
            "latest run",
            "latest status",
            "task status",
            "task states",
            "execution status",
        ]
    )

    if latest_requested:
        latest_run = runs[0]
        run_id = latest_run.get("dag_run_id")

        if not run_id:
            return (
                f"Unable to identify the DAG run ID "
                f"for the latest run of '{dag_id}'."
            )

        details = airflow.get_run_details(
            dag_id=dag_id,
            dag_run_id=run_id,
        )

        run = details.get("run", {})

        lines = [
            f"Latest run for DAG: {dag_id}",
            f"Run ID: {run_id}",
            f"State: {run.get('state')}",
            f"Start time: {run.get('start_date')}",
            f"End time: {run.get('end_date')}",
            "",
            f"Task count: {details.get('task_count', 0)}",
            "Task state counts:",
        ]

        for state, count in details.get(
            "task_state_counts", {}
        ).items():
            lines.append(f"- {state}: {count}")

        lines.extend(["", "Task details:"])

        for task in details.get("tasks", []):
            lines.append(
                f"- {task.get('task_id')}: "
                f"{task.get('state')} "
                f"(attempt: {task.get('try_number')})"
            )

        return "\n".join(lines)

    lines = [
        f"Recent runs for DAG: {dag_id}",
        "",
    ]

    for run in runs:
        lines.append(
            f"- Run ID: {run.get('dag_run_id')} | "
            f"State: {run.get('state')} | "
            f"Start: {run.get('start_date')}"
        )

    return "\n".join(lines)

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
# AIRFLOW READ-ONLY API ENDPOINTS
# --------------------------------------------------

@app.get("/api/v1/airflow/dags")
def airflow_list_dags(
    limit: int = Query(default=100, ge=1, le=100)
):
    try:
        return AirflowService().list_dags(limit=limit)
    except (RuntimeError, ValueError) as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc


@app.get("/api/v1/airflow/dags/{dag_id}/runs")
def airflow_list_runs(
    dag_id: str,
    limit: int = Query(default=10, ge=1, le=50),
):
    try:
        return AirflowService().list_runs(
            dag_id=dag_id,
            limit=limit,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc


@app.get("/api/v1/airflow/dags/{dag_id}/runs/{dag_run_id}")
def airflow_run_details(dag_id: str, dag_run_id: str):
    try:
        return AirflowService().get_run_details(
            dag_id=dag_id,
            dag_run_id=dag_run_id,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc


# --------------------------------------------------
# CHAT ENDPOINT
# --------------------------------------------------

@app.post(
    "/api/v1/chat",
    response_model=ChatResponse,
    summary="DataPilot AI Agentic RAG Chat",
    description=(
        "Routes read-only Airflow operational questions to Airflow "
        "and documentation questions to Pinecone RAG."
    ),
)
def chat(request: ChatRequest):

    request_id = f"req_{uuid4().hex[:12]}"
    execution_id = f"exec_{uuid4().hex[:12]}"
    start_time = time.perf_counter()

    try:
        # STEP 1: DETECT AIRFLOW AND SUPERVISOR INTENTS

        supervisor_requested = detect_supervisor_intent(request.question)
        airflow_intent = detect_airflow_intent(request.question)

        # An explicit assessment request should use the Supervisor route,
        # even if the phrase does not match a Stage 1 Airflow keyword.
        if supervisor_requested:
            airflow_intent = "dag_runs"

        if airflow_intent:
            airflow = AirflowService()

            if airflow_intent == "list_dags":
                answer = format_dag_list(airflow)
                components = ["AirflowIntentRouter", "AirflowService", "AirflowClient"]
                orchestration_status = "not_integrated"
                selected_agent = None
                agents_invoked = []
                supervisor_name = None
            else:
                dag_id = find_dag_id(request.question, airflow)

                if not dag_id:
                    answer = (
                        "I identified an Airflow operational question, "
                        "but could not identify a DAG ID in your request. "
                        "Please include the exact DAG ID. You can ask "
                        "'List all Airflow DAGs' to discover available DAGs."
                    )
                    duration_ms = int((time.perf_counter() - start_time) * 1000)
                    return build_chat_response(
                        request_id=request_id,
                        execution_id=execution_id,
                        question=request.question,
                        answer=answer,
                        duration_ms=duration_ms,
                        components=["AirflowIntentRouter", "AirflowService"],
                        grounded=False,
                        routing_reason=(
                            "supervisor_assessment_intent"
                            if supervisor_requested
                            else "airflow_operational_intent"
                        ),
                        orchestration_status="not_integrated",
                    )

                if supervisor_requested:
                    requested_run_id = extract_requested_run_id(request.question)

                    supervisor_result = execute_supervisor_assessment(
                        airflow=airflow,
                        dag_id=dag_id,
                        requested_run_id=requested_run_id,
                    )
                    answer = format_supervisor_assessment(supervisor_result)
                    components = [
                        "AirflowIntentRouter",
                        "AirflowService",
                        "AirflowClient",
                        "DataPilotSupervisor",
                    ]
                    orchestration_status = "completed"
                    selected_agent = "Supervisor"
                    agents_invoked = ["Supervisor"]
                    supervisor_name = "DataPilot Supervisor"
                else:
                    answer = format_dag_runs(
                        request.question,
                        airflow,
                        dag_id,
                    )
                    components = [
                        "AirflowIntentRouter",
                        "AirflowService",
                        "AirflowClient",
                    ]
                    orchestration_status = "not_integrated"
                    selected_agent = None
                    agents_invoked = []
                    supervisor_name = None

            duration_ms = int((time.perf_counter() - start_time) * 1000)

            return build_chat_response(
                request_id=request_id,
                execution_id=execution_id,
                question=request.question,
                answer=answer,
                duration_ms=duration_ms,
                components=components,
                grounded=True,
                routing_reason=(
                    "supervisor_assessment_intent"
                    if supervisor_requested
                    else "airflow_operational_intent"
                ),
                orchestration_status=orchestration_status,
                selected_agent=selected_agent,
                agents_invoked=agents_invoked,
                supervisor=supervisor_name,
            )

        # STEP 2: DOCUMENTATION QUESTIONS USE EXISTING RAG

        retriever = RAGRetriever(
            top_k=5,
            candidate_k=20,
            similarity_threshold=0.40,
            strategy="mmr",
            mmr_lambda=0.70,
            max_chunks_per_document=1,
        )

        retrieved_chunks = retriever.retrieve(request.question)

        generator = LLMResponseGenerator()

        result = generator.generate_response(
            question=request.question,
            retrieved_chunks=retrieved_chunks,
        )

        duration_ms = int(
            (time.perf_counter() - start_time) * 1000
        )

        sources = result.get("sources", [])

        return build_chat_response(
            request_id=request_id,
            execution_id=execution_id,
            question=request.question,
            answer=result["answer"],
            duration_ms=duration_ms,
            components=[
                "EmbeddingService",
                "PineconeVectorStore",
                "RAGRetriever",
                "LLMResponseGenerator",
            ],
            grounded=result["grounded"],
            sources=sources,
            rag_enabled=True,
            retrieval_status=(
                "success" if retrieved_chunks else "no_results"
            ),
            results_found=len(retrieved_chunks),
            top_k=retriever.top_k,
            similarity_threshold=retriever.similarity_threshold,
            routing_reason="documentation_rag_intent",
            llm_model=result.get("model"),
            llm_response_id=result.get("response_id"),
            usage=result.get("usage"),
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


# --------------------------------------------------
# MCP READ-ONLY REMEDIATION AUDIT API
# --------------------------------------------------

def _parse_mcp_tool_result(result: dict[str, Any]) -> Any:
    """Convert an MCP CallToolResult wrapper into its returned payload."""

    if result.get("is_error"):
        error_text = " ".join(
            item.get("text", "")
            for item in result.get("content", [])
            if item.get("type") == "text"
        ).strip()
        raise RuntimeError(error_text or "MCP tool returned an error.")

    structured_content = result.get("structured_content")
    if structured_content is not None:
        return structured_content

    text_parts = [
        item.get("text", "")
        for item in result.get("content", [])
        if item.get("type") == "text" and item.get("text")
    ]

    if not text_parts:
        return {"content": result.get("content", [])}

    text_payload = "\n".join(text_parts)
    try:
        return json.loads(text_payload)
    except json.JSONDecodeError:
        return {"text": text_payload}


@app.get(
    "/api/v1/remediation/audit",
    summary="Read remediation execution audit history",
    description=(
        "Read-only access to remediation audit records through the MCP server. "
        "Provide either dataset or execution_id. This endpoint does not approve "
        "or execute remediation."
    ),
)
async def remediation_audit(
    dataset: str = Query(default="", max_length=128),
    execution_id: str = Query(default="", max_length=256),
    limit: int = Query(default=10, ge=1, le=20),
):
    """Fetch remediation audit records using the read-only MCP tool."""

    dataset = dataset.strip()
    execution_id = execution_id.strip()

    if not dataset and not execution_id:
        raise HTTPException(
            status_code=400,
            detail="Provide either the dataset or execution_id query parameter.",
        )

    try:
        mcp = MCPService()
        tool_result = await mcp.call_tool(
            "get_remediation_audit",
            {
                "dataset": dataset,
                "execution_id": execution_id,
                "limit": limit,
            },
        )
        return _parse_mcp_tool_result(tool_result)

    except RuntimeError as exc:
        raise HTTPException(
            status_code=502,
            detail={
                "error": "MCP_TOOL_ERROR",
                "message": str(exc),
            },
        ) from exc
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(
            status_code=503,
            detail={
                "error": "MCP_SERVICE_UNAVAILABLE",
                "message": str(exc),
            },
        ) from exc
