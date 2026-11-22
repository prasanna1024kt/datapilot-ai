# DataPilot AI – End-to-End Architecture

## 1. Project overview

DataPilot AI is an AI-assisted data engineering and DataOps portfolio platform. It brings together configuration-driven data processing, data-quality validation, root-cause analysis, controlled remediation, operational supervision, and a retrieval-augmented Data Engineering Copilot.

The project uses the Olist e-commerce dataset for its data engineering workflows. Its architecture combines Apache Airflow, Apache Spark/PySpark, a reusable data-processing framework, Python agents, a Model Context Protocol (MCP) server, FastAPI, Pinecone, and OpenAI services.

The portfolio is currently focused on local execution and demonstrable engineering workflows. AWS deployment is deferred; S3 and AWS Glue remain part of the cloud-oriented target architecture and configuration, not evidence of a completed cloud deployment.

## 2. Architecture objectives

- Provide configuration-driven Bronze, Silver, and Gold data processing.
- Orchestrate data workflows with Airflow and process data with Spark/PySpark.
- Apply reusable data-quality rules and produce machine-readable results.
- Investigate data-quality failures through an RCA workflow.
- Generate remediation plans with fingerprints and human approval controls.
- Keep remediation execution bounded by explicit dataset, action, mode, and approval checks.
- Assess pipeline and remediation outcomes with the Supervisor Agent.
- Expose selected platform capabilities through MCP tools.
- Provide knowledge ingestion, vector retrieval, and LLM-generated responses through the Copilot API.
- Preserve test evidence, operational documentation, and known limitations for technical review.

## 3. Logical architecture

1. **Data processing:** Apache Spark and PySpark.
2. **Orchestration:** Apache Airflow.
3. **Data quality:** reusable configurable PySpark DQ framework.
4. **Table and storage architecture:** Apache Iceberg, with AWS S3 and Glue in the cloud-oriented configuration.
5. **Agents:** RCA, Remediation, and Supervisor.
6. **Tool integration:** MCP server and selected platform tools.
7. **Copilot API:** FastAPI application.
8. **Knowledge retrieval:** document loading/chunking, embedding generation, Pinecone vector storage, and retrieval.
9. **LLM response generation:** OpenAI integration using retrieved context.
10. **Observability:** LangSmith tracing for the agent workflows where tracing is configured. Do not assume Copilot tracing is active unless verified separately.

## 4. Technology and implementation status

| Layer | Technology | Current evidence-based status |
|---|---|---|
| Processing | Spark / PySpark | Implemented; project tests and local services have been exercised |
| Orchestration | Airflow | Implemented; DAGs and local Compose services exist |
| Data quality | Custom PySpark framework | Implemented; tests and DQ workflow documentation exist |
| Table format | Apache Iceberg | Implemented for the documented Gold/quarantine workflows; cloud catalog/storage configuration remains environment-dependent |
| Storage/catalog | S3 / AWS Glue | Configured target architecture; AWS deployment is deferred |
| RCA Agent | Python | Implemented; v1 hardening reported complete |
| Remediation Agent/executor | Python / Spark / Iceberg | Implemented with restricted DRY_RUN/APPLY paths and approval/fingerprint checks; see limitations |
| Supervisor Agent | Python | Implemented; reported complete |
| MCP | MCP server | Implemented with read and remediation-related tools; tool-level controls and authentication limitations are documented separately |
| Copilot API | FastAPI | Implemented; API and component tests exist |
| Vector retrieval | Pinecone | Integration exercised; 316 vectors were reported ingested in the `datapilot-ai` namespace of `datapilot-ai-knowledge` |
| Embeddings and generation | OpenAI | Ingestion and response-generation paths have been exercised; verify current model/settings before quoting model-specific details |
| Observability | LangSmith | Tracing is documented for existing agent workflows; do not claim end-to-end Copilot tracing without fresh evidence |

**Status interpretation:** “Implemented” means code and supporting tests or execution evidence exist. It does not mean every path has production-grade security, load, resilience, or end-to-end validation.

## 5. Data engineering architecture

### 5.1 Orchestration

Airflow coordinates the pipeline and remediation workflows. The existing documentation identifies these DAGs:

- `datapilot_olist_order_pipeline`
- `datapilot_olist_data_remediation`
- `datapilot_remediation_approval`

The main pipeline includes Bronze and Silver processing, DQ validation, conditional RCA, Gold processing, business summary generation, and Supervisor assessment. Remediation and approval workflows are separate from the main pipeline.

### 5.2 Reusable Spark framework

The reusable Spark framework is under `spark/framework/`. Its documented components include configuration loading, path construction, reading, transformation, writing, pipeline coordination, and DQ execution. Dataset behavior is configuration-dependent.

### 5.3 Bronze and Silver

The Bronze layer stores ingested source datasets in the configured target format and path. The reusable Spark framework resolves input and output locations using centralized variables and dataset-level configuration.

The Silver layer applies dataset-specific transformations through the reusable `DataTransformer` framework.

Documented transformation capabilities include:

- Column-name standardization.
- String trimming.
- Data type casting.
- Timestamp conversion.
- Duplicate removal.
- Null handling.
- Row filtering.
- Column removal.

The actual transformations applied depend on each dataset's configuration and the corresponding job implementation.

### 5.4 Data quality

The DQ framework supports row-count, required-column, null, duplicate, and range checks. The DQ job writes machine-readable reports under `results/dq/`. A failed DQ result can fail the corresponding Airflow task and provide evidence for RCA.

### 5.5 Gold

The Gold layer uses Apache Iceberg tables and contains two distinct processing paths.

**1. Incremental Iceberg processing**

The incremental processing workflow uses `spark/jobs/gold_incremental_iceberg.py` and `config/gold_tables.yaml`.

Its implementation includes:

- Dataset-specific configuration.
- Business-key validation.
- Null and duplicate key checks.
- Iceberg table existence checks.
- Incremental MERGE operations.
- Quarantine-related validation for the supported payment workflow.

**2. Olist order-summary processing**

The business-summary workflow uses `spark/jobs/run_gold.py` and `spark/framework/business_transformer.py`.

The `BusinessTransformer` builds an order-level analytical dataset by:

1. Aggregating order items by `order_id` to calculate product count, total price, and freight value.
2. Aggregating payment values by `order_id`.
3. Selecting customer attributes such as city and state.
4. Joining the aggregated item and payment results with orders and customer attributes.
5. Selecting the final Gold business columns and handling missing aggregate values.

Aggregating the one-to-many item and payment datasets before joining helps avoid multiplying order-level records during the joins.

The resulting `olist_order_summary` table is written using Apache Iceberg format version 2. Existing tables are updated through an Iceberg MERGE operation.

The two Gold paths have separate configurations and execution behavior. Their active catalog, warehouse, and storage settings must be checked for the target environment.

## 6. Agent architecture and responsibility boundaries

### 6.1 RCA Agent

The RCA Agent consumes DQ failure evidence, investigates supported conditions, and writes a structured RCA result. Its current v1 hardening is reported complete.

### 6.2 Remediation Agent and executor

The Remediation Agent validates an RCA result and creates a fingerprinted plan, initially marked `PENDING_APPROVAL` and `DRY_RUN`. The downstream approval workflow and executor enforce additional scope and execution checks.

The documented executor supports a restricted quarantine workflow. APPLY requires explicit confirmation and a separate APPLY-scoped approval artifact. The MCP approval flow described in the tool documentation is DRY_RUN-scoped; do not imply it grants APPLY authorization.

### 6.3 Supervisor Agent

The Supervisor assesses pipeline, DQ, RCA, and remediation evidence. It produces structured assessments and recommended next actions. It does not itself approve remediation or execute data writes.

## 7. Copilot and RAG architecture

The Copilot is composed of:

- FastAPI endpoints and configuration.
- Document loading and chunking.
- Embedding generation.
- Pinecone vector storage and retrieval.
- LLM response generation using retrieved context.

A prior ingestion run reported 14 documents, 316 chunks, 316 embeddings, and 316 vectors upserted. The Pinecone index was reported as `datapilot-ai-knowledge`, dimension 1536, namespace `datapilot-ai`. These are historical execution results; verify current index statistics before presenting them as live state.

A retrieval benchmark compared Similarity, MMR, and Multi-Query on 16 questions. The experiment found different tradeoffs between recall and ranking quality. It did not establish production latency/cost or complete generated-answer quality for every strategy. See `docs/copilot/rag-evaluation.md` (proposed companion document).

The Copilot and RAG integration is completed and has been tested end-to-end. The operational path includes document ingestion and chunking, embedding generation, Pinecone vector upsert and retrieval, and LLM response generation through the Copilot API. Describe this as a completed end-to-end Copilot/RAG capability. Keep autonomous MCP/agent orchestration as a separate claim: only describe that additional orchestration as integrated if the specific workflow has also been exercised end-to-end.

## 8. Human approval and remediation governance

The remediation flow separates diagnosis, plan creation, approval, and execution:

```text
DQ evidence
   -> RCA result
   -> fingerprinted remediation plan
   -> human approval/rejection record
   -> scope and fingerprint validation
   -> restricted executor
   -> execution result and audit evidence
```

A pending plan is not authorization. The executor must validate the applicable approval artifact, plan fingerprint, action, dataset, execution mode, and other configured safeguards. APPLY must not be demonstrated as a routine portfolio walkthrough; use read-only or DRY_RUN evidence unless a separately authorized test environment and approval are explicitly arranged.
### Remediation action scope

The currently supported MCP remediation execution path is restricted to `QUARANTINE_RECORDS` for `olist_order_payments`.

The executor contains a separate `SOURCE_CORRECTION` action branch. This should not be presented as an equivalent, fully integrated MCP remediation workflow without separate evidence of its approval and execution path.

The quarantine workflow writes invalid payment records to the configured Iceberg quarantine target and includes record-verification safeguards. Source preservation and downstream Gold processing are separate concerns.

## 9. MCP integration

The MCP server is implemented under `mcp_server/` and runs as a separate Docker Compose service.

It exposes selected platform capabilities through the Model Context Protocol, including:

- Data-quality and dataset inspection.
- Remediation-plan and approval operations.
- Remediation execution within the supported scope.
- Operational audit retrieval.

The Copilot configuration defines the internal MCP endpoint as:

`http://mcp-server:8000/mcp`

This endpoint uses the Docker Compose service name and is intended for communication within the local Compose network.

The presence of MCP tools does not mean every Copilot or agent workflow is automatically orchestrated through MCP. Describe individual integrations as completed only where the corresponding workflow has been exercised.

## 10. Observability

LangSmith tracing is present in the RCA and Supervisor agent implementations.

The Copilot settings define a separate LangSmith project configuration, but tracing is disabled by default (`langsmith_tracing=False`). Do not claim active Copilot tracing without separate execution evidence.

Observability capabilities should therefore be described by component rather than as one universally integrated tracing layer.

## 11. Repository structure

Major project components:

- `agents/` – RCA, remediation, and Supervisor agents.
- `config/` – centralized pipeline, DQ, Gold, and quarantine configuration.
- `copilot/` – FastAPI application, RAG ingestion, retrieval, and evaluation.
- `dags/` – Airflow DAG definitions.
- `docs/` – architecture, agent, pipeline, and operational documentation.
- `mcp_server/` – MCP server and tool implementations.
- `spark/framework/` – reusable Spark processing and DQ components.
- `spark/jobs/` – Bronze, Silver, Gold, and related processing jobs.
- `tests/` – automated component and workflow tests.
- `docker-compose.yml` – local development service orchestration.

## 12. Implementation summary

The project has implemented its core data engineering pipeline, DQ framework, RCA workflow, controlled remediation path, Supervisor Agent, MCP service, and Copilot/RAG capability. The end-to-end data pipeline, end-to-end Copilot/RAG integration, Supervisor Agent, and RCA v1 hardening are completed milestones; Copilot/RAG integration has been tested end-to-end.

The current portfolio closure focus is documentation accuracy, reproducible local demonstration, validation evidence, and explicit communication of security and evaluation limitations. AWS deployment is intentionally outside the current scope.
