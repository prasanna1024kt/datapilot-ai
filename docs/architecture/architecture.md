# DataPilot AI – End-to-End Architecture

## 1. Project Overview

DataPilot AI is an AI-assisted data engineering and DataOps platform. It demonstrates data pipeline orchestration, distributed processing, data quality validation, root-cause analysis, controlled remediation, operational supervision, and a planned RAG-powered AI Data Engineering Copilot.

The current data engineering workflows use the Olist e-commerce dataset. The platform combines cloud data engineering services with Python-based AI agents and Model Context Protocol (MCP) integration.

## 2. Architecture Objectives

- Build a configuration-driven data engineering platform.
- Implement Bronze, Silver, and Gold data processing.
- Support reusable PySpark transformations.
- Implement configurable data quality checks.
- Automate root-cause analysis through an RCA agent.
- Generate remediation plans through a remediation agent.
- Enforce human approval before authorized data writes.
- Maintain execution history and audit records.
- Use a Supervisor agent to assess pipeline and remediation outcomes.
- Develop a RAG-powered AI Data Engineering Copilot.

## 3. High-Level Architecture

The platform is organized into these logical layers:

1. Data storage: Amazon S3.
2. Data processing: Apache Spark and PySpark.
3. Orchestration: Apache Airflow.
4. Catalog and table format: AWS Glue Data Catalog and Apache Iceberg.
5. Data quality: configurable PySpark DQ framework.
6. AI agents: RCA, Remediation, and Supervisor.
7. Tool integration: MCP server.
8. Observability: LangSmith.
9. AI Copilot: FastAPI, retrieval, Pinecone, and LLM integration (under development).

The existing pipeline, agent, and remediation workflows are implemented. The Copilot/RAG service has foundational modules but is not yet an end-to-end operational capability.

## 4. Infrastructure and Technology Stack

| Layer | Technology | Responsibility | Status |
|---|---|---|---|
| Cloud storage | Amazon S3 | Raw and processed data storage | Implemented |
| Processing | Apache Spark / PySpark | Distributed data processing | Implemented |
| Orchestration | Apache Airflow | DAG scheduling and task coordination | Implemented |
| Catalog | AWS Glue Data Catalog | Iceberg table metadata | Implemented |
| Table format | Apache Iceberg | Table storage and transactional operations | Implemented |
| Data quality | Custom PySpark framework | Configurable DQ validation | Implemented |
| AI agents | Python | RCA, remediation, and supervision | Implemented |
| Tool integration | MCP | Agent access to platform capabilities | Implemented |
| Observability | LangSmith | Agent and LLM tracing | Implemented |
| API layer | FastAPI | Copilot API | Foundation created |
| Vector database | Pinecone | Knowledge retrieval | Integration pending |
| LLM integration | OpenAI | Embeddings and response generation | Integration pending |

## 5. Data Storage Architecture

The project uses the S3 bucket:

`datapilot-ai-data-practice`

Configured storage layout:

```text
s3://datapilot-ai-data-practice/
├── raw/
└── processed/
    ├── bronze/
    ├── silver/
    ├── gold/
    └── quarantine/
```

Storage paths and formats are centrally configured in `config/variables.yaml`.

Configured formats:
- Raw: CSV
- Bronze: Parquet
- Silver: Parquet
- Gold: Parquet

The quarantine workflow uses an Iceberg table registered through AWS Glue Data Catalog. Its dataset-specific configuration is maintained under `config/quarantine/`.

## 6. Data Engineering Architecture

### 6.1 Orchestration

Apache Airflow coordinates the data pipeline and remediation workflows.

The main pipeline DAG ID is `datapilot_olist_order_pipeline`.

The pipeline coordinates:
- Bronze processing
- Silver processing
- Data quality validation
- RCA execution when the relevant DQ failure condition is met
- Gold processing
- Gold business summary
- Supervisor assessment

Separate DAGs implement the remediation and approval workflows:
- `datapilot_olist_data_remediation`
- `datapilot_remediation_approval`

### 6.2 Reusable Spark Framework

The reusable Spark framework is located under `spark/framework/`.

| Component | Responsibility |
|---|---|
| `ConfigLoader` | Loads centralized variables and layer-specific YAML configuration |
| `PathBuilder` | Constructs raw and layer-specific S3 paths |
| `DataReader` | Reads data using configured Spark formats and options |
| `DataTransformer` | Applies reusable DataFrame transformations |
| `DataWriter` | Writes DataFrames using configured formats and write modes |
| `DataPipeline` | Coordinates configuration-driven source, transformation, and target processing |
| `DQEngine` | Executes configured data quality checks |

The generic `DataPipeline` resolves source and target paths and formats from configuration. It applies transformations when transformation configuration is supplied. The DQ framework is a separate component.

### 6.3 Bronze Layer

The Bronze layer stores source data in Parquet format under the configured processed-data structure. The configuration-driven Spark pipeline resolves the raw input path and Bronze target path using centralized variables and dataset configuration.

### 6.4 Silver Layer

The Silver layer contains transformed datasets. The reusable `DataTransformer` supports:
- Column-name standardization
- String trimming
- Data type casting
- Timestamp conversion
- Duplicate removal
- Null handling
- Row filtering
- Column removal

The actual transformations applied depend on each dataset's configuration.

### 6.5 Data Quality Framework

The DQ framework is implemented under `spark/framework/dq/`.

Supported checks:
- Row-count validation
- Required-column validation
- Null checks
- Duplicate checks
- Range checks

Dataset-level DQ configurations are maintained under `config/dq/`.

The `spark/jobs/run_dq.py` job reads Silver Parquet data, executes configured checks, and writes a JSON report under `results/dq/`. A failed DQ result raises an error so Airflow can record the task failure.

### 6.6 Gold Layer

Gold processing uses Apache Iceberg and AWS Glue Data Catalog.

The project contains two distinct Gold workflows:

1. **Generic incremental Silver-to-Gold loading:** `spark/jobs/gold_incremental_iceberg.py`, driven by `config/gold_tables.yaml`. It validates configured business keys and uses Iceberg MERGE operations.
2. **Business-level order summary:** `spark/jobs/run_gold.py` and `spark/framework/business_transformer.py`. This workflow builds the `olist_order_summary` analytical dataset from orders, order items, payments, and customers.

The business transformer aggregates item and payment measures by order before joining them to order-level data. This avoids multiplying order rows because of one-to-many item and payment relationships.

The two Gold workflows have separate configuration and execution paths. Their warehouse and storage locations should be checked against the deployed environment before changing either configuration.

## 7. AI Agent Architecture

DataPilot AI currently implements three major agents.

### 7.1 RCA Agent

Location: `agents/rca/`

Responsibilities:
- Analyze data quality failures.
- Use available DQ evidence.
- Identify potential root causes.
- Produce structured RCA results.
- Recommend corrective or remediation actions.

The RCA workflow is invoked by the pipeline when its DQ failure conditions are met. The Supervisor's Airflow DAG assessment collects evidence; it does not itself invoke the RCA agent.

### 7.2 Remediation Agent

Location: `agents/remediation/`

Responsibilities:
- Generate remediation plans.
- Identify the proposed remediation action and scope.
- Require approval for controlled execution.
- Generate and validate plan fingerprints.
- Coordinate approved remediation execution.
- Maintain execution history and audit records.

The current quarantine execution path is deliberately restricted to the supported dataset and action described in the remediation workflow documentation.

### 7.3 Supervisor Agent

Location: `agents/supervisor/`

Responsibilities:
- Assess pipeline execution outcomes.
- Collect task, DQ, and RCA evidence.
- Evaluate remediation workflow outcomes.
- Produce structured operational decisions.

The Supervisor assesses outcomes; it is not the component that independently authorizes or executes remediation writes.

## 8. Remediation and Governance

The remediation framework uses a human-in-the-loop execution model.

Current quarantine workflow:

```text
DQ Failure
    |
    v
RCA Analysis
    |
    v
Remediation Plan
    |
    v
Human Approval
    |
    v
Fingerprint and Scope Validation
    |
    v
DRY_RUN
    |
    v
Explicit APPLY Authorization
    |
    v
Iceberg Quarantine Write
    |
    v
Post-commit Verification
    |
    v
Execution Audit
    |
    v
Gold Refresh and Supervisor Assessment
```

The current `QUARANTINE_RECORDS` implementation supports `olist_order_payments`.

Invalid payment-installment records are written to the configured quarantine Iceberg table. The quarantine writer preserves the source Silver data. Gold cleanup and refresh are separate downstream operations.

A separate `SOURCE_CORRECTION` mechanism exists for approved Bronze corrections. It is distinct from the quarantine workflow.

Detailed workflow documentation: `docs/operations/remediation-workflow.md`.

## 9. MCP Integration

The MCP server is implemented under `mcp_server/` and deployed as a separate Docker Compose service.

It exposes platform capabilities to connected agents through the Model Context Protocol. The MCP service acts as a tool integration layer between agents and supported platform operations.

Within the Docker Compose network, services can address the MCP server by its service name. The configured MCP endpoint for Copilot settings is `http://mcp-server:8000/mcp`.

The exact tools exposed by the server should be documented from the current MCP server implementation and tool definitions.

## 10. Observability

LangSmith tracing is integrated with the existing agent workflows.

The RCA and Supervisor workflows use tracing to support inspection of agent execution. The existing RCA tracing project is `datapilot-ai-rca`.

The Copilot settings define a separate configurable LangSmith project name for future Copilot tracing.

## 11. AI Data Engineering Copilot and RAG

The Copilot is a separate service under development. It is intended to provide a conversational interface over DataPilot's technical knowledge and approved platform tools.

Planned architecture:
- FastAPI API layer
- Configuration-driven model settings
- Document ingestion
- Text chunking
- Embedding generation
- Pinecone vector database
- Retrieval service
- MCP tool integration
- LLM-based response generation
- LangSmith tracing

### 11.1 Knowledge Sources

Planned knowledge sources:
- Architecture documentation
- Pipeline documentation
- Spark framework documentation
- Data quality configurations
- Agent documentation
- Remediation workflow documentation
- Relevant historical RCA reports

Operational outputs, credentials, and sensitive execution artifacts must not be indiscriminately ingested.

### 11.2 RAG Processing Flow

```text
Technical Documents
       |
       v
Document Loader
       |
       v
Text Chunking
       |
       v
Embedding Generation
       |
       v
Pinecone Vector Store
       |
       v
Question Embedding
       |
       v
Similarity Retrieval
       |
       v
Retrieved Context
       |
       v
LLM Response Generation
       |
       v
Grounded Answer with Sources
```

The current codebase has foundational document-loader, chunking, embedding-service, vector-store, retriever, and ingestion-pipeline modules. Unit tests use mocked external services.

The presence of these modules does not mean that live Pinecone ingestion or an end-to-end RAG query has been completed. Actual external ingestion, index verification, and full Copilot orchestration remain to be completed.

RAG evaluation is intentionally deferred until the working RAG integration is complete.

### 11.3 Intended Agentic Copilot Flow

1. A user submits a data engineering question.
2. The Copilot retrieves relevant technical knowledge.
3. The orchestrator identifies the required operation.
4. MCP tools provide access to approved platform capabilities.
5. The LLM generates a grounded explanation or recommendation.
6. Write operations require explicit authorization.
7. The Copilot returns the result with supporting evidence.

## 12. Security and Operational Controls

Current implementation controls include:
- Explicit approval before remediation APPLY.
- Plan fingerprint validation.
- Execution-scope validation.
- Dataset and action restrictions.
- Source preservation during quarantine.
- Iceberg target validation.
- Post-commit record verification.
- Atomic execution audit creation.
- Conservative handling of uncertain write outcomes.

Credentials are maintained outside source-controlled configuration. Secrets should not be placed in documentation, source code, or committed configuration files.

External service calls and persistent writes should be performed only through explicitly authorized workflows.

## 13. Repository Structure

Major project directories include:

```text
DataPilot AI/
├── agents/
│   ├── rca/
│   ├── remediation/
│   └── supervisor/
├── config/
│   ├── bronze/
│   ├── silver/
│   ├── dq/
│   ├── gold/
│   └── quarantine/
├── copilot/
│   ├── api/
│   ├── config/
│   └── rag/
│       ├── ingestion/
│       └── retrieval/
├── dags/
├── docs/
│   ├── architecture/
│   ├── agents/
│   ├── operations/
│   └── pipeline/
├── mcp_server/
├── spark/
│   ├── framework/
│   └── jobs/
├── tests/
└── docker-compose.yml
```

## 14. Implementation Status

### Implemented

- Configuration-driven Spark processing framework.
- Bronze, Silver, and Gold processing workflows.
- Airflow pipeline orchestration.
- Configurable data quality framework.
- RCA agent and its current v1 hardening.
- Remediation agent and approval-controlled quarantine workflow.
- Iceberg quarantine APPLY and post-commit verification.
- Supervisor agent.
- MCP integration.
- LangSmith tracing for existing agent workflows.

### Foundation Created; Integration Pending

- Copilot FastAPI application structure.
- Copilot configuration and settings validation.
- Document loading and chunking.
- Embedding service abstraction.
- Pinecone vector-store abstraction.
- Retrieval service.
- Ingestion pipeline with dry-run protection.
- Mocked unit tests for the RAG foundation.

### Remaining Work

- Complete technical knowledge-base documentation.
- Create and verify the Pinecone index.
- Perform authorized live document embedding and ingestion.
- Verify end-to-end retrieval against the live vector store.
- Implement LLM response generation with retrieved context and source references.
- Implement Copilot orchestration and MCP tool integration.
- Add safe conversational controls for read and write operations.
- Complete RAG evaluation after the working RAG integration is operational.

## 15. Architecture Summary

DataPilot AI combines a configuration-driven data engineering platform with AI-assisted DataOps capabilities.

Its implemented foundation provides pipeline orchestration, distributed processing, data quality validation, root-cause analysis, approval-controlled remediation, MCP-based tool integration, and operational supervision.

The next major development phase is completing the RAG-powered AI Data Engineering Copilot and integrating it safely with the existing MCP tools and agents.
