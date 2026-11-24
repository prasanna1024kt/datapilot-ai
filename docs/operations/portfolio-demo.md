# DataPilot AI – Local Portfolio Demo and Walkthrough



## 1. Demo objective

Demonstrate how DataPilot AI connects data engineering workflows with governed AI-assisted operations:

1. Configuration-driven data processing.
2. Data-quality validation.
3. RCA investigation.
4. Fingerprinted remediation planning and approval boundaries.
5. Supervisor assessment.
6. Knowledge ingestion, retrieval, and Copilot response generation.
7. Local Copilot latency, token-usage, and estimated-cost observability.

The demo should show clear component boundaries and evidence, not imply unrestricted autonomous execution.

## 2. Safety rules

- Use a local development environment and non-production data.
- Do not run AWS provisioning or deployment as part of this walkthrough.
- Do not trigger APPLY or modify source data.
- Prefer read-only health/API checks and DRY_RUN paths.
- Do not approve a real remediation request merely to make a demo succeed.
- Sanitize logs, screenshots, API responses, and configuration output before sharing.
- Never display API keys, passwords, tokens, or credential files.

## 3. Suggested walkthrough order

### Step 1 – Explain the architecture

Show `docs/architecture/architecture.md` and the existing diagrams:

- `images/pipeline-orchestration.png`
- `images/agent-interactions.png`
- `images/gold-processing-overview.png`
- `images/gold-incremental-iceberg-flow.png`
- `images/airflow_olist.png`

Explain the difference between the local Compose environment and the AWS-oriented target architecture.

### Step 2 – Show the local platform

Use the existing deployment runbook to start or inspect the local environment. Do not copy commands into this document until they have been verified against the current Compose file and local setup.

Show only the services needed for the demonstration. Confirm service health before proceeding. A healthy service does not prove that all workflows are integrated.

### Step 3 – Demonstrate the data pipeline

Show the Airflow DAG and explain the Bronze, Silver, and Gold responsibilities.

Use historical execution evidence where appropriate:
- Bronze and Silver tasks completed successfully.
- Selected Gold incremental Iceberg tasks completed successfully.
- The payment DQ task detected two invalid records.

Explain that the historical payment DQ failure is used to demonstrate the investigation workflow. The corresponding RCA evidence identifies the invalid payment-installment values.

Distinguish successful individual tasks from overall DAG completion. Do not present the historical DAG run as a fully successful pipeline execution.

### Step 4 – Demonstrate data quality and RCA

Show a known DQ report and its checks. If using a historical failure artifact, label it as historical. Show the RCA JSON and explain which evidence supports the root-cause conclusion.

Avoid manufacturing a new failure in a shared or production-like environment solely for presentation.

### Step 5 – Demonstrate remediation governance

Show a candidate remediation plan with its fingerprint, `PENDING_APPROVAL` status, and `DRY_RUN` mode. Explain the separate approval record, fingerprint/scope validation, and the executor's restrictions.

The demo stops before APPLY. A pending plan is not approval, and a DRY_RUN approval does not authorize APPLY.

### Step 6 – Demonstrate Supervisor assessment

Show a Supervisor assessment artifact and explain the evidence it consumes: Airflow task states, DQ reports, RCA artifacts, and remediation task outcomes. Emphasize that Supervisor assessment is not remediation authorization.

### Step 7 – Demonstrate Copilot and RAG

Show the knowledge ingestion summary, Pinecone namespace/index details if currently available, and one Copilot question with its retrieved source references.

Explain the distinction between:

- document ingestion and embedding;
- vector retrieval;
- LLM response generation;
- any additional agent/MCP orchestration, which should only be claimed if the specific path is currently verified.

### Step 8 – Demonstrate Copilot observability

Show the Streamlit observability dashboard and explain the local instrumentation for the RAG API request path:
- End-to-end request and RAG-stage latency.
- Prompt, completion, and embedding token usage where available.
- Estimated embedding and LLM input/output costs.
- Historical trends, recent requests, top-cost requests, and data-completeness indicators.

Use the documented API commands and observed baseline in [Latency, Token Usage and Cost Observability](observability-latency-token-cost.md). Explain that costs are estimates based on configured model pricing, not provider billing. The V1 dashboard does not include Airflow or Supervisor execution metrics, and AWS infrastructure costs are outside scope.

### Step 9 – Close with validation and limitations

Present the testing report, RAG evaluation findings, observability evidence, and security/governance limitations. State explicitly that AWS deployment is deferred and that the project is a locally demonstrable portfolio.

## 4. Suggested 10-minute interview narrative

| Time | Topic | Main message |
|---|---|---|
| 0–1 min | Problem and architecture | AI-assisted DataOps with governed actions |
| 1–3 min | Pipeline | Configuration-driven Spark and Airflow processing |
| 3–5 min | DQ, RCA, remediation | Evidence-based diagnosis and approval boundaries |
| 5–6 min | Supervisor | Operational assessment across workflow evidence |
| 6–8 min | Copilot/RAG | Knowledge ingestion, Pinecone retrieval, and LLM response |
| 8–9 min | Observability | RAG latency, token usage, estimated cost, and dashboard limitations |
| 9–10 min | Engineering tradeoffs | Evaluation results, limitations, security, and local/cloud boundary |

The timings are presentation targets. Allow additional time when showing live API responses, logs, or detailed technical explanations.

## 5. Evidence checklist

Before presenting, prepare sanitized screenshots or outputs for:

- Architecture diagram and local service topology.
- Successful pipeline run and Gold output.
- DQ report and RCA result.
- Fingerprinted DRY_RUN remediation plan.
- Approval-scope/fingerprint validation evidence without executing APPLY.
- Supervisor assessment.
- Copilot answer with retrieved source references.
- Observability dashboard overview, latency, token-usage, and cost screenshots.
- Sanitized observability API summary/trend output, if used.
- Test summary and RAG benchmark summary.

## 6. Demo completion criteria

The walkthrough is ready when another engineer can follow the documented sequence, understand the component boundaries, reproduce the safe checks, and distinguish demonstrated behavior from deferred or unvalidated capabilities.
