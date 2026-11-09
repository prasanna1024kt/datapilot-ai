"""DataPilot AI RAG evaluation dataset.

Reference answers were drafted from the 14 Markdown files supplied in docs.zip.
Review them against the current implementation before treating them as gold labels.
"""

EVALUATION_DATASET = [
    {
        "id": "RAG001",
        "category": "architecture",
        "question": "Explain the DataPilot AI architecture.",
        "expected_sources": ["architecture/architecture.md"],
        "reference_answer": (
            "DataPilot AI is an AI-assisted data engineering and DataOps platform built around "
            "the Olist e-commerce data workflows. Its core architecture uses Amazon S3 for raw "
            "and processed storage; Apache Spark and PySpark for distributed processing; Apache "
            "Airflow for orchestration; AWS Glue Data Catalog and Apache Iceberg for catalog and "
            "table management; and a configurable PySpark data-quality framework. Three Python "
            "agents provide root-cause analysis (RCA), remediation-plan generation, and operational "
            "supervision. An MCP server exposes platform tools to connected agents, while LangSmith "
            "supports tracing. The documented storage layout separates raw data from processed "
            "Bronze, Silver, Gold, and quarantine areas. The architecture document describes the "
            "FastAPI/RAG Copilot as a developing capability with foundational modules and further "
            "integration work pending; it should not be represented by that document as a fully "
            "integrated autonomous agent workflow."
        ),
    },
    {
        "id": "RAG002",
        "category": "agents",
        "question": "How do the different agents interact with each other?",
        "expected_sources": [
            "architecture/agent-interactions.md",
            "agents/agent-interactions.md",
        ],
        "reference_answer": (
            "The agents participate in coordinated workflows with explicit responsibility "
            "boundaries rather than one unrestricted autonomous chain. The RCA Agent investigates "
            "a data-quality failure using DQ, Bronze, Silver, invalid-record, and remediation-audit "
            "evidence, then writes a structured RCA JSON result. The Remediation Agent reads that "
            "RCA JSON, validates it, creates a remediation plan, attaches a plan fingerprint, and "
            "saves the plan with PENDING_APPROVAL status and DRY_RUN execution mode. A separate "
            "human approval workflow records approval or rejection. MCP tools expose inspection, "
            "approval, audit, and supported execution operations to the workflow, and a separate "
            "executor performs the authorized operation. The Supervisor Agent assesses pipeline "
            "and remediation outcomes. Importantly, the Remediation Agent does not directly call "
            "MCP or approve/execute its plan, and the Supervisor does not approve or execute "
            "remediation."
        ),
    },
    {
        "id": "RAG003",
        "category": "agents",
        "question": "What responsibilities does the RCA Agent have?",
        "expected_sources": ["agents/rca-agent.md"],
        "reference_answer": (
            "The RCA Agent investigates data-quality incidents and produces a structured root-cause "
            "analysis and recommendation; it does not execute remediation or write to Bronze, Silver, "
            "Gold, or quarantine data. It gathers evidence through MCP tools: get_dq_result for the "
            "DQ report, inspect_bronze and inspect_silver for layer evidence, get_invalid_records "
            "for invalid Bronze records, and get_remediation_audit for recent audit history. It "
            "validates the evidence, identifies the failed check and affected dataset/column, compares "
            "Bronze and Silver findings, distinguishes observed evidence from inference, and avoids "
            "inventing unsupported causes or records. Its deterministic classification guardrail "
            "distinguishes UPSTREAM_DATA_QUALITY when Bronze and Silver both fail, "
            "TRANSFORMATION_DATA_QUALITY when Bronze passes and Silver fails, and "
            "RCA_INCONCLUSIVE when evidence is insufficient or unavailable. It validates the "
            "structured output before saving the RCA result."
        ),
    },
    {
        "id": "RAG004",
        "category": "agents",
        "question": "How does the Remediation Agent generate remediation plans?",
        "expected_sources": ["agents/remediation-agent.md"],
        "reference_answer": (
            "The Remediation Agent takes an RCA JSON file supplied through --rca-result. It validates "
            "that the input is an object containing dataset, dq_status, classification, root_cause, "
            "recommendation, remediation, next_actions, and confidence, and it only creates a plan "
            "when dq_status is FAIL. It checks required strings, the confidence allowlist (HIGH, "
            "MEDIUM, LOW), and a remediation object containing an allowed action, a Boolean "
            "requires_approval field, and a non-empty reason. It builds a plan by carrying forward "
            "the RCA findings and initializes status to PENDING_APPROVAL, execution_mode to "
            "DRY_RUN, execution.executed to false, and execution identity/result fields to null. "
            "For UPDATE_DQ_RULE it marks business justification as required and leaves rule_change "
            "null rather than inventing a business rule. It attaches a plan fingerprint and writes "
            "the JSON plan to the --output path. Plan generation itself does not request approval, "
            "call MCP, execute remediation, or modify source data."
        ),
    },
    {
        "id": "RAG005",
        "category": "agents",
        "question": "What responsibilities does the Supervisor Agent have?",
        "expected_sources": [
            "agents/supervisor-agent.md",
            "architecture/architecture.md",
            "architecture/agent-interactions.md",
        ],
        "reference_answer": (
            "The Supervisor Agent provides operational oversight by collecting and assessing Airflow "
            "task states, DQ reports, RCA evidence, Gold task outcomes, and remediation workflow "
            "results, then writing structured assessment JSON with decisions and recommended next "
            "actions. It has three distinct entry paths. The standalone run_assessment path loads "
            "and validates a DQ report, checks recent DAG runs and failure diagnostics, combines DQ "
            "and pipeline signals, and can invoke the existing RCA Agent as a subprocess for a "
            "supported failed numeric range check. The in-DAG run_dag_assessment path assesses "
            "the current pipeline run using task states and run-related DQ/RCA/Gold evidence; it "
            "does not invoke RCA. The remediation-DAG assessment path evaluates DRY_RUN, APPLY, "
            "or GOLD_REFRESH_ONLY task outcomes and execution metadata. The Supervisor is an "
            "assessment component: it does not grant approval, execute remediation, modify source "
            "data, or trigger DAGs."
        ),
    },
    {
        "id": "RAG006",
        "category": "agents",
        "question": "How does the agent interaction workflow work?",
        "expected_sources": ["agents/agent-interactions.md"],
        "reference_answer": (
            "The documented workflow begins when a pipeline data-quality failure produces evidence "
            "for the RCA Agent. The RCA Agent investigates and saves an RCA JSON result. The "
            "Remediation Agent then loads and validates that result and creates a fingerprinted plan "
            "whose initial state is PENDING_APPROVAL and DRY_RUN. A separate approval workflow "
            "records a human approval or rejection. If approved, the downstream remediation workflow "
            "validates the plan, fingerprint, scope, and execution authorization before invoking the "
            "supported remediation executor. The quarantine path can then be verified and audited, "
            "followed by separate Gold refresh work. The Supervisor assesses pipeline or remediation "
            "outcomes and produces an assessment report. These are separate components connected "
            "through artifacts, Airflow tasks, and workflow tools: the Remediation Agent only creates "
            "the plan file, and the Supervisor observes outcomes rather than approving or executing "
            "the remediation."
        ),
    },
    {
        "id": "RAG007",
        "category": "pipeline",
        "question": "How is the data pipeline orchestrated?",
        "expected_sources": ["pipeline/pipeline-orchestration.md"],
        "reference_answer": (
            "The main Airflow DAG is datapilot_olist_order_pipeline. It is manually triggered "
            "(schedule=None), has catchup disabled, and limits active tasks and active DAG runs to "
            "one. For each of nine Olist datasets, Airflow submits a Bronze Spark job to convert raw "
            "CSV data to Parquet, then a matching Silver Spark job, followed by a DQ job that reads "
            "Silver data and writes a JSON result. Each DQ task is upstream of both its matching RCA "
            "task and Gold task. RCA tasks use TriggerRule.ONE_FAILED and run when the relevant "
            "DQ failure condition is met; the documented RCA callable expects exactly one failed "
            "numeric range check. Gold tasks run the generic incremental Iceberg loader for each "
            "dataset. After all nine Gold tasks, the Gold business-summary task builds "
            "olist_order_summary. Finally, supervisor_assessment uses TriggerRule.ALL_DONE and "
            "assesses the run after DQ, RCA, Gold, and summary tasks reach terminal states."
        ),
    },
    {
        "id": "RAG008",
        "category": "pipeline",
        "question": "How does the Spark processing framework work?",
        "expected_sources": ["pipeline/spark-framework.md"],
        "reference_answer": (
            "The Spark framework is a reusable, configuration-driven PySpark pipeline centered on "
            "DataPipeline. It loads centralized project variables and the selected layer/dataset "
            "configuration, resolves source path and format, resolves target path and format, reads "
            "the source through DataReader, applies configured DataFrame transformations through "
            "DataTransformer when present, resolves write mode and partition strategy, and writes "
            "through DataWriter. ConfigLoader handles YAML settings and PathBuilder constructs raw "
            "or layer-specific storage paths, avoiding hard-coded dataset paths. Supported configured "
            "transformations include column-name standardization, trimming, casting, timestamp "
            "conversion, duplicate removal, null handling, filtering, and dropping columns. The "
            "generic DataPipeline does not automatically execute DQ checks, business transformations, "
            "Gold/Iceberg processing, RCA, or remediation; those are separate jobs or components. "
            "Its partition strategy controls Spark execution partitions with coalesce/repartition, "
            "not storage partitioning via partitionBy()."
        ),
    },
    {
        "id": "RAG009",
        "category": "pipeline",
        "question": "What data quality checks are supported?",
        "expected_sources": ["pipeline/data-quality.md"],
        "reference_answer": (
            "The PySpark DQEngine supports five configured check groups: row count, required "
            "columns, null checks, duplicate checks, and numeric range checks. Row count verifies "
            "that the DataFrame meets a configured minimum number of rows. Required-columns checks "
            "verify that configured names exist in the schema. Null checks count Spark null values "
            "for configured columns. Duplicate checks detect repeated combinations of configured "
            "business-key columns. Range checks compare numeric values with configured minimum and "
            "maximum bounds and report the column, range, invalid count, and up to ten sample invalid "
            "values. Only enabled checks in the dataset's config/dq YAML are executed against Silver "
            "Parquet data. The job writes a dataset-level JSON report under results/dq/{dataset}.json "
            "before raising an error when the overall DQ status is FAIL, allowing Airflow to mark the "
            "task failed while retaining diagnostic evidence. Null checks do not automatically treat "
            "empty strings as null, and range checks do not explicitly classify nulls as range "
            "violations."
        ),
    },
    {
        "id": "RAG010",
        "category": "pipeline",
        "question": "How does Gold-layer processing work?",
        "expected_sources": ["pipeline/gold-processing.md"],
        "reference_answer": (
            "DataPilot AI documents two separate Gold workflows. The generic incremental loader "
            "(spark/jobs/gold_incremental_iceberg.py) reads gold_tables.yaml, selects an enabled "
            "dataset, resolves its Silver Parquet source, target Iceberg table, and business keys, "
            "starts Spark with the AWS Glue Iceberg catalog and S3 FileIO, validates required and "
            "non-null business keys and source duplicates, adds ingestion metadata, then creates "
            "the Iceberg table on first load or performs an incremental MERGE on later loads. It "
            "performs post-load uniqueness and dataset-specific checks. For olist_order_payments, "
            "additional quarantine validation is applied. The separate run_gold.py business-summary "
            "workflow reads the orders, order-items, payments, and customers Gold tables, aggregates "
            "item and payment measures by order before joining to order-level data, validates the "
            "summary, and creates or incrementally MERGEs the olist_order_summary Iceberg table. "
            "The generic table loader and business-summary builder have separate configuration "
            "and execution paths."
        ),
    },
    {
        "id": "RAG011",
        "category": "operations",
        "question": "How is the remediation approval workflow controlled?",
        "expected_sources": ["operations/remediation-workflow.md"],
        "reference_answer": (
            "The remediation workflow is human-approved and currently supports QUARANTINE_RECORDS "
            "for olist_order_payments. A plan is created in PENDING_APPROVAL and DRY_RUN state. "
            "Before APPLY, the executor validates the dataset and action, that approval is required, "
            "the plan is still pending and in DRY_RUN, the approval record is APPROVED and explicitly "
            "execution-authorized, the plan fingerprint matches, and the execution scope permits "
            "APPLY. APPLY also requires --mode APPLY and --confirm-write, plus valid quarantine "
            "configuration and DQ evidence. DRY_RUN validates the candidate operation and writes a "
            "local audit without modifying source data or writing to the quarantine Iceberg table. "
            "APPLY writes invalid payment-installment records to the configured Iceberg quarantine "
            "table, verifies the committed business keys and source values, and records an execution "
            "audit. The quarantine writer preserves Silver; Gold refresh is a separate downstream "
            "operation. Unknown write outcomes must be investigated rather than automatically retried."
        ),
    },
    {
        "id": "RAG012",
        "category": "mcp",
        "question": "What MCP tools are available in DataPilot AI?",
        "expected_sources": ["operations/mcp-tools.md"],
        "reference_answer": (
            "The DataPilot AI MCP server registers eight tools. get_dq_result reads a dataset's DQ "
            "JSON report. inspect_bronze and inspect_silver run Spark inspection jobs for numeric "
            "range evidence in the respective layers. get_invalid_records runs a Spark job to "
            "retrieve invalid Bronze records. get_remediation_audit performs a read-only PostgreSQL "
            "audit query. approve_remediation validates the approval request, plan scope, and "
            "fingerprint before writing a restricted DRY_RUN_ONLY approval record. reject_remediation "
            "writes a rejection record. execute_remediation delegates to the existing executor and "
            "validates that the result is a DRY_RUN with executed=false, no S3 write, and no source "
            "modification. The server exposes these tools through Streamable HTTP. The MCP server "
            "is an integration layer: it calls existing jobs, reads files, queries audit data, or "
            "delegates to the executor; it does not implement the DQ engine, RCA reasoning, or Spark "
            "transformation logic itself."
        ),
    },
    {
        "id": "RAG013",
        "category": "operations",
        "question": "What configuration values are defined in variables.yaml and gold_tables.yaml?",
        "expected_sources": ["operations/configuration-reference.md"],
        "reference_answer": (
            "In variables.yaml, project.name is datapilot-ai and project.environment is dev. "
            "aws.region is us-east-1. Storage type is s3 and the bucket is datapilot-ai-data-practice. "
            "The configured layer prefixes are raw=raw, bronze=processed/bronze, "
            "silver=processed/silver, gold=processed/gold, and quarantine=processed/quarantine. "
            "Formats are raw=csv and bronze, silver, and gold=parquet; the file does not specify "
            "a quarantine format. In gold_tables.yaml, the Gold catalog is glue_catalog, database "
            "is datapilot, and warehouse is s3a://datapilot-ai-data-practice/gold. It defines nine "
            "enabled Silver-to-Gold dataset mappings: customers keyed by customer_id; geolocation "
            "by geolocation_zip_code_prefix, geolocation_lat, and geolocation_lng; order_items by "
            "order_id and order_item_id; order_payments by order_id and payment_sequential; "
            "order_reviews by review_id and order_id; orders by order_id; product_category_translation "
            "by product_category_name; products by product_id; and sellers by seller_id. All nine "
            "mappings enable source-duplicate validation."
        ),
    },
    {
        "id": "RAG014",
        "category": "operations",
        "question": "How do I deploy DataPilot AI?",
        "expected_sources": ["operations/deployment-and-environment-setup.md"],
        "reference_answer": (
            "The documented local deployment uses Docker Compose with PostgreSQL 16, Airflow "
            "3.3.1 services, Spark 4.1.3 standalone master/worker plus a development container, "
            "and a separate MCP server. Build the Spark image first because the MCP Dockerfile uses "
            "datapilotai-spark:latest as its base image. The one-shot airflow-init service waits for "
            "PostgreSQL health and runs airflow db migrate; the Airflow API server, scheduler, DAG "
            "processor, and triggerer depend on that initialization completing. The Spark worker "
            "depends on the Spark master, and MCP depends on Spark master and worker. Host endpoints "
            "documented are Airflow localhost:8080, Spark master localhost:7077, Spark UI "
            "localhost:8081, and MCP localhost:8000; the internal MCP URL is "
            "http://mcp-server:8000/mcp. The document describes the services and dependencies but "
            "explicitly says the intended first-time build/start and restart commands still require "
            "confirmation. It also notes that no Copilot API service appears in the supplied Compose "
            "file, so deployment steps for that service cannot be inferred from this runbook."
        ),
    },
    {
        "id": "RAG015",
        "category": "agents",
        "question": "How are agent interactions and remediation controlled?",
        "expected_sources": [
            "architecture/agent-interactions.md",
            "operations/remediation-workflow.md",
        ],
        "reference_answer": (
            "Agent interactions are controlled through separated roles and workflow artifacts. The "
            "RCA Agent produces a structured RCA result from validated DQ and layer evidence. The "
            "Remediation Agent validates that result and creates a fingerprinted PENDING_APPROVAL "
            "plan in DRY_RUN mode; it does not approve or execute the plan. A separate human approval "
            "workflow records the decision. The executor revalidates dataset, action, plan status, "
            "approval authorization, fingerprint, execution scope, and DQ/quarantine conditions "
            "before APPLY. The current quarantine implementation is restricted to "
            "olist_order_payments and QUARANTINE_RECORDS, targeting an Iceberg quarantine table "
            "using order_id and payment_sequential as business keys. DRY_RUN makes no source or "
            "quarantine-table write. APPLY quarantines invalid payment_installments values while "
            "preserving Silver, verifies committed records, and writes audit evidence. Gold refresh "
            "is separate. The Supervisor assesses outcomes but cannot approve, execute, or modify "
            "source data."
        ),
    },
    {
        "id": "RAG016",
        "category": "operations",
        "question": "How are Docker services, environment variables and runtime settings configured?",
        "expected_sources": ["operations/deployment-and-environment-setup.md"],
        "reference_answer": (
            "The supplied Docker Compose deployment defines PostgreSQL 16; Airflow 3.3.1 API "
            "server, scheduler, DAG processor, triggerer, and one-shot initialization service; "
            "Spark 4.1.3 standalone master and worker plus a development/debug container; and the "
            "MCP server. Airflow uses LocalExecutor, disables example DAGs, and runs database "
            "migration through airflow-init after PostgreSQL becomes healthy. Spark worker depends "
            "on spark-master, and MCP depends on both Spark master and worker. Runtime environment "
            "references include AIRFLOW_FERNET_KEYRING, AWS_PROFILE, AWS_DEFAULT_REGION, "
            "AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, OPENAI_API_KEY, LANGSMITH_API_KEY, "
            "LANGSMITH_TRACING, LANGSMITH_PROJECT, DATAPILOT_ENV, and MCP_AUDIT_DB_HOST, "
            "PORT, NAME, USER, and PASSWORD. Project code, DAGs, Spark apps, configuration, "
            "results, AWS credentials, and secrets are mounted into services with differing "
            "read/write permissions. Host ports include 8080 for Airflow, 7077 and 8081 for Spark, "
            "and 8000 for MCP. The runbook warns against committing or sharing secrets and says "
            "development credentials/static JWT values must be replaced before non-local deployment. "
            "It also explicitly records that the supplied Compose file does not define a Copilot API "
            "service or health checks for Airflow, Spark, and MCP."
        ),
    },
]
