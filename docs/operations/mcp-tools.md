# DataPilot AI – MCP Server and Tools

## 1. Overview

DataPilot AI exposes operational capabilities through an MCP (Model Context Protocol) server implemented in:

`mcp_server/server.py`

The server registers tools that allow connected agents and MCP clients to:
- Retrieve Data Quality (DQ) results.
- Inspect Bronze and Silver data through Spark jobs.
- Retrieve invalid records for RCA investigation.
- Read remediation audit records from PostgreSQL.
- Approve or reject a remediation request.
- Request the supported remediation execution workflow.

The server is implemented using the MCP Python server API and exposes a Streamable HTTP application.

**Important boundary:** MCP tools provide controlled access to existing DataPilot components. The MCP server does not itself implement the Spark transformations, DQ engine, RCA reasoning, or remediation business logic. It calls existing jobs, reads result files, queries the audit database, or delegates to the remediation executor.

## 2. Server Configuration

The server defines the project root relative to `mcp_server/server.py`:

`PROJECT_ROOT = Path(__file__).resolve().parent.parent`

It defines result directories for:
- DQ reports: `results/dq`
- RCA and inspection outputs: `results/rca`

The MCP server is initialized with the name:

`DataPilot AI MCP Server`

Its instructions describe it as providing controlled MCP tools for the DataPilot AI DataOps platform.

The server creates a Streamable HTTP ASGI application using `mcp.streamable_http_app()`.

## 3. Registered MCP Tools

The supplied implementation registers eight tools.

| Tool | Purpose | Main interaction |
|---|---|---|
| `get_dq_result` | Retrieve a dataset's DQ JSON report | Reads a local JSON file |
| `inspect_bronze` | Inspect Bronze data for a numeric range | Runs a Spark inspection job |
| `inspect_silver` | Inspect Silver data for a numeric range | Runs a Spark inspection job |
| `get_invalid_records` | Retrieve invalid Bronze records for investigation | Runs a Spark job |
| `get_remediation_audit` | Retrieve remediation execution audit records | Executes a read-only PostgreSQL query |
| `approve_remediation` | Approve a fingerprint-matched remediation request within a restricted scope | Validates request and plan, then writes an approval JSON |
| `reject_remediation` | Record rejection of a remediation request | Writes a rejection JSON |
| `execute_remediation` | Delegate the supported remediation request to the existing executor | Runs the executor and validates its dry-run result |

The following sections describe the behavior visible in `mcp_server/server.py`.

## 4. Input Validation Helpers

### 4.1 Dataset Validation

`validate_dataset(dataset)` rejects:
- An empty dataset name.
- A dataset name containing `/`.
- A dataset name containing `\\`.
- A dataset name containing `..`.

This validation is used before dataset names are used in result paths or Spark command arguments, and by the remediation tools.

### 4.2 Column Validation

`validate_column(column)` rejects:
- An empty column name.
- A column name containing `/`.
- A column name containing `\\`.
- A column name containing `..`.

This validation is used by the Bronze, Silver, and invalid-record inspection tools before the column name is passed to a Spark job.

These helpers provide basic path and input safeguards. They are not a complete allowlist of known datasets or known schema columns.

## 5. Tool: `get_dq_result`

**Purpose:** retrieve the existing DQ result for a dataset.

**Input:**
- `dataset` – dataset name.

**Execution flow:**
1. Validate the dataset name.
2. Construct the report path `results/dq/<dataset>.json`.
3. Check that the report exists.
4. Load and return the JSON content.

If the report does not exist, the tool raises `FileNotFoundError`.

This tool reads an existing report; it does not run DQ checks or modify the dataset.

## 6. Tool: `inspect_bronze`

**Purpose:** inspect Bronze data for a specified column and numeric range.

**Inputs:**
- `dataset`
- `column`
- `minimum`
- `maximum`

**Execution flow:**
1. Validate the dataset and column.
2. Build an output filename `mcp_bronze_<dataset>.json`.
3. Construct a `spark-submit` command for `spark/jobs/inspect_bronze.py`.
4. Submit the job to the existing Spark master at `spark://spark-master:7077`.
5. Pass the dataset, column, minimum, maximum, and container output path as command-line arguments.
6. Capture standard output and standard error.
7. Raise an error if the Spark process returns a non-zero exit code.
8. Verify that the expected host-mounted output file exists.
9. Read and return the JSON result.

The container output path is under:

`/opt/datapilot/results/rca/`

The corresponding host-side result is read from:

`results/rca/`

The tool delegates the inspection to Spark; it does not implement the inspection logic itself.

## 7. Tool: `inspect_silver`

**Purpose:** inspect Silver data for a specified column and numeric range.

**Inputs:**
- `dataset`
- `column`
- `minimum`
- `maximum`

Its execution pattern is similar to `inspect_bronze`, but it invokes:

`spark/jobs/inspect_silver.py`

The output filename is:

`mcp_silver_<dataset>.json`

The job runs on the existing Spark master, and the tool checks the process return code and expected output file before returning the JSON result.

This is an inspection operation. The tool does not perform Silver transformations or write a new Silver dataset.

## 8. Tool: `get_invalid_records`

**Purpose:** retrieve invalid Bronze records for RCA investigation.

**Inputs:**
- `dataset`
- `column`
- `minimum`
- `maximum`

**Execution flow:**
1. Validate the dataset and column.
2. Define the output file `results/rca/mcp_invalid_<dataset>.json`.
3. Create the output directory if necessary.
4. Submit `spark/jobs/get_invalid_records.py` to the existing Spark master.
5. Pass the dataset, column, numeric bounds, and container output path.
6. Check the process return code.
7. Verify that the output file exists.
8. Read and return the JSON result.

The MCP tool delegates record inspection to the Spark job. It does not itself implement the invalid-record selection logic.

## 9. Tool: `get_remediation_audit`

**Purpose:** retrieve recent remediation audit records from PostgreSQL without changing audit data.

**Inputs:**
- `dataset` – optional dataset filter, default empty string.
- `execution_id` – optional execution ID filter, default empty string.
- `limit` – number of records to return, default 10; permitted range is 1 to 20.

At least one of `dataset` or `execution_id` must be supplied.

### 9.1 Database Query

The tool queries:

`datapilot_audit.remediation_execution_summary`

It selects execution, DAG, dataset, action, mode, status, approval, count, target, path, source-write, timestamp, error, and audit-detail fields.

The query:
- Applies optional dataset and execution-ID filters.
- Orders results by `updated_at` descending.
- Applies the requested row limit.
- Uses SQL parameters for the filter values and limit.

### 9.2 Connection Configuration

Connection values are read from environment variables:
- `MCP_AUDIT_DB_HOST`
- `MCP_AUDIT_DB_PORT` (default `5432`)
- `MCP_AUDIT_DB_NAME`
- `MCP_AUDIT_DB_USER`
- `MCP_AUDIT_DB_PASSWORD`

The connection uses a five-second connection timeout and sets PostgreSQL's transaction option:

`default_transaction_read_only=on`

Rows are returned using `dict_row`.

### 9.3 JSON-Safe Result

The helper `json_safe()` converts:
- `date` and `datetime` values to ISO-formatted strings.
- `Decimal` values to floats.
- Nested dictionaries and lists recursively.

The response contains:
- `read_only: true`
- `count`
- `records`

This tool is read-only by design; it does not update remediation audit records.

## 10. Tool: `approve_remediation`

**Purpose:** approve the exact remediation plan submitted for review, within the execution scope enforced by the tool.

**Inputs:**
- `dataset`
- `action`
- `approved_by`
- `plan_fingerprint`

### 10.1 Validation

The tool:
1. Validates the dataset.
2. Accepts only `QUARANTINE_RECORDS`.
3. Requires a non-empty approver identity.
4. Requires a 64-character lowercase hexadecimal fingerprint.
5. Locates the matching approval request at `results/approvals/requests/<dataset>_<fingerprint>.json`.
6. Requires the request status to be `PENDING_REVIEW`.
7. Checks the request dataset, action, and fingerprint.
8. Requires the request execution scope to be `DRY_RUN_ONLY`.

### 10.2 Plan Verification

The request's plan path must be relative. The resolved path must remain inside the project root, and the plan file must exist.

The tool checks that the plan:
- Matches the requested dataset.
- Matches the requested action.
- Has status `PENDING_APPROVAL`.
- Has `requires_approval` set to `true`.
- Has execution mode `DRY_RUN`.

It then calls `validate_plan_fingerprint(plan)` and verifies that the calculated fingerprint matches the requested fingerprint.

### 10.3 Approval Record and Scope

On successful validation, the tool writes:

`results/approvals/<dataset>.json`

The record includes:
- Dataset and action.
- Status `APPROVED`.
- Approver identity.
- Verified plan fingerprint.
- Execution scope `DRY_RUN_ONLY`.
- `execution_authorized: true`.

**Critical scope distinction:** this approval authorizes only the fingerprint-matched dry-run workflow. It does not authorize APPLY or source-data modification. The source comments explicitly state that the executor rejects APPLY under this approval scope.

This tool records an approval decision; it does not itself execute remediation.

## 11. Tool: `reject_remediation`

**Purpose:** record rejection of a remediation request.

**Inputs:**
- `dataset`
- `action`
- `rejected_by`
- `reason`

The tool validates the dataset, then writes a JSON record to:

`results/approvals/<dataset>.json`

The record contains:
- Dataset.
- Action.
- Status `REJECTED`.
- Rejection identity.
- Rejection reason.
- `execution_authorized: false`.

The tool records rejection only. It does not execute remediation.

The supplied implementation does not perform the same request-file, plan-fingerprint, or pending-review checks as `approve_remediation`. It also does not explicitly validate that `action`, `rejected_by`, and `reason` are non-empty strings.

## 12. Tool: `execute_remediation`

**Purpose:** delegate the supported remediation request to the existing remediation executor and return a validated dry-run result.

**Inputs:**
- `dataset`
- `action`

### 12.1 Scope Restrictions

The tool:
- Validates the dataset.
- Restricts execution to `olist_order_payments`.
- Supports only `QUARANTINE_RECORDS`.
- Checks that `agents/remediation/executor.py` exists.

### 12.2 Executor Invocation

The tool invokes the executor using `python3` and passes:
- `--dataset`
- The requested dataset.
- `--action`
- The requested action.

The subprocess:
- Runs from the project root.
- Captures standard output and standard error.
- Has a 60-second timeout.
- Does not automatically raise on a non-zero exit code.

A timeout is converted to a `RuntimeError`. A non-zero return code produces a generic execution error and directs investigation to MCP server logs rather than returning arbitrary process output to the MCP client.

### 12.3 Execution Record Validation

After the subprocess completes, the tool expects:

`results/remediation/<dataset>_execution.json`

It validates that the result:
- Matches the requested dataset.
- Matches the requested action.
- Reports execution mode `DRY_RUN`.
- Reports `executed` as `false`.
- Reports `source_modification` as `false`.

If these checks fail, the tool raises an error rather than returning a successful dry-run response.

### 12.4 MCP Response

The response reports:
- Status `DRY_RUN_COMPLETED`.
- Dataset and action.
- `executed: false`.
- `s3_write_performed: false`.
- `source_modified: false`.
- Executor message.
- Audit file path.

The tool adds the MCP response to the executor result and persists the updated JSON using a temporary file, flush, `fsync`, and replacement of the original file.

**Safety boundary:** the supplied MCP implementation validates and returns a dry-run outcome. It does not expose an APPLY mode through this tool's arguments.

## 13. Transport and ASGI Application

The server configures `TransportSecuritySettings` with DNS rebinding protection enabled.

The allowed host entries are:
- `127.0.0.1:8000`
- `localhost:8000`
- `mcp-server:8000`

It then creates the Streamable HTTP ASGI application using:

`mcp.streamable_http_app(transport_security=transport_security)`

The server source shown here configures the ASGI application. The process startup command and container port publishing are deployment concerns defined outside this Python file.

## 14. Agent and Platform Integration

The MCP server provides a tool boundary between agents and operational capabilities.

Based on the supplied implementation and the previously reviewed agent responsibilities:

| Consumer or workflow | MCP capability used |
|---|---|
| RCA Agent | Reads DQ results, inspects Bronze and Silver data, retrieves invalid records, and can retrieve remediation audit evidence |
| Remediation workflow | Uses approval and remediation-related tools where wired into the workflow |
| Other MCP clients | Can call the registered tools through the Streamable HTTP endpoint, subject to their connection and authorization setup |

The RCA Agent's use of DQ, Bronze, Silver, invalid-record, and remediation-audit tools is part of its evidence-gathering workflow.

The MCP server does not contain the RCA reasoning engine or Supervisor decision logic. Those responsibilities remain in their respective agent modules.

## 15. Operational and Security Observations

The following observations are specific to the supplied `mcp_server/server.py` implementation:

1. Dataset and column validation reject empty values and basic path-traversal patterns, but do not enforce a known-dataset or known-column allowlist.
2. Bronze, Silver, and invalid-record inspection use `subprocess.run()` without an explicit timeout. The remediation executor call does have a 60-second timeout.
3. Inspection subprocess errors include captured standard output and standard error in the raised exception. The remediation execution tool intentionally avoids returning arbitrary process output to the MCP client.
4. Inspection result filenames are dataset-based rather than run-specific. Concurrent calls for the same dataset could contend for the same output filename.
5. `get_remediation_audit` uses parameterized SQL and a PostgreSQL read-only transaction option.
6. The audit database password is read from an environment variable; the source code does not contain a literal password.
7. `approve_remediation` verifies the request, plan scope, and fingerprint before recording a DRY_RUN_ONLY approval.
8. `reject_remediation` records a rejection but does not perform the same fingerprint and request-state validation as the approval tool.
9. `execute_remediation` is restricted to one dataset and one action, and validates that the resulting execution record describes a dry run with no source modification or S3 write.
10. Approval and rejection records are written to dataset-scoped JSON paths. The supplied code does not implement an atomic-write or concurrency-control mechanism for those approval files.
11. The server configures DNS rebinding protection and a specific allowed-host list. This source alone does not establish the complete network exposure, authentication, or authorization posture of the deployed service.
12. The tools do not all have identical side-effect profiles: some read reports, some launch Spark inspection jobs and write local result files, approval/rejection tools write workflow records, and the remediation tool delegates to the executor in dry-run mode.

These are implementation observations, not claims that controls elsewhere in the deployment do or do not exist.

## 16. Tool Capability and Side-Effect Summary

| Tool | Reads data or state | Runs Spark or subprocess | Writes files or records | Source/S3 data modification in this implementation |
|---|---|---|---|---|
| `get_dq_result` | DQ JSON | No | No | No |
| `inspect_bronze` | Bronze data through Spark job | Yes, Spark | Yes, inspection output JSON | Not indicated by this MCP wrapper |
| `inspect_silver` | Silver data through Spark job | Yes, Spark | Yes, inspection output JSON | Not indicated by this MCP wrapper |
| `get_invalid_records` | Bronze data through Spark job | Yes, Spark | Yes, invalid-record output JSON | Not indicated by this MCP wrapper |
| `get_remediation_audit` | PostgreSQL audit table | No Spark subprocess | No database writes | No |
| `approve_remediation` | Approval request and plan JSON | No | Yes, approval JSON | No |
| `reject_remediation` | No request/plan read in this implementation | No | Yes, rejection JSON | No |
| `execute_remediation` | Executor result JSON | Yes, remediation subprocess | Yes, execution/audit JSON | Validated as DRY_RUN with no S3 write and no source modification |

For the inspection tools, the actual Spark job implementations determine the detailed read behavior. This MCP wrapper launches those jobs and reads their output.

## 17. Source Code Reference

Primary implementation:
- `mcp_server/server.py`

Related components:
- `agents/remediation/plan_fingerprint.py`
- `agents/remediation/executor.py`
- `agents/rca/rca_agent.py`
- `spark/jobs/inspect_bronze.py`
- `spark/jobs/inspect_silver.py`
- `spark/jobs/get_invalid_records.py`

Related documentation:
- `docs/agents/rca-agent.md`
- `docs/agents/remediation-agent.md`
- `docs/operations/remediation-workflow.md`
- `docs/architecture/architecture.md`

## 18. Summary

The DataPilot AI MCP Server exposes controlled tools for operational evidence gathering and remediation workflow interaction. It connects MCP clients to existing DQ reports, Spark inspection jobs, invalid-record retrieval, PostgreSQL remediation audit data, approval records, and the remediation executor.

The tool set has distinct side-effect boundaries. Evidence retrieval and audit lookup are read-only with respect to source data; inspection jobs create result artifacts; approval and rejection write workflow records; and the remediation execution tool is constrained to a validated dry-run result. The approval tool records only a `DRY_RUN_ONLY` approval, not permission to apply source-data changes.

This separation allows agents and MCP clients to use operational capabilities while keeping the underlying data-processing, reasoning, approval, and execution responsibilities in their respective components.
