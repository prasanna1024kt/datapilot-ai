# DataPilot AI – RCA Agent

## 1. Overview

The DataPilot AI Root Cause Analysis (RCA) Agent investigates data-quality incidents using evidence collected through DataPilot AI MCP tools.

The agent combines:
- Data quality results.
- Bronze-layer inspection.
- Silver-layer inspection.
- Invalid-record evidence.
- Historical remediation audit evidence.
- OpenAI structured reasoning.
- Deterministic validation of the generated RCA result.
- LangSmith tracing with input/output privacy filters.

The RCA agent produces a structured recommendation. The code shown does not itself execute remediation actions or write to Bronze, Silver, Gold, or quarantine data.

## 2. Source Code and Dependencies

Main implementation:

`agents/rca/rca_agent.py`

MCP client:

`agents/rca/mcp_client.py`

The RCA agent imports and uses:
- `argparse` for command-line arguments.
- `json` for evidence serialization and result parsing.
- `math` for numeric-boundary validation.
- `os` and `python-dotenv` for environment configuration.
- `re` for parsing failed DQ range expressions.
- `langsmith.traceable` and `tracing_context` for tracing.
- `openai.OpenAI` for LLM analysis.
- `jsonschema.validate` for structured-output validation.
- `agents.rca.mcp_client.call_tool` for MCP tool calls.

The code loads environment variables using `load_dotenv()`. It requires `OPENAI_API_KEY` and raises a `RuntimeError` during module initialization if the key is absent.

The OpenAI client is initialized from that key. The configured model is:

`gpt-5.6`

## 3. RCA Agent Responsibilities

The system prompt defines the agent's responsibilities as:

1. Identify the failed data-quality check.
2. Identify the affected dataset and column.
3. Identify invalid values and their counts.
4. Compare DQ evidence with Bronze and Silver evidence.
5. Determine the most likely origin of the problem.
6. Distinguish observed evidence from inference.
7. Recommend appropriate next actions.
8. Avoid inventing evidence that is not present in the input.

The agent is instructed not to invent source records, business rules, timestamps, system behavior, or unsupported causality.

## 4. Evidence Collection Architecture

The `collect_mcp_evidence()` function collects the evidence required for an investigation.

It calls these MCP tools:

| MCP tool | Purpose in the RCA workflow |
|---|---|
| `get_dq_result` | Retrieves the dataset's DQ report |
| `inspect_bronze` | Inspects the selected column and range in Bronze |
| `inspect_silver` | Inspects the selected column and range in Silver |
| `get_invalid_records` | Retrieves invalid-record evidence from Bronze |
| `get_remediation_audit` | Retrieves recent remediation audit history |

The remediation audit request uses a limit of five records.

### 4.1 Investigation Scope

The CLI requires a dataset, column, minimum, and maximum. These values are initially used as a fallback investigation scope.

After retrieving and validating the DQ report, the agent searches its checks for the first failed check whose name begins with `range_check:`.

If such a check exists, the agent:
- Extracts the column name from the check identifier.
- Parses the range expression.
- Validates that the parsed bounds are finite.
- Validates that the minimum does not exceed the maximum.
- Uses the failed DQ rule's column and bounds as the investigation scope.

If no failed range check is found, the CLI values remain the investigation scope.

### 4.2 Evidence Bundle

After the MCP responses are validated, the function returns an evidence bundle containing:
- `dq_result`
- `bronze`
- `silver`
- `invalid_records`
- `remediation_audit`
- `investigation_scope` with column, minimum, maximum, and rule identifier

The evidence bundle is passed to the RCA reasoning stage.

## 5. Evidence Validation

The RCA agent validates MCP evidence before sending it to the LLM.

### 5.1 DQ Result Validation

`validate_dq_result()` checks that:
- The response is a dictionary.
- The dataset matches the requested dataset.
- Overall status is `PASS` or `FAIL`.
- `checks` is a list.
- Each check is a dictionary.
- Each check status is `PASS` or `FAIL`.
- Each check has a non-empty check name.
- A failed range check has a non-empty range expression.

### 5.2 Bronze and Silver Evidence Validation

`validate_mcp_evidence()` validates:
- Response object types.
- Dataset consistency across DQ, Bronze, and Silver.
- Expected layer identifiers (`bronze` and `silver`).
- Non-empty column and storage path values.
- Match with the selected investigation column and range.
- Supported evidence statuses.
- Numeric, finite-type-compatible range boundaries and minimum/maximum ordering.
- Non-negative integer invalid counts.
- Sample invalid values represented as a list.
- Sample count not exceeding the invalid count.
- Consistency between status and invalid count.

The allowed status values differ:
- Bronze: `PASS` or `FAIL`.
- Silver: `PASS`, `FAIL`, or `UNAVAILABLE`.

For unavailable Silver evidence, the validator requires a non-empty reason and rejects an `invalid_count` field. It does not treat unavailable evidence as PASS or as zero invalid records.

### 5.3 Invalid-Record and Remediation-Audit Validation

`validate_additional_mcp_evidence()` checks that:
- Invalid-record evidence matches the dataset, Bronze layer, column, and investigation range.
- Its status is `PASS` or `FAIL`.
- Invalid count and returned-record count are non-negative integers.
- The records field is a list of objects.
- The returned-record count matches the list length and does not exceed the invalid count.
- Status agrees with whether the invalid count is zero.
- The remediation audit is marked `read_only=true`.
- Audit count matches the number of returned audit records.
- Every audit record is an object for the requested dataset.

These checks validate the response structure and internal consistency. They do not independently prove that the underlying source systems are correct.

## 6. Classification Rules

The system prompt defines the intended classification decision matrix:

| DQ status | Bronze status | Silver status | Classification |
|---|---|---|---|
| FAIL | FAIL | FAIL | `UPSTREAM_DATA_QUALITY` |
| FAIL | PASS | FAIL | `TRANSFORMATION_DATA_QUALITY` |
| FAIL | PASS | PASS | `RCA_INCONCLUSIVE` |
| FAIL | Critical evidence unavailable | — | `RCA_INCONCLUSIVE` |

Other combinations that do not establish a supported classification should be classified as `RCA_INCONCLUSIVE`.

The prompt instructs the model to use LOW confidence and recommend `MANUAL_REVIEW` when critical layer evidence is unavailable.

### 6.1 Deterministic Classification Guardrail

`determine_expected_classification()` independently derives a classification from the DQ, Bronze, and Silver statuses.

Its implemented rules are:
- If DQ status is not `FAIL`, return `RCA_INCONCLUSIVE`.
- If Bronze or Silver status is `UNAVAILABLE`, return `RCA_INCONCLUSIVE`.
- If Bronze and Silver are both `FAIL`, return `UPSTREAM_DATA_QUALITY`.
- If Bronze is `PASS` and Silver is `FAIL`, return `TRANSFORMATION_DATA_QUALITY`.
- Otherwise, return `RCA_INCONCLUSIVE`.

`validate_rca_result()` compares the LLM classification with this deterministic result when RCA evidence is supplied.

## 7. Remediation Recommendation Rules

The system prompt defines the classification-to-action mapping:

| Classification | Allowed recommendation |
|---|---|
| `UPSTREAM_DATA_QUALITY` | `QUARANTINE_RECORDS` |
| `TRANSFORMATION_DATA_QUALITY` | `TRANSFORMATION_FIX` |
| `RCA_INCONCLUSIVE` | `MANUAL_REVIEW` or `NO_ACTION` |

The prompt says every remediation action must have `requires_approval=true`.

The output validator also checks that:
- The action is permitted for the selected classification.
- An inconclusive RCA does not recommend automated remediation.
- `NO_ACTION` has a meaningful justification.
- Every action other than `NO_ACTION` has `requires_approval=true`.

The validator's explicit approval check does not enforce `requires_approval=true` for `NO_ACTION`, even though the system prompt requests it.

## 8. Structured Output Contract

The `RCA_SCHEMA` defines a strict JSON object with these required top-level fields:

| Field | Type / allowed values |
|---|---|
| `dataset` | String |
| `dq_status` | `PASS` or `FAIL` |
| `classification` | `UPSTREAM_DATA_QUALITY`, `TRANSFORMATION_DATA_QUALITY`, or `RCA_INCONCLUSIVE` |
| `root_cause` | String |
| `evidence` | Array of strings |
| `recommendation` | String |
| `remediation` | Object |
| `next_actions` | Array of strings |
| `confidence` | `HIGH`, `MEDIUM`, or `LOW` |

The `remediation` object requires:
- `action`
- `requires_approval`
- `reason`

The schema rejects additional properties at both the top level and inside the remediation object.

The allowed action enum in the schema includes:
- `QUARANTINE_RECORDS`
- `TRANSFORMATION_FIX`
- `UPDATE_DQ_RULE`
- `NO_ACTION`
- `MANUAL_REVIEW`

The classification/action validator is more restrictive than the schema and does not permit `UPDATE_DQ_RULE` for any classification.

## 9. RCA Output Guardrails

`validate_rca_result()` validates the LLM result before it is accepted.

It checks:
1. The output is a dictionary.
2. The input DQ result is a dictionary.
3. The output conforms to `RCA_SCHEMA`.
4. The output dataset matches the input DQ dataset.
5. The output DQ status matches the input DQ status.
6. The classification matches the deterministic classification when layer evidence is provided.
7. Unavailable Silver evidence requires `RCA_INCONCLUSIVE`, LOW confidence, and `MANUAL_REVIEW`.
8. `RCA_INCONCLUSIVE` cannot have HIGH confidence.
9. The remediation action is permitted for the classification.
10. Inconclusive RCA cannot recommend automated remediation.
11. `NO_ACTION` has a meaningful reason.
12. Approval is required for actions other than `NO_ACTION`.

If a validation check fails, the function raises a `ValueError`; the invalid result is not returned as an accepted RCA result.

## 10. OpenAI Analysis and Orchestration

`analyze_with_openai()` sends the DQ result and RCA evidence bundle to the OpenAI Responses API.

The request includes:
- The configured model.
- The RCA system instructions.
- A JSON serialization of the collected evidence.
- Strict JSON Schema output formatting using `RCA_SCHEMA`.

The response text is parsed by `parse_llm_response()` using `json.loads()`. Invalid JSON raises a `RuntimeError`.

`run_rca()` coordinates:
1. OpenAI analysis.
2. JSON parsing.
3. RCA output validation.
4. Returning the validated structured result.

The agent therefore combines LLM reasoning with deterministic post-generation checks.

## 11. LangSmith Tracing and Privacy Filters

The implementation uses LangSmith tracing at three levels:
- MCP evidence collection.
- OpenAI RCA analysis.
- Overall RCA orchestration.

The trace input and output processors use allowlists.

### 11.1 Input Filtering

`_trace_rca_inputs()` retains selected metadata such as dataset, column, minimum, and maximum. It records summaries of DQ checks, Bronze/Silver status and invalid counts, invalid-record counts, and remediation-audit counts.

### 11.2 Output Filtering

`_trace_rca_outputs()` records selected result metadata such as dataset, DQ status, classification, confidence, remediation action, approval requirement, evidence count, and next-action count.

### 11.3 Evidence Filtering

`_trace_evidence_outputs()` records evidence summaries rather than raw MCP records. It includes layer statuses and counts, investigation scope, DQ summary, and audit summary.

The main execution uses `tracing_context()` with tags and metadata including dataset, column, environment, agent, and project.

These filters are intended to prevent raw record payloads from being included in the corresponding LangSmith trace inputs and outputs. They do not replace broader credential management or data-governance controls.

## 12. Result Persistence and CLI

The command-line interface requires:

| Argument | Purpose |
|---|---|
| `--dataset` | Dataset to investigate |
| `--column` | Fallback suspected anomaly column |
| `--minimum` | Fallback minimum valid value |
| `--maximum` | Fallback maximum valid value |
| `--output` | Destination path for the RCA JSON result |

The main function:
1. Parses the CLI arguments.
2. Starts a LangSmith tracing context.
3. Collects and validates MCP evidence.
4. Separates DQ evidence from the remaining RCA evidence.
5. Runs the RCA reasoning and guardrail workflow.
6. Saves the structured result as JSON.
7. Prints the RCA result to the console.

`save_result()` creates the output directory if needed and writes the JSON result with indentation.

The result printer displays dataset, DQ status, classification, confidence, root cause, evidence, remediation action, approval requirement, reason, and next actions.

The code shown writes the RCA result file using a normal file write; it does not implement an atomic file replacement in this function.

## 13. End-to-End Execution Flow

```text
CLI Arguments
     |
     v
Start LangSmith Trace Context
     |
     v
MCP: get_dq_result
     |
     v
Validate DQ Response
     |
     v
Derive Investigation Scope
     |
     +-----------------------------+
     |             |               |
     v             v               v
inspect_bronze  inspect_silver  get_invalid_records
     |             |               |
     +-------------+---------------+
                   |
                   v
        get_remediation_audit
                   |
                   v
        Validate All MCP Evidence
                   |
                   v
         Build Evidence Bundle
                   |
                   v
         OpenAI Responses API
                   |
                   v
          Parse JSON Response
                   |
                   v
       Validate Schema and Guardrails
                   |
                   v
          Save RCA JSON Result
                   |
                   v
          Print RCA Result
```

## 14. Safety Boundaries

The RCA agent:
- Collects evidence through MCP tools.
- Sends validated evidence to the configured LLM.
- Produces a structured root-cause assessment and recommendation.
- Enforces classification and remediation recommendation guardrails.
- Treats remediation audit data as historical evidence, not proof that a current incident is resolved.
- Distinguishes DRY_RUN and APPLY history.
- Does not itself execute the recommended remediation action in this file.

Actual remediation execution and approval are handled by the separate remediation workflow.

## 15. Current Scope and Limitations

The following observations are based on the supplied `rca_agent.py` implementation:

1. The classification helper supports only the explicit status combinations coded in `determine_expected_classification()`; other combinations become inconclusive.
2. `validate_mcp_evidence()` permits `UNAVAILABLE` for Silver, but its allowed Bronze statuses are only PASS and FAIL.
3. The special unavailable-evidence guardrail in `validate_rca_result()` explicitly checks unavailable Silver evidence.
4. The system prompt says all actions, including `NO_ACTION`, require approval. The validator explicitly checks approval for actions other than `NO_ACTION`.
5. The schema includes `UPDATE_DQ_RULE`, but the classification/action mapping does not allow it.
6. The first failed range check is used to derive investigation scope. If no failed range check is found, the CLI parameters are used.
7. The output JSON is written by `save_result()` without an atomic-write mechanism.
8. The code shown does not include retry logic around MCP calls or the OpenAI request.
9. The code shown does not execute remediation; it only recommends an action.
10. The code requires OpenAI configuration at import time, so importing the module without `OPENAI_API_KEY` raises an error.

These points describe the supplied file and should be considered when documenting or extending the agent.

## 16. Related Components

- RCA agent: `agents/rca/rca_agent.py`
- RCA MCP client: `agents/rca/mcp_client.py`
- RCA evaluators: `agents/rca/rca_evaluators.py`
- Actual RCA evaluation runner: `agents/rca/run_actual_rca_evaluation.py`
- Airflow pipeline DAG: `dags/datapilot_olist_pipeline.py`
- DQ engine: `spark/framework/dq/engine.py`
- DQ checks: `spark/framework/dq/checks.py`
- DQ execution job: `spark/jobs/run_dq.py`
- Remediation workflow: `docs/operations/remediation-workflow.md`

## 17. Summary

The DataPilot AI RCA agent is an MCP-powered, LLM-assisted incident-analysis workflow.

It collects DQ, Bronze, Silver, invalid-record, and historical remediation evidence; validates the evidence; derives an investigation scope; requests a structured OpenAI analysis; and validates the resulting classification and remediation recommendation with deterministic guardrails.

LangSmith tracing uses allowlisted summaries for the traced inputs and outputs. The RCA agent produces a JSON assessment and recommendation, while approval and remediation execution remain responsibilities of the separate remediation workflow.
