# DataPilot AI – Supervisor Agent

## 1. Overview

The DataPilot AI Supervisor Agent coordinates read-only assessment of pipeline health, data-quality outcomes, RCA evidence, Gold-layer completion, and remediation workflow outcomes.

The supplied implementation is:

`agents/supervisor/supervisor_agent.py`

The implementation has three main assessment entry points:

1. `run_dag_assessment()` – assesses the current main pipeline DAG run using current-run task states and DQ/RCA evidence.
2. `run_remediation_dag_assessment()` – assesses a remediation DAG run and its DRY_RUN, APPLY, or GOLD_REFRESH_ONLY workflow state.
3. `run_assessment()` – standalone, read-only assessment using a DQ report and recent Airflow DAG-run information. This path can invoke the existing RCA agent when DQ assessment indicates RCA is required.

The Supervisor does not itself execute remediation, approve remediation, or trigger Airflow DAGs. The code records these boundaries in its assessment output.

## 2. Dependencies and Configuration

The script uses:
- `argparse` for the command-line interface.
- `json` for reading DQ/RCA results and writing assessment reports.
- `datetime` and `timezone` for UTC assessment timestamps and Airflow timestamp parsing.
- `pathlib.Path` for project and results paths.
- `typing.Any` for flexible Airflow data structures.
- `re` for range-check parsing and safe output filenames.
- `subprocess` and `sys` for the standalone RCA subprocess invocation.
- `AirflowClient` from `agents.supervisor.airflow_client`.
- `traceable` from LangSmith for tracing selected assessment functions.

The project root is calculated from the script location:

`Path(__file__).resolve().parents[2]`

Default paths and DAG ID are:

| Setting | Default |
|---|---|
| DQ report | `results/dq/olist_order_payments.json` |
| Standalone assessment output | `results/supervisor/assessment.json` |
| Main pipeline DAG ID | `datapilot_olist_order_pipeline` |

The module defines `SupervisorError` for errors while loading or validating DQ evidence.

## 3. LangSmith Tracing and Data Minimization

The main pipeline assessment and remediation assessment functions are decorated with LangSmith `@traceable`.

### 3.1 Input Allowlist

`_trace_supervisor_inputs()` creates a reduced input payload. It includes selected fields such as:
- DAG ID
- DAG run ID
- Supported remediation mode (`DRY_RUN`, `APPLY`, or `GOLD_REFRESH_ONLY`)
- DQ table count
- Gold table count

It does not forward the complete DAG-run object or the full input dictionary.

### 3.2 Output Allowlist

`_trace_supervisor_outputs()` returns a compact operational summary, including selected fields such as:
- Assessment type
- DAG ID
- Supervisor decision
- Task count and task-state counts
- Failed and upstream-failed task counts
- Requested remediation mode
- Execution-ID status

It does not return the complete assessment object to the LangSmith trace output processor.

This is an allowlist-based approach: only explicitly selected operational fields are sent through these trace processors.

## 4. DQ Report Loading and Validation

`load_dq_result(path)` loads an existing DQ JSON report.

It validates that:
- The file exists.
- The file can be read and parsed as JSON.
- The parsed value is a JSON object represented as a Python dictionary.
- The required fields `dataset`, `status`, and `checks` are present.
- `checks` is a list.

File access and JSON parsing errors are wrapped in `SupervisorError`. The function does not validate every individual check object's schema.

## 5. Airflow Task-State Collection

The Supervisor uses `AirflowClient` to inspect Airflow through its REST API.

### 5.1 Current DAG Run Tasks

`_get_current_run_task_instances(dag_run)` retrieves task instances for the supplied DAG ID and run ID, with a limit of 100.

`collect_current_run_tasks(dag_run)` summarizes each task instance using:
- `task_id`
- `state`
- `try_number`
- `start_date`
- `end_date`

It also returns:
- DAG ID
- DAG run ID
- DAG run state
- Total task count
- Counts grouped by task state
- The task list

### 5.2 Timestamp Parsing

`_parse_airflow_timestamp(value)` parses an ISO-formatted Airflow timestamp and returns a Unix timestamp.

A trailing `Z` is converted to a UTC offset. If the parsed timestamp has no timezone, UTC is assumed. Invalid or missing values return `None`.

## 6. Run-Aware DQ and RCA Evidence

`collect_dag_run_evidence(dag_run, dq_tables, results_root)` collects evidence for the current DAG run.

It uses:
- DQ reports under `results/dq/`
- RCA reports under `results/rca/<dag_run_id>/`

### 6.1 DQ Evidence

For each dataset, the function looks for the Airflow task named:

`dq_<dataset>`

It then checks the DQ report at:

`results/dq/<dataset>.json`

Possible evidence statuses include:
- `TASK_INSTANCE_MISSING`
- `MISSING`
- `UNKNOWN_TASK_START_TIME`
- `STALE`
- A successfully loaded and assessed report
- `READ_FAILED`

The DQ report freshness check compares the report modification time with the current DQ task's start time. A report more than one second older than the task start is rejected as stale.

A successfully loaded report is passed to `assess_dq()` and marked `CURRENT_RUN_CANDIDATE`.

### 6.2 RCA Evidence

For each dataset, the function looks for:

`rca_<dataset>`

RCA evidence is accepted only when the corresponding Airflow task state is `success`. The report is then read from:

`results/rca/<dag_run_id>/<dataset>.json`

Possible statuses include:
- `TASK_INSTANCE_MISSING`
- `NOT_SUCCESSFUL`
- `REPORT_MISSING`
- `SUCCESS`
- `READ_FAILED`

The supplied implementation scopes RCA reports by DAG run ID and checks the RCA task's success state. It does not perform the same report-modification-time freshness check that it performs for DQ reports.

## 7. DQ Assessment

`assess_dq(dq_result)` summarizes the checks in a DQ report.

It separates checks into:
- Passed checks (`PASS`)
- Failed checks (`FAIL`)
- Unknown checks (any status other than `PASS` or `FAIL`)

It then assigns a decision:

| Condition | DQ decision |
|---|---|
| DQ status is not PASS/FAIL, or any check has an unknown status | `REVIEW_REQUIRED` |
| DQ status is FAIL, or one or more checks failed | `INVOKE_RCA` |
| DQ status is PASS and all check statuses are recognized | `DQ_PASSED` |

The returned summary includes dataset, normalized DQ status, total/passed/failed/unknown counts, failed checks, unknown checks, and the decision.

## 8. Pipeline Status and Failure Diagnostics

### 8.1 Recent DAG Runs

`get_pipeline_status(airflow_client, dag_id)` retrieves up to five recent DAG runs. If there are no runs, it returns `NO_RUNS`.

Otherwise, it selects the latest run by logical date, falling back to start date for sorting, and returns the selected run's ID, state, logical date, start date, and end date.

This function reads Airflow state; it does not trigger a DAG.

### 8.2 Failed Task Diagnostics

`get_task_failure_diagnostics()` retrieves task instances for a specific DAG run and inspects tasks in either:
- `failed`
- `upstream_failed`

For failed tasks, it retrieves task attempts and identifies the latest failed attempt. It then attempts to retrieve the full task log for that attempt.

The code identifies useful log entries by searching event text for indicators including:
- `traceback`
- `exception`
- `error`
- `failed`
- `caused by:`

It records task metadata, attempt metadata, and log evidence. Log retrieval errors are represented as `LOG_RETRIEVAL_FAILED`.

The overall diagnostic status is:
- `ERROR_EVIDENCE_AVAILABLE` when useful log evidence is found.
- `UNKNOWN_INSUFFICIENT_LOG_EVIDENCE` when a task failed but useful log evidence was not found.
- `NO_FAILED_TASKS` when no task is failed.

The diagnostic result contains the collected log entries for failed tasks. The LangSmith output processor remains separately allowlisted.

## 9. Decision Logic for Standalone Assessment

`make_decision(dq_assessment, pipeline_assessment)` treats DQ and pipeline state as independent signals before combining them.

### 9.1 Pipeline Signal

| Airflow state | Pipeline decision |
|---|---|
| `queued` or `running` | `PIPELINE_IN_PROGRESS` |
| `failed` | `INVESTIGATE_PIPELINE_FAILURE` |
| `success` | `PIPELINE_SUCCESS` |
| Other or missing state | `PIPELINE_STATUS_UNKNOWN` |

### 9.2 DQ Signal

The DQ action is derived from the DQ assessment:
- `INVOKE_RCA`
- `REVIEW_DQ_EVIDENCE`
- `DQ_PASSED`

### 9.3 Combined Decision

The combined decision can be:
- `REVIEW_REQUIRED`
- `INVOKE_RCA_AND_INVESTIGATE_PIPELINE`
- `PIPELINE_IN_PROGRESS_WITH_DQ_FAILURE`
- `INVOKE_RCA`
- `INVESTIGATE_PIPELINE_FAILURE`
- `PIPELINE_IN_PROGRESS`
- `PIPELINE_HEALTHY`

The function also builds recommended next actions, such as invoking the existing RCA agent, investigating a historical pipeline failure, monitoring execution, reviewing evidence, or continuing normal monitoring.

## 10. Optional RCA Invocation in the Standalone Path

The standalone assessment path contains an RCA invocation helper. This is distinct from the in-DAG assessment path.

### 10.1 Extracting the Investigation Scope

`extract_failed_range_check(dq_assessment)` scans failed checks and looks for a check name matching:

`range_check:<column>`

It then parses a numeric range expression in the form:

`minimum <= value <= maximum`

The first supported failed numeric range check is returned with its column, minimum, and maximum. If none is found, the helper returns `None`.

### 10.2 Invoking the Existing RCA Agent

`invoke_rca_agent(dataset, dq_assessment)`:
1. Extracts a supported failed numeric range check.
2. Returns `SKIPPED` if no supported range check is found.
3. Creates a sanitized dataset-based output filename under `results/rca/`.
4. Invokes `agents.rca.rca_agent` as a Python module using the current Python interpreter.
5. Passes dataset, column, minimum, maximum, and output path.
6. Uses a 900-second subprocess timeout.
7. Checks the subprocess return code and output file.
8. Parses the resulting RCA JSON.

It handles timeout, process-start errors, non-zero exit, missing output, and invalid/unreadable JSON with explicit failure statuses.

A successful result returns the investigation scope, output path, and parsed RCA result.

**Important distinction:** `run_dag_assessment()` explicitly does not invoke RCA. It consumes the run-specific RCA report produced by the pipeline's RCA task. The standalone `run_assessment()` path can invoke the RCA agent when its DQ decision is `INVOKE_RCA`.

## 11. Main Pipeline DAG-Run Assessment

`run_dag_assessment(dag_run, dq_tables, gold_tables)` is the in-DAG assessment entry point.

Its sequence is:

1. Collect current-run Airflow task states.
2. Build a task lookup by task ID.
3. Collect DQ and RCA evidence for the current DAG run.
4. Collect Gold task states for each configured dataset.
5. Collect the `gold_olist_order_summary` task state.
6. Count DQ outcomes.
7. Count failed and upstream-failed tasks.
8. Determine the final Supervisor decision.
9. Build and save a run-specific assessment JSON.
10. Return the assessment.

### 11.1 Gold Status Collection

For every configured Gold dataset, it looks up:

`gold_<dataset>`

It separately looks up:

`gold_olist_order_summary`

Missing task instances are represented as `TASK_INSTANCE_MISSING`.

### 11.2 Final Decision Priority

The decision logic is evaluated in this order:

| Condition | Supervisor decision |
|---|---|
| DQ evidence is missing, stale, or otherwise invalid | `REVIEW_REQUIRED` |
| One or more DQ reports have status FAIL | `DQ_FAILURE_INVESTIGATION_REQUIRED` |
| One or more tasks are failed | `PIPELINE_FAILURE_INVESTIGATION_REQUIRED` |
| Gold business summary task is not successful | `GOLD_SUMMARY_NOT_COMPLETED` |
| None of the above | `PIPELINE_HEALTHY` |

The resulting assessment includes:
- Pipeline and task-state summary
- DQ assessment and per-dataset evidence
- RCA assessment and per-dataset evidence
- Gold dataset task results
- Gold business-summary status
- Supervisor decision and recommended next actions
- Execution-safety flags

The safety flags explicitly state:
- `remediation_executed: false`
- `dag_triggered: false`
- `rca_invoked_by_supervisor: false`

The report is saved under:

`results/supervisor/<sanitized_dag_run_id>_assessment.json`

## 12. Remediation DAG Assessment

`run_remediation_dag_assessment(dag_run)` assesses an existing remediation DAG run. It does not execute remediation.

It reads the requested mode from DAG run configuration, defaulting to `DRY_RUN`, and collects current task states.

### 12.1 Monitored Tasks

The function monitors these task IDs:
- `execute_remediation_and_write_summary`
- `validate_existing_apply_for_gold`
- `continue_to_gold_only_after_apply`
- `refresh_gold_olist_order_payments`
- `refresh_gold_olist_order_summary`
- `update_postgres_summary_after_gold_refresh`
- `update_postgres_summary_after_gold_failure`

Missing task instances are represented as `TASK_INSTANCE_MISSING`.

### 12.2 Mode-Specific Decisions

**DRY_RUN**
- `DRY_RUN_COMPLETED_NO_GOLD_REFRESH`
- `REMEDIATION_DRY_RUN_FAILED`
- `REVIEW_REMEDIATION_EXECUTION`

**APPLY**
- `APPLY_EXECUTION_FAILED_OR_INCOMPLETE`
- `APPLY_AND_GOLD_REFRESH_SUCCESS`
- `GOLD_SUCCESS_SUMMARY_UPDATE_INCOMPLETE`
- `APPLY_COMMITTED_GOLD_REFRESH_FAILED`
- `APPLY_COMPLETED_GOLD_OUTCOME_UNKNOWN`

**GOLD_REFRESH_ONLY**
- `EXISTING_APPLY_VALIDATION_FAILED_OR_INCOMPLETE`
- `GOLD_REFRESH_ONLY_SUCCESS`
- `GOLD_SUCCESS_SUMMARY_UPDATE_INCOMPLETE`
- `GOLD_REFRESH_ONLY_FAILED`
- `GOLD_REFRESH_OUTCOME_UNKNOWN`

Any other mode results in:

`UNSUPPORTED_REMEDIATION_MODE`

### 12.3 Execution-ID Metadata

`_get_remediation_execution_metadata()` resolves an execution ID for assessment and traceability.

- For `GOLD_REFRESH_ONLY`, it reads `execution_id` from DAG run configuration.
- For `DRY_RUN` and `APPLY`, it retrieves the executor task's XCom return value and looks for `execution_id`.
- Unsupported modes are marked `UNSUPPORTED_MODE`.
- If the executor task did not succeed, the ID is marked `EXECUTOR_NOT_SUCCESSFUL`.
- Missing or failed XCom retrieval is represented by explicit statuses.

XCom retrieval is best-effort; an XCom error does not fail the Supervisor assessment.

The remediation assessment is saved under:

`results/supervisor/remediation/<sanitized_dag_run_id>_assessment.json`

Its execution-safety section states:
- `remediation_executed_by_supervisor: false`
- `dag_triggered_by_supervisor: false`
- `approval_granted_by_supervisor: false`
- `source_data_modified_by_supervisor: false`

## 13. Standalone Read-Only Assessment

`run_assessment(dq_path, dag_id, output_path)` is the standalone assessment flow.

It:
1. Loads and validates the selected DQ report.
2. Assesses DQ checks.
3. Creates an `AirflowClient`.
4. Retrieves recent pipeline status.
5. If the latest run failed, attempts to collect task-failure diagnostics.
6. Builds the combined decision using `make_decision()`.
7. Invokes the existing RCA agent when the DQ decision is `INVOKE_RCA`.
8. Builds an assessment marked `READ_ONLY`.
9. Saves the assessment JSON to the requested output path.
10. Returns the assessment.

The output explicitly includes:
- `execution_mode: READ_ONLY`
- `remediation_executed: false`
- `dag_triggered: false`

The standalone flow can write its assessment file and invoke the existing RCA subprocess, but it does not execute remediation or trigger an Airflow DAG.

## 14. Command-Line Interface

The script provides these arguments:

| Argument | Purpose | Default |
|---|---|---|
| `--dataset` | Dataset name | `olist_order_payments` |
| `--dag-id` | Airflow DAG ID | `datapilot_olist_order_pipeline` |
| `--dq-file` | Explicit DQ report path | Derived from dataset |
| `--output` | Assessment output JSON path | `results/supervisor/assessment.json` |

The CLI derives the DQ report path as:

`results/dq/<dataset>.json`

unless `--dq-file` is provided.

It then calls `run_assessment()` and prints the returned assessment as formatted JSON.

## 15. End-to-End Functional View

```text
                         Airflow REST API
                                |
                                v
                    Current DAG / Task States
                                |
              +-----------------+-----------------+
              |                 |                 |
              v                 v                 v
          DQ Reports        RCA Reports       Gold Task States
              |                 |                 |
              v                 v                 |
         DQ Assessment     RCA Evidence           |
              |                 |                 |
              +-----------------+-----------------+
                                |
                                v
                       Supervisor Decision
                                |
                                v
                    Run-Specific JSON Report
```

The standalone path has an additional optional branch:

```text
Standalone DQ Report
        |
        v
    DQ Assessment
        |
        v
    INVOKE_RCA?
        |
        +---- No ----> Continue assessment
        |
        +---- Yes ---> Extract supported numeric range check
                           |
                           v
                    Invoke existing RCA agent
                           |
                           v
                    Include RCA result/status
```

The remediation assessment path is separate:

```text
Remediation DAG Run
        |
        v
Collect current task states
        |
        v
Read requested mode
        |
        +---- DRY_RUN
        +---- APPLY
        +---- GOLD_REFRESH_ONLY
        |
        v
Resolve execution-ID metadata
        |
        v
Evaluate task outcomes
        |
        v
Save remediation assessment
```

## 16. Operational Boundaries and Implementation Notes

The following points are directly observable in the supplied source:

1. The Supervisor reads Airflow state through `AirflowClient`; it does not trigger DAGs.
2. The in-DAG pipeline assessment consumes run-specific RCA reports and does not invoke the RCA agent.
3. The standalone `run_assessment()` path can invoke the existing RCA agent through a subprocess.
4. The remediation DAG assessment observes task outcomes and execution metadata; it does not approve or execute remediation.
5. The in-DAG DQ report path is dataset-scoped (`results/dq/<dataset>.json`), and the code uses task start time versus file modification time to reject stale DQ reports.
6. The RCA report path is run-scoped (`results/rca/<dag_run_id>/<dataset>.json`), and the code requires the RCA task to be successful before reading it.
7. The RCA report collection does not include an equivalent file modification-time check.
8. The standalone RCA output path is dataset-based (`results/rca/<dataset>_supervisor_rca.json`), rather than DAG-run-based.
9. The standalone assessment retrieves up to five recent DAG runs and selects the latest by logical date or start date.
10. Task failure diagnostics can collect full log content for failed attempts. The LangSmith trace output processor is a separate allowlist and does not forward the full assessment.
11. The assessment reports are written to local project result directories. The supplied code does not implement an atomic-write pattern for these assessment files.
12. The decision logic is deterministic Python logic based on task states and DQ evidence; this script does not use an LLM to make the Supervisor decision.

These notes describe this file only. They should not be interpreted as claims about additional safeguards or behavior implemented in other modules.

## 17. Summary

The DataPilot AI Supervisor Agent provides operational oversight across the main data pipeline and remediation workflows. It combines Airflow task state, DQ results, RCA evidence, Gold completion status, and remediation execution outcomes into structured decisions and JSON assessment reports.

Its three entry points serve different purposes: current pipeline-run assessment, remediation-run assessment, and standalone read-only assessment. The in-DAG paths observe existing run evidence without invoking RCA or executing remediation. The standalone path can invoke the existing RCA agent when DQ evidence indicates that investigation is needed, while still keeping remediation execution and DAG triggering outside the Supervisor's responsibility.
