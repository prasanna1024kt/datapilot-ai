# DataPilot AI – Remediation Agent

## 1. Overview

The DataPilot AI Remediation Agent converts a validated RCA result into a structured remediation plan.

The supplied implementation is located at:

`agents/remediation/remediation_agent.py`

Its responsibilities are to:
- Load an RCA result from JSON.
- Validate the RCA fields required for planning.
- Validate the proposed remediation action and approval flag.
- Build a remediation plan in a pending-approval state.
- Attach a plan fingerprint.
- Save the plan as JSON.
- Print the plan for review.

**Important boundary:** this file creates a remediation plan. It does not execute the remediation action, approve the plan, or perform data writes. Execution and approval are handled by separate remediation components and workflows.

## 2. Source Dependencies

The script uses:
- `argparse` for command-line arguments.
- `json` for reading and writing RCA and plan files.
- `pathlib.Path` for filesystem operations.
- `sys` to add the project root to the Python import path.
- `attach_plan_fingerprint` from `agents/remediation/plan_fingerprint.py`.

The project root is derived from the script location using `Path(__file__).resolve().parents[2]` and added to `sys.path` if it is not already present.

The script contains a repeated `from pathlib import Path` import. This is redundant but does not change the described workflow.

## 3. Supported Actions and Confidence Values

The script defines the following allowed remediation actions:

- `QUARANTINE_RECORDS`
- `TRANSFORMATION_FIX`
- `UPDATE_DQ_RULE`
- `NO_ACTION`
- `MANUAL_REVIEW`

Allowed confidence values are:

- `HIGH`
- `MEDIUM`
- `LOW`

These constants constrain the action and confidence values accepted by the RCA validation function.

## 4. RCA Result Loading

`load_rca_result(path)` opens the supplied file as UTF-8 and parses it using `json.load()`.

The expected input is a JSON object representing the RCA result.

The script does not catch file-not-found, permission, or JSON parsing errors in this function. Such errors therefore propagate to the caller.

## 5. RCA Validation

`validate_rca(rca)` validates the RCA object before a plan is built.

### 5.1 Object and Required Fields

The RCA input must be a dictionary and contain:

- `dataset`
- `dq_status`
- `classification`
- `root_cause`
- `recommendation`
- `remediation`
- `next_actions`
- `confidence`

### 5.2 DQ Status

The script permits remediation planning only when:

`dq_status == "FAIL"`

If the DQ status is not `FAIL`, validation raises a `ValueError`.

### 5.3 Basic Field Validation

The following fields must be non-empty strings:
- `dataset`
- `root_cause`
- `recommendation`

The confidence value must be one of `HIGH`, `MEDIUM`, or `LOW`.

`next_actions` must be a list. The supplied implementation checks that it is a list but does not validate the type or content of each list item.

### 5.4 Remediation Object

The `remediation` field must be a dictionary.

Its `action` must be one of the allowed actions.

The object must contain:
- `requires_approval`, which must be a Boolean.
- `reason`, which must be a non-empty string.

The script has a specific approval rule for `UPDATE_DQ_RULE`: this action must have `requires_approval` set to `True`.

The supplied `validate_rca()` function does not enforce `requires_approval=True` for every other action. It validates that the field exists and is Boolean, and applies the explicit true requirement to `UPDATE_DQ_RULE`.

The function also does not validate the classification-to-action mapping. That mapping is not implemented in this file.

## 6. Remediation Plan Construction

`build_remediation_plan(rca)` creates a new plan using the validated RCA result.

The plan contains:

| Field | Source or default |
|---|---|
| `dataset` | RCA dataset |
| `classification` | RCA classification |
| `confidence` | RCA confidence |
| `dq_status` | RCA DQ status |
| `action` | RCA remediation action |
| `requires_approval` | RCA remediation approval flag |
| `status` | Set to `PENDING_APPROVAL` |
| `execution_mode` | Set to `DRY_RUN` |
| `reason` | RCA remediation reason |
| `root_cause` | RCA root cause |
| `recommendation` | RCA recommendation |
| `next_actions` | RCA next actions |
| `business_justification_required` | `True` only for `UPDATE_DQ_RULE` |
| `rule_change` | Set to `None` |
| `execution` | Initialized as not executed |

The initial execution object is:

```json
{
  "executed": false,
  "executed_by": null,
  "execution_result": null
}
```

### 6.1 DQ Rule Change Governance

For `UPDATE_DQ_RULE`, the plan sets:

`business_justification_required = true`

The `rule_change` field is deliberately initialized to `None`.

The code comments explain that a proposed rule change must come from remediation reasoning or a verified business proposal. The agent must not guess a new business rule.

### 6.2 Plan Fingerprint

Before returning the plan, `build_remediation_plan()` calls:

`attach_plan_fingerprint(plan)`

The fingerprint implementation is in:

`agents/remediation/plan_fingerprint.py`

This document describes the call site only; the exact fingerprint algorithm and canonicalization behavior must be documented from that separate implementation.

## 7. Saving the Plan

`save_plan(path, plan)`:

1. Converts the output path to a `Path`.
2. Creates the parent directory if required.
3. Calls `attach_plan_fingerprint(plan)` again.
4. Writes the plan as UTF-8 JSON with indentation.
5. Uses `ensure_ascii=False` to preserve non-ASCII characters.
6. Uses `allow_nan=False` to reject non-standard NaN and infinity values.
7. Prints the saved output path.

The fingerprint helper is therefore called both when the plan is built and immediately before it is saved.

The supplied function writes directly to the target file. It does not implement an atomic temporary-file-and-rename operation or an explicit no-overwrite guard.

## 8. Plan Display

`print_plan(plan)` prints:
- Dataset
- DQ status
- Classification
- Confidence
- Action
- Approval requirement
- Whether business justification is required
- Execution mode
- Plan status
- Reason
- Root cause
- Recommendation
- Rule-change proposal, if present
- Next actions
- Initial execution state

When `rule_change` is `None`, the display states that it is not specified and business validation is required.

## 9. Command-Line Interface

The script exposes two required arguments:

| Argument | Purpose |
|---|---|
| `--rca-result` | Path to the RCA JSON input |
| `--output` | Path where the remediation plan JSON is saved |

The main execution sequence is:

1. Parse command-line arguments.
2. Load the RCA JSON.
3. Validate the RCA result.
4. Build the remediation plan.
5. Save the plan.
6. Print the plan.

The argument parser description identifies this script as the DataPilot AI Remediation Agent in dry-run mode.

## 10. End-to-End Planning Flow

```text
RCA JSON Result
       |
       v
Load RCA Result
       |
       v
Validate Required Fields
       |
       v
Validate DQ Status = FAIL
       |
       v
Validate Confidence and Remediation Object
       |
       v
Build PENDING_APPROVAL Plan
       |
       v
Set execution_mode = DRY_RUN
       |
       v
Initialize Execution as Not Executed
       |
       v
Attach Plan Fingerprint
       |
       v
Save Plan JSON
       |
       v
Print Plan for Review
```

## 11. Governance and Safety Boundaries

The supplied script establishes several planning-time controls:

- It requires a failed DQ status before creating a plan.
- It restricts actions to a declared allowlist.
- It restricts confidence values to a declared allowlist.
- It requires an approval field with Boolean type.
- It explicitly requires approval for `UPDATE_DQ_RULE`.
- It creates plans with `PENDING_APPROVAL` status.
- It initializes execution mode to `DRY_RUN`.
- It initializes execution as not performed.
- It does not infer a DQ rule change; `rule_change` remains `None`.
- It attaches a plan fingerprint.

These controls do not by themselves approve or execute a plan. The separate approval and execution components must validate the plan and authorization before any persistent action.

## 12. Relationship to Other Remediation Components

This script is the plan-generation component, not the complete remediation system.

Related components include:

| Component | Responsibility |
|---|---|
| `agents/remediation/remediation_agent.py` | Validates RCA input and creates a fingerprinted remediation plan |
| `agents/remediation/plan_fingerprint.py` | Attaches and validates plan fingerprints (algorithm details belong to that file) |
| `agents/remediation/approval_request.py` | Approval request workflow |
| `agents/remediation/executor.py` | Validates and coordinates remediation execution |
| `agents/remediation/summary_store.py` | Remediation summary/history support |
| `dags/datapilot_remediation_approval.py` | Airflow approval workflow |
| `dags/datapilot_olist_data_remediation.py` | Airflow remediation workflow |
| `spark/jobs/quarantine_records_candidate.py` | Quarantine Iceberg dry-run/APPLY implementation |

The detailed execution and approval process is documented separately in:

`docs/operations/remediation-workflow.md`

## 13. Current Implementation Scope and Limitations

The following observations are specific to the supplied `remediation_agent.py` source:

1. The script validates that `classification` exists but does not validate it against a classification allowlist.
2. It does not validate a classification-to-action mapping.
3. It checks that `next_actions` is a list but does not validate individual entries as strings.
4. It checks the approval field's Boolean type, but explicitly enforces `True` only for `UPDATE_DQ_RULE`.
5. It does not validate that `rule_change` is populated for `UPDATE_DQ_RULE`; it intentionally initializes the field to `None`.
6. It does not execute remediation or modify source data.
7. It does not itself verify approval records, execution fingerprints, or execution history.
8. It writes the plan directly to the requested output path; no atomic-write or no-overwrite behavior is implemented in this file.
9. The exact fingerprint algorithm is implemented in the separate `plan_fingerprint.py` module and is not visible in this script.
10. It imports `Path` twice; this is redundant.

These observations should be considered before extending the plan-generation logic. They do not imply that the separate executor or approval workflow lacks corresponding safeguards.

## 14. Summary

The DataPilot AI Remediation Agent converts a failed-DQ RCA result into a structured plan. It validates required RCA fields and allowed actions, initializes the plan as `PENDING_APPROVAL` in `DRY_RUN` mode, marks execution as not performed, attaches a plan fingerprint, and saves the result as JSON.

Approval, fingerprint verification during execution, and persistent remediation actions are responsibilities of separate components. Keeping plan generation separate from approval and execution provides a clear boundary between an AI-generated recommendation and an authorized data operation.
