# DataPilot AI – Remediation Workflow

## 1. Purpose

DataPilot AI implements a controlled, human-approved data remediation workflow.

The workflow identifies invalid data, generates a remediation plan, validates human approval, executes the approved action, verifies the result and maintains an execution audit.

The current quarantine implementation supports the `olist_order_payments` dataset and the `QUARANTINE_RECORDS` action.

## 2. Architecture

The remediation workflow involves the following components:

1. RCA Agent – identifies data-quality failures and provides root-cause analysis.
2. Remediation Agent – prepares a candidate remediation plan.
3. Approval workflow – validates and records human approval.
4. Remediation Executor – validates the plan, approval, DQ evidence and execution scope.
5. Spark Quarantine Writer – identifies invalid records and writes them to an Iceberg quarantine table when APPLY is authorized.
6. Audit verification – verifies committed records and records execution results.
7. Gold refresh – performs downstream Gold processing separately.
8. Supervisor Agent – assesses the remediation workflow outcome.

## 3. Supported remediation action

| Attribute | Current implementation |
|---|---|
| Dataset | olist_order_payments |
| Action | QUARANTINE_RECORDS |
| Source layer | Silver |
| Target layer | Quarantine |
| Storage format | Apache Iceberg |
| Catalog | glue_catalog |
| Database | datapilot |
| Target table | olist_order_payments_quarantine |
| Business keys | order_id, payment_sequential |

The configured quarantine location is derived from the central storage configuration:

`processed/quarantine/olist_order_payments`

## 4. Data-quality rule

The current quarantine workflow addresses invalid `payment_installments` values.

The accepted range is inclusive:

- Minimum: 1
- Maximum: 100

Records are classified as invalid when:

- payment_installments is NULL
- payment_installments is less than 1
- payment_installments is greater than 100

The current approved execution validates an expected invalid-record count of 2.

The Spark writer also checks other payment-related DQ conditions before proceeding. It refuses installment-only quarantine if payment_type or payment_value null checks fail, or if payment_value violates its configured range.

## 5. Execution modes

### 5.1 DRY_RUN

DRY_RUN validates the candidate quarantine operation without modifying source data or writing to the quarantine Iceberg table.

The process:

1. Loads the Silver payment dataset.
2. Validates required columns and business keys.
3. Checks for null and duplicate business keys.
4. Validates other payment DQ rules.
5. Identifies invalid installment records.
6. Checks the expected invalid-record count.
7. Generates quarantine metadata in memory.
8. Writes a local execution audit.

Expected audit attributes:

- mode: DRY_RUN
- status: VALIDATED
- s3_write_performed: false
- source_modified: false

Spark job: `spark/jobs/quarantine_records.py`

### 5.2 APPLY

APPLY performs the explicitly approved quarantine operation.

It requires:

- `--mode APPLY`
- `--confirm-write`
- An APPROVED approval record
- `execution_authorized = true`
- Matching dataset and action
- Matching remediation plan fingerprint
- Explicit APPLY execution scope
- Validated quarantine configuration
- Validated DQ evidence

The executor also validates the expected invalid-record count before launching Spark.

Spark job: `spark/jobs/quarantine_records_candidate.py`

## 6. Approval and fingerprint validation

The workflow validates the relationship between the remediation plan and its approval record.

The executor checks:

- Dataset matches the requested dataset.
- Action matches QUARANTINE_RECORDS.
- Plan requires approval.
- Plan status is PENDING_APPROVAL.
- Plan execution mode is DRY_RUN.
- Approval status is APPROVED.
- Execution authorization is explicitly true.
- Approval fingerprint matches the current plan fingerprint.
- Execution scope matches the requested mode.

APPLY is rejected if any of these validations fail.

## 7. Spark quarantine execution

The Spark quarantine writer reads the Silver Parquet source and identifies invalid installment records.

It adds the following metadata to each incoming quarantine record:

| Metadata column | Purpose |
|---|---|
| execution_id | Identifies the remediation execution |
| quarantine_reason | Explains why the record was quarantined |
| rule_name | Identifies the failed DQ rule |
| source_layer | Records the source layer |
| quarantined_at | Records the quarantine timestamp |

### Iceberg target handling

During APPLY:

1. The Glue database is created if required.
2. The quarantine Iceberg table is created if it does not exist.
3. The registered Iceberg provider is validated.
4. The table location is compared with the expected quarantine S3 location.
5. The target schema and metadata types are validated.
6. Existing null or duplicate business keys are rejected.
7. Existing records with matching keys but conflicting source values are rejected.
8. A MERGE operation inserts records that do not already exist.

The MERGE uses:

- order_id
- payment_sequential

The writer does not update or delete existing quarantine records through this MERGE.

## 8. Post-commit verification

After the Iceberg MERGE, the Spark writer verifies:

1. Every expected invalid business key exists in the quarantine target.
2. All original source-column values match the Silver records.
3. Expected business keys are unique in the committed records.

The comparison uses null-safe equality for original source values.

Only after these checks pass does the Spark writer generate a successful APPLY audit.

Expected success attributes:

- mode: APPLY
- status: COMMITTED_AND_VERIFIED
- s3_write_performed: true
- source_modified: false

## 9. Audit and execution history

The workflow maintains execution-level audit records.

The Spark job writes mode-specific audit reports:

- DRY_RUN: `{dataset}_{execution_id}_dry_run.json`
- APPLY: `{dataset}_{execution_id}_apply.json`

The remediation executor also stores execution history under:

`results/remediation/history/`

The executor maintains a dataset-level execution result under:

`results/remediation/`

Audit creation uses temporary files and atomic publication to avoid overwriting an existing audit.

## 10. Failure handling and recovery

The Spark APPLY writer distinguishes failures according to the execution stage.

| Status | Meaning |
|---|---|
| FAILED_BEFORE_WRITE | Failure occurred before persistent changes began |
| WRITE_OUTCOME_UNKNOWN | Persistent changes may have started, but commit status is uncertain |
| COMMIT_SUCCEEDED_VERIFICATION_FAILED | MERGE completed, but verification failed |
| COMMITTED_VERIFIED_AUDIT_FAILED | Commit and verification succeeded, but successful audit publication failed |

The executor also uses `APPLY_OUTCOME_UNKNOWN` when the Spark APPLY subprocess does not return a verified success report.

### Recovery rules

- Do not automatically retry an APPLY operation with an unknown outcome.
- Inspect the Iceberg target table and Spark logs.
- Reconcile existing business keys before retrying.
- Use a new execution ID after the existing state has been investigated.
- Do not assume that a failed subprocess means the Iceberg write was rolled back.

The APPLY lock uses a local file lock to serialize concurrent attempts sharing the same mounted project directory. It is not a distributed lock across multiple hosts.

## 11. Source preservation and downstream processing

The quarantine writer reads from Silver and writes to the quarantine Iceberg target.

It does not modify Silver or Gold.

Gold cleanup and refresh are separate downstream operations. The remediation workflow must not treat successful quarantine as proof that Gold has already been refreshed.

The Supervisor Agent evaluates the broader workflow outcome after downstream processing.

## 12. Separate source-correction mechanism

DataPilot AI also contains a separate `SOURCE_CORRECTION` implementation in:

`spark/jobs/remediate_data.py`

That implementation applies explicitly approved corrections to Bronze and overwrites the Bronze Parquet dataset after validation.

It is distinct from the `QUARANTINE_RECORDS` workflow:

- SOURCE_CORRECTION changes approved values in Bronze.
- QUARANTINE_RECORDS preserves Silver and writes invalid records to a separate quarantine Iceberg table.

These two mechanisms must be documented and operated independently.

## 13. Relevant implementation files

| Component | Source file |
|---|---|
| Remediation Agent | `agents/remediation/remediation_agent.py` |
| Approval request | `agents/remediation/approval_request.py` |
| Plan fingerprint | `agents/remediation/plan_fingerprint.py` |
| Remediation executor | `agents/remediation/executor.py` |
| Dry-run Spark job | `spark/jobs/quarantine_records.py` |
| APPLY Spark job | `spark/jobs/quarantine_records_candidate.py` |
| Gold incremental loader | `spark/jobs/gold_incremental_iceberg.py` |
| Source correction job | `spark/jobs/remediate_data.py` |
| Quarantine configuration | `config/quarantine/olist_order_payments.yaml` |
| DQ configuration | `config/dq/olist_order_payments.yaml` |
| Central storage configuration | `config/variables.yaml` |
| Remediation DAG | `dags/datapilot_olist_data_remediation.py` |
| Approval DAG | `dags/datapilot_remediation_approval.py` |

## 14. Current implementation scope

The current quarantine implementation is intentionally dataset-specific.

It supports:

- One dataset: olist_order_payments
- One quarantine action: QUARANTINE_RECORDS
- One configured DQ rule for installment-range quarantine
- Glue Catalog and Iceberg as the quarantine target

Extending this framework to additional datasets or remediation actions requires corresponding configuration, validation, Spark implementation and approval-scope changes.
