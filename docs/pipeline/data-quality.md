# DataPilot AI – Data Quality Framework

## 1. Overview

DataPilot AI implements a configurable data quality (DQ) framework using PySpark.

The framework evaluates datasets against checks defined in YAML configuration. The current pipeline runs DQ validation against Silver-layer data and writes a JSON result report for each dataset.

The framework is intended to make common data quality checks reusable across datasets rather than embedding every check directly in a dataset-specific job.

## 2. Component Structure

The main implementation files are:

| File | Responsibility |
|---|---|
| `spark/framework/dq/engine.py` | Reads DQ configuration and orchestrates enabled checks |
| `spark/framework/dq/checks.py` | Implements individual data quality checks |
| `spark/jobs/run_dq.py` | Loads dataset configuration, reads Silver data, runs DQ, and writes the report |
| `config/dq/` | Stores dataset-specific DQ YAML configuration |

## 3. Supported Data Quality Checks

The current `DQEngine` supports the following configured check types.

### 3.1 Row Count

Validates that the DataFrame contains at least the configured minimum number of rows.

The implementation uses a Spark `count()` action and returns PASS when the actual count is greater than or equal to the configured minimum.

### 3.2 Required Columns

Validates that configured column names are present in the DataFrame schema.

The check reports missing columns. It does not validate the semantic correctness of the column values.

### 3.3 Null Checks

Counts null values for each configured column.

If a configured column does not exist, the check returns a failure with a `COLUMN_NOT_FOUND` result.

The current implementation checks Spark null values. It does not treat empty strings or whitespace-only strings as null unless another transformation or check handles them.

### 3.4 Duplicate Check

Checks duplicate combinations of configured business-key columns.

The implementation groups by the configured keys and counts groups with more than one row.

If configured key columns are missing, the check returns a failure.

This check validates uniqueness of the configured key combination; it does not independently establish whether the selected keys are the correct business keys.

### 3.5 Range Checks

Validates configured numeric values against minimum and maximum bounds.

The check reports:
- The column being checked
- The configured range
- The number of invalid records
- Up to ten sample invalid values

If the configured column is missing, the check returns a failure.

The current range-check implementation does not explicitly classify null values as range violations. Null handling should be configured separately when required.

## 4. Configuration

Dataset-specific DQ configuration files are maintained under:

`config/dq/`

The DQ engine reads the `checks` section and executes enabled checks.

The supported configuration groups are:

- `row_count`
- `required_columns`
- `null_checks`
- `duplicate_check`
- `range_checks`

Only checks enabled in the dataset configuration are executed.

The configuration loader for the DQ job reads the selected dataset file from the DQ configuration directory.

## 5. Execution Flow

The DQ execution job is:

`spark/jobs/run_dq.py`

The current execution flow is:

```text
Airflow DQ Task
      |
      v
Load variables.yaml
      |
      v
Load dataset DQ YAML
      |
      v
Resolve Silver S3 path
      |
      v
Read Silver Parquet data
      |
      v
Initialize DQEngine
      |
      v
Execute enabled checks
      |
      v
Build overall PASS / FAIL status
      |
      v
Write JSON DQ report
      |
      v
Raise error when DQ status is FAIL
```

The Silver input path is resolved using the bucket and Silver-layer configuration in `config/variables.yaml`.

The job reads the Silver dataset in Parquet format and passes the DataFrame and dataset DQ configuration to `DQEngine`.

## 6. Result Reporting

The DQ job writes a dataset-level JSON report under:

`results/dq/{dataset}.json`

The report contains the overall DQ status and individual check results.

The job writes the report before raising an error for a failed DQ result. This allows downstream diagnosis to inspect the DQ evidence even when the Airflow task is marked as failed.

The report path is dataset-level rather than run-specific. The current pipeline configuration limits active runs, which reduces the risk of concurrent runs overwriting the same dataset report. A run-specific report path would be a future improvement for stronger isolation and historical traceability.

## 7. Airflow Integration

The main pipeline DAG is:

`datapilot_olist_order_pipeline`

DQ tasks execute after the relevant Silver processing tasks.

When the DQ job detects a failed check, it raises a runtime error. Airflow therefore records the DQ task as failed.

The pipeline's RCA task routing uses task failure conditions to determine when RCA processing should run. The Supervisor later collects task and DQ/RCA evidence for its assessment.

The Supervisor's DAG assessment does not itself invoke the RCA agent.

## 8. Current Implementation Characteristics

The current framework has the following characteristics:

- PySpark-based checks operate on Spark DataFrames.
- Dataset rules are configured separately in YAML.
- The DQ engine orchestrates the enabled checks.
- Individual checks return structured results.
- The execution job produces a JSON report.
- A failed DQ result becomes an Airflow task failure.

Some checks use Spark actions such as `count()`, `groupBy()` and `collect()`. These actions can trigger Spark jobs. The range check collects up to ten sample invalid values for reporting.

If no checks are enabled, the current `has_failures()` implementation finds no failed results and the overall outcome can be PASS. Dataset configurations should therefore be reviewed to ensure that the intended checks are enabled.

## 9. Known Scope and Limitations

The following points describe the current implementation and should not be interpreted as completed capabilities:

1. **Empty strings:** Null checks test null values; they do not automatically treat empty or whitespace-only strings as null.
2. **Range-check null handling:** Null values are not explicitly classified by the range-check implementation. Configure null checks separately where needed.
3. **Type validation:** The range-check implementation does not provide a general schema/type validation framework.
4. **Check configuration validation:** The current DQ engine reads the expected configuration structure but does not provide comprehensive schema validation or versioning for DQ YAML files.
5. **Performance:** Multiple checks can trigger multiple Spark actions and scans. Performance optimization may be needed for larger datasets.
6. **Report isolation:** Reports use dataset-level filenames rather than unique Airflow run identifiers.
7. **Rule coverage:** The framework executes the configured checks; it does not automatically infer business rules or prove that configured rules are complete.

## 10. Potential Future Enhancements

These are possible enhancements, not claims about current functionality:

- Add explicit empty-string and whitespace checks.
- Add schema and data-type validation.
- Add configurable null handling to range checks.
- Validate DQ YAML files against a formal schema.
- Add run-specific report paths and execution metadata.
- Add severity levels and configurable warning-versus-failure behavior.
- Reduce repeated Spark scans by consolidating compatible checks.
- Add reusable business-rule checks where justified by dataset requirements.

## 11. Related Components

- Spark DQ engine: `spark/framework/dq/engine.py`
- Individual checks: `spark/framework/dq/checks.py`
- DQ execution job: `spark/jobs/run_dq.py`
- DQ configurations: `config/dq/`
- Pipeline orchestration: `dags/datapilot_olist_pipeline.py`
- RCA agent: `agents/rca/`

## 12. Summary

The DataPilot AI DQ framework provides configurable PySpark checks for row counts, required columns, null values, duplicate keys, and value ranges.

The framework is integrated into the Airflow pipeline through a dataset-level execution job that writes structured JSON evidence and fails the task when configured checks fail.

Its current design provides a reusable foundation for data quality validation. Schema validation, richer rule semantics, run-specific reporting, and performance optimization remain potential areas for future enhancement.
