# DataPilot AI – Testing and Validation Report

> **Review draft:** This report records evidence shared in the project audit conversation. Re-run the commands against the current checkout before treating the results as a release record.

## 1. Purpose

This document distinguishes automated test results, configuration checks, smoke-test evidence, and validation that has not yet been established. A passing unit/API test suite is not equivalent to full end-to-end or production-readiness validation.

## 2. Validation evidence and audit snapshots

The latest shared local test audit reported:

| Check | Result | Interpretation |
|---|---|---|
| Full pytest suite | 132 passed; 1 warning; 7 subtests passed | Full local automated suite passed |
| Warning | Starlette TestClient uses deprecated anyio.abc.BlockingPortal alias | Non-failing deprecation warning |
| Local Python | 3.14.7 | Runtime used for the full local suite |
| CI Python | 3.11 | Runtime configured in GitHub Actions |
| CI workflow | Passed | Selected deployment-critical tests passed |
| Docker Compose validation | Exit code 0 | Compose configuration validated |

The full local suite and CI use different Python versions and different test selections. The available evidence does not establish that all 132 tests were executed under Python 3.11.

The previously reported clean Git working tree at commit `f16e805` is a historical snapshot. The latest documentation review showed modified and untracked documentation files. Do not describe the current working tree as clean until a new Git status confirms it.

## 3. CI scope

The reviewed CI workflow installs the Copilot requirements and PySpark, then runs these selected tests:

- `tests/test_airflow_service_smoke.py`
- `tests/test_approval_request.py`
- `tests/test_copilot_api.py`
- `tests/test_plan_fingerprint.py`
- `tests/test_quarantine_fingerprint_guard.py`
- `tests/test_remediation_executor.py`

It also validates the Docker Compose configuration. The reviewed workflow does not build and publish container images, deploy infrastructure, or perform an AWS deployment.

The full local suite and the narrower CI selection should not be described as identical coverage.

## 4. Historical pipeline execution evidence

A historical Airflow run of `datapilot_olist_order_pipeline`, dated October 7, 2026, provides execution evidence for individual pipeline stages.

| Stage | Historical evidence |
|---|---|
| Bronze | Multiple dataset tasks completed successfully |
| Silver | Multiple dataset tasks completed successfully, with Parquet writes committed |
| Gold | Selected incremental Iceberg loads completed successfully |
| Data quality | `olist_order_payments` DQ task failed |
| RCA | RCA workflow collected DQ and dataset evidence |

The historical Gold logs recorded successful incremental Iceberg processing for:
- `olist_customers`: 99,441 records
- `olist_order_items`: 112,650 records
- `olist_product_category_translation`: 71 records

The payment DQ task detected two records with `payment_installments = 0`, violating the configured range of 1–100. The corresponding RCA workflow investigated this failure.

**Important:** These results demonstrate successful individual processing tasks and a meaningful DQ failure investigation. They do not establish that the complete Airflow DAG run succeeded.

## 5. Test coverage interpretation

The test inventory includes tests for Airflow service smoke behavior, approval requests, Copilot API, chunking, embeddings, ingestion, LLM response generation, retrieval, vector storage, plan fingerprints, quarantine fingerprint guards, RCA evaluation, and remediation execution.

The existence of a test file does not establish that it is included in the CI workflow or that it exercises live external services. Inspect the workflow and test fixtures before making coverage claims.

## 6. Validation gaps

The current evidence does not establish all of the following:

- A single reproducible run covering source ingestion through Gold output, DQ failure, RCA, approval, remediation, Supervisor assessment, and Copilot retrieval.
- Python 3.11 parity for the complete local test suite.
- Real Iceberg APPLY behavior under concurrent execution and post-commit failure scenarios.
- Authenticated approver identity, approval expiry, and one-time approval enforcement.
- Retrieval latency and cost across strategies.
- Generated-answer quality across the complete retrieval benchmark.
- Production load, resilience, security penetration testing, or disaster-recovery objectives.

## 7. Recommended evidence capture for portfolio review

For a reproducible review, capture:

1. Git commit hash and clean/dirty status.
2. Python version and dependency-check output.
3. Full test summary and CI result.
4. Compose configuration validation result.
5. Service health checks for the services used in the demo.
6. A safe local pipeline run with expected output locations.
7. A DQ failure and corresponding RCA artifact.
8. A DRY_RUN remediation plan and approval-scope evidence; do not execute APPLY.
9. Supervisor assessment output.
10. A Copilot query with retrieved source references and the corresponding API response.

Store sanitized output and screenshots under a dedicated evidence directory only after reviewing them for secrets, personal data, and environment-specific values.

## 8. Conclusion

The shared audit demonstrates a strong automated-test and local-configuration baseline. It does not establish production readiness or complete end-to-end integration across every service. The most valuable next validation is a reproducible local demonstration and Python 3.11 compatibility run, with no AWS deployment or production writes required.
