# DataPilot AI – Security, Governance, and Known Limitations

## 1. Purpose

DataPilot AI includes tools that inspect data and workflows as well as tools that can participate in remediation. This document explains the intended governance boundaries, the safeguards described in the implementation, and the limitations that should be disclosed before any production use.

## 2. Governance principles

- Separate diagnosis, plan generation, approval, and execution.
- Treat a pending plan as a proposal, not authorization.
- Bind approvals to the plan fingerprint and permitted scope.
- Keep the supported remediation action and dataset narrowly constrained.
- Prefer DRY_RUN for review and demonstration.
- Preserve source data and record execution/audit outcomes.
- Keep Supervisor assessment separate from approval and execution authority.

## 3. Remediation controls documented

The remediation implementation and runbook describe controls including:

- Validation of the remediation plan and its fingerprint.
- Validation of approval state, dataset, action, fingerprint, and execution scope.
- A restricted quarantine action for the currently supported execution path.
- DRY_RUN and APPLY as distinct execution modes.
- APPLY requiring explicit `confirm_write=True` and a separate dataset-specific APPLY approval artifact.
- Post-commit verification for the Iceberg quarantine operation.
- Execution history/audit records.

The MCP approval path documented in `docs/operations/mcp-tools.md` is scoped to `DRY_RUN_ONLY`. Do not represent that approval path as granting APPLY authorization.

## 4. MCP tool boundaries

The MCP server exposes read-oriented inspection and audit tools as well as approval, rejection, and execution tools. The documentation describes parameter validation, restricted execution scope, and read-only database access for audit retrieval.

Important implementation limitations recorded in the audit/review include:

- No caller authentication/authorization was visible in the inspected MCP tool implementation; an external network/authentication boundary may exist but must be independently verified.
- The `approved_by` value is caller-supplied text and is not, by itself, proof of authenticated human identity.
- The rejection path does not enforce the same request-file, fingerprint, and pending-review checks as the approval path.
- Some Spark subprocess calls in inspection tools do not have an explicit timeout.
- Dataset validation rejects unsafe path patterns but does not implement a strict allowlist/regular-expression policy.

These points should be rechecked against the current source before release.

## 5. Executor and data-integrity limitations

The executor contains plan/approval/fingerprint and scope checks. The reviewed design also has limitations that should be disclosed:

- MCP approval does not create the separate APPLY approval artifact required by the executor.
- The APPLY path has not been exercised as part of the current portfolio closure audit and must not be described as recently validated.
- The audit write is not atomic with the Iceberg commit; a post-commit audit failure can leave the data commit successful while audit persistence is incomplete.
- The file lock described in the implementation is host/filesystem scoped and is not a distributed lock across independent hosts.
- Existing tests mock Spark behavior and do not establish real Iceberg concurrency, post-commit failure recovery, or authenticated approver behavior.

## 6. Copilot and LLM considerations

The Copilot combines document retrieval and LLM response generation. The following considerations should be addressed before production use:

- Retrieved content must be treated as untrusted input, not as executable instructions.
- Source references should be preserved so users can inspect supporting evidence.
- The response-generation `grounded` indicator should not be interpreted as independent proof of factual grounding unless it is computed from a defined verification procedure.
- The API's authentication and authorization boundary should be confirmed; no in-application auth control was visible in the reviewed routes.
- Apply request limits, timeouts, context-size controls, error sanitization, and cost controls before exposing the API broadly.
- Do not claim Copilot LangSmith tracing or full MCP agent orchestration unless those specific integrations are verified.
- Retrieval quality and answer quality must be evaluated separately.

## 7. Local environment and secrets

The deployment documentation identifies local-development authentication defaults and sensitive configuration in Compose. Before sharing or deploying beyond a private development environment:

- Ensure no real secrets are committed or included in screenshots/logs.
- Rotate any real credential that may have been exposed.
- Use unique, strong credentials and an appropriate secret-management mechanism.
- Restrict access to mounted host credential/configuration files.
- Review published service ports and network exposure.
- Confirm which services actually require external cloud credentials.

This document intentionally does not reproduce credential values.

## 8. Backup and recovery evidence

The deployment runbook records a PostgreSQL backup/restore validation on November 16, 2026: a custom-format backup was inspected, 72 user tables were restored, selected record counts matched, and the temporary restore database was removed without overwriting the live database.

The same runbook lists outstanding operational capabilities: scheduled backups, retention policy, off-site storage, automated integrity monitoring, periodic automated restore testing, and defined RTO/RPO. The successful restore test is useful evidence but does not establish a complete disaster-recovery program.

## 9. Production-readiness boundary

The current portfolio is intended for local demonstration. AWS deployment is deferred. The available audit does not establish production-grade identity management, network isolation, full end-to-end authorization, load/performance behavior, distributed locking, or formal recovery objectives.

Before production deployment, perform a dedicated security and operational review covering identity, least privilege, secrets, network boundaries, audit integrity, failure recovery, performance, data retention, and incident response.

## 10. Safe portfolio demonstration

For an interview or portfolio walkthrough:

- Use read-only inspection and DRY_RUN behavior.
- Show a pending plan and explain the approval boundary.
- Do not run APPLY or mutate source data.
- Use sanitized examples and redact credentials.
- Clearly label historical test results and unvalidated capabilities.
