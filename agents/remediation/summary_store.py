import json

from airflow.providers.postgres.hooks.postgres import PostgresHook

POSTGRES_CONN_ID = "datapilot_postgres"

UPSERT_SQL = """
INSERT INTO datapilot_audit.remediation_execution_summary (
    execution_id,
    dag_id,
    dag_run_id,
    dataset,
    action,
    execution_mode,
    execution_status,
    approval_status,
    approved_by,
    plan_fingerprint,
    invalid_count,
    quarantined_count,
    target_table,
    quarantine_path,
    source_modified,
    s3_write_performed,
    started_at,
    completed_at,
    error_type,
    error_message,
    audit_details,
    updated_at
)
VALUES (
    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, NOW()
)
ON CONFLICT (execution_id) DO UPDATE SET
    dag_id = COALESCE(EXCLUDED.dag_id,
                      remediation_execution_summary.dag_id),
    dag_run_id = COALESCE(EXCLUDED.dag_run_id,
                          remediation_execution_summary.dag_run_id),
    execution_status = EXCLUDED.execution_status,
    approval_status = COALESCE(EXCLUDED.approval_status,
                               remediation_execution_summary.approval_status),
    approved_by = COALESCE(EXCLUDED.approved_by,
                           remediation_execution_summary.approved_by),
    plan_fingerprint = COALESCE(EXCLUDED.plan_fingerprint,
                                remediation_execution_summary.plan_fingerprint),
    invalid_count = COALESCE(EXCLUDED.invalid_count,
                             remediation_execution_summary.invalid_count),
    quarantined_count = COALESCE(EXCLUDED.quarantined_count,
                                 remediation_execution_summary.quarantined_count),
    target_table = COALESCE(EXCLUDED.target_table,
                            remediation_execution_summary.target_table),
    quarantine_path = COALESCE(EXCLUDED.quarantine_path,
                               remediation_execution_summary.quarantine_path),
    source_modified = COALESCE(EXCLUDED.source_modified,
                               remediation_execution_summary.source_modified),
    s3_write_performed = COALESCE(EXCLUDED.s3_write_performed,
                                  remediation_execution_summary.s3_write_performed),
    started_at = COALESCE(EXCLUDED.started_at,
                          remediation_execution_summary.started_at),
    completed_at = COALESCE(EXCLUDED.completed_at,
                            remediation_execution_summary.completed_at),
    error_type = EXCLUDED.error_type,
    error_message = EXCLUDED.error_message,
    audit_details = remediation_execution_summary.audit_details
                    || EXCLUDED.audit_details,
    updated_at = NOW()
"""


def upsert_execution_summary(summary: dict) -> None:
    """Insert or update one execution summary idempotently."""

    required = (
        "execution_id",
        "dataset",
        "action",
        "execution_mode",
        "execution_status",
    )

    missing = [
        field for field in required
        if not summary.get(field)
    ]
    if missing:
        raise ValueError(
            f"Execution summary is missing required fields: {missing}"
        )

    fields = (
        "execution_id",
        "dag_id",
        "dag_run_id",
        "dataset",
        "action",
        "execution_mode",
        "execution_status",
        "approval_status",
        "approved_by",
        "plan_fingerprint",
        "invalid_count",
        "quarantined_count",
        "target_table",
        "quarantine_path",
        "source_modified",
        "s3_write_performed",
        "started_at",
        "completed_at",
        "error_type",
        "error_message",
    )

    values = [summary.get(field) for field in fields]
    values.append(json.dumps(summary.get("audit_details", {})))

    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    hook.run(UPSERT_SQL, parameters=tuple(values))
