import json
import subprocess
from pathlib import Path
import os
import re
from datetime import date, datetime
from decimal import Decimal
import psycopg
from psycopg.rows import dict_row
from agents.remediation.plan_fingerprint import validate_plan_fingerprint
from mcp.server.transport_security import TransportSecuritySettings
from mcp.server import MCPServer

# ---------------------------------------------------------
# Configuration
# ---------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DQ_RESULTS_DIR = PROJECT_ROOT / "results" / "dq"
RCA_RESULTS_DIR = PROJECT_ROOT / "results" / "rca"
# ---------------------------------------------------------
# Validation helpers
# ---------------------------------------------------------

def validate_dataset(dataset: str) -> None:
    """
    Validate dataset name before using it in file paths
    or Spark commands.
    """

    if not dataset:
        raise ValueError(
            "Dataset name cannot be empty."
        )

    if (
        "/" in dataset
        or "\\" in dataset
        or ".." in dataset
    ):
        raise ValueError(
            "Invalid dataset name."
        )


def validate_column(column: str) -> None:
    """
    Validate column name before using it in Spark commands.
    """

    if not column:
        raise ValueError(
            "Column name cannot be empty."
        )

    if (
        "/" in column
        or "\\" in column
        or ".." in column
    ):
        raise ValueError(
            "Invalid column name."
        )


# ---------------------------------------------------------
# MCP Server
# ---------------------------------------------------------

mcp = MCPServer(
    "DataPilot AI MCP Server",
    instructions=(
        "Controlled MCP tools for the DataPilot AI "
        "DataOps platform."
    ),
)


# ---------------------------------------------------------
# Tool: get_dq_result
# ---------------------------------------------------------

@mcp.tool(title="Get DQ Result")
def get_dq_result(dataset: str) -> dict:
    """
    Retrieve the Data Quality result for a dataset.
    """

    validate_dataset(dataset)

    result_file = (
        DQ_RESULTS_DIR
        / f"{dataset}.json"
    )

    if not result_file.exists():
        raise FileNotFoundError(f"DQ result not found for dataset: {dataset}")

    with result_file.open("r", encoding="utf-8") as file:
        return json.load(file)


# ---------------------------------------------------------
# Tool: inspect_bronze
# ---------------------------------------------------------

@mcp.tool(title="Inspect Bronze Data")
def inspect_bronze(dataset: str, column: str, minimum: float, maximum: float) -> dict:
    """
    Inspect Bronze data using the existing Spark cluster.
    """

    validate_dataset(dataset)
    validate_column(column)

    output_filename = (f"mcp_bronze_{dataset}.json")

    container_output = ("/opt/datapilot/results/rca/"f"{output_filename}")

    host_output = (
        RCA_RESULTS_DIR
        / output_filename
    )

    command = [
        "/opt/spark/bin/spark-submit",
        "--master",
        "spark://spark-master:7077",
        "/opt/spark-apps/jobs/inspect_bronze.py",

        "--dataset",
        dataset,

        "--column",
        column,

        "--minimum",
        str(minimum),

        "--maximum",
        str(maximum),

        "--output",
        container_output,
    ]

    process = subprocess.run(
        command,
        cwd=str(PROJECT_ROOT),
        capture_output=True,
        text=True,
        check=False,
    )

    if process.returncode != 0:
        raise RuntimeError(
            "Spark inspection job failed.\n\n"
            f"STDOUT:\n{process.stdout}\n\n"
            f"STDERR:\n{process.stderr}"
        )

    if not host_output.exists():
        raise RuntimeError(
            "Spark job completed, but the result "
            f"file was not found: {host_output}"
        )

    with host_output.open("r", encoding="utf-8") as file:
        return json.load(file)


# ---------------------------------------------------------
# Tool: inspect_silver
# ---------------------------------------------------------

@mcp.tool(title="Inspect Silver Data")
def inspect_silver(dataset: str, column: str, minimum: float, maximum: float) -> dict:
    """
    Inspect Silver data using the existing Spark cluster.
    """

    validate_dataset(dataset)
    validate_column(column)

    output_filename = (f"mcp_silver_{dataset}.json")

    container_output = ("/opt/datapilot/results/rca/"f"{output_filename}")

    host_output = (
        RCA_RESULTS_DIR
        / output_filename
    )

    command = [
        "/opt/spark/bin/spark-submit",
        "--master",
        "spark://spark-master:7077",
        "/opt/spark-apps/jobs/inspect_silver.py",

        "--dataset",
        dataset,

        "--column",
        column,

        "--minimum",
        str(minimum),

        "--maximum",
        str(maximum),

        "--output",
        container_output,
    ]

    process = subprocess.run(
        command,
        cwd=str(PROJECT_ROOT),
        capture_output=True,
        text=True,
        check=False,
    )

    if process.returncode != 0:
        raise RuntimeError(
            "Spark Silver inspection job failed.\n\n"
            f"STDOUT:\n{process.stdout}\n\n"
            f"STDERR:\n{process.stderr}"
        )

    if not host_output.exists():
        raise RuntimeError(
            "Spark job completed, but the result "
            f"file was not found: {host_output}"
        )

    with host_output.open("r", encoding="utf-8") as file:
        return json.load(file)


# ---------------------------------------------------------
# Tool: get_invalid_records
# ---------------------------------------------------------

@mcp.tool(title="Get Invalid Records")
def get_invalid_records(dataset: str, column: str, minimum: float, maximum: float) -> dict:
    """
    Retrieve actual invalid Bronze records for RCA investigation.
    """

    validate_dataset(dataset)
    validate_column(column)

    output_file = (
        PROJECT_ROOT
        / "results"
        / "rca"
        / f"mcp_invalid_{dataset}.json"
    )

    output_file.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    command = [
        "/opt/spark/bin/spark-submit",

        "--master",
        "spark://spark-master:7077",

        "/opt/spark-apps/jobs/get_invalid_records.py",

        "--dataset",
        dataset,

        "--column",
        column,

        "--minimum",
        str(minimum),

        "--maximum",
        str(maximum),

        "--output",
        (
            f"/opt/datapilot/results/rca/"
            f"mcp_invalid_{dataset}.json"
        ),
    ]

    process = subprocess.run(
        command,
        cwd=str(PROJECT_ROOT),
        capture_output=True,
        text=True,
        check=False,
    )

    if process.returncode != 0:
        raise RuntimeError(
            "Spark invalid-record inspection failed.\n\n"
            f"STDOUT:\n{process.stdout}\n\n"
            f"STDERR:\n{process.stderr}"
        )

    if not output_file.exists():
        raise RuntimeError(
            "Spark job completed, but the invalid-record "
            f"result file was not found: {output_file}"
        )

    with output_file.open("r", encoding="utf-8") as file:
        return json.load(file)

# ---------------------------------------------------------
# Tool: Get Remediation Audit (read-only)
# ---------------------------------------------------------


@mcp.tool(title="Get Remediation Audit")
def get_remediation_audit(dataset: str = "", execution_id: str = "", limit: int = 10) -> dict:
    """Read recent remediation audit records. This tool never changes data."""

    dataset = dataset.strip()
    execution_id = execution_id.strip()

    if dataset:
        validate_dataset(dataset)

    if limit < 1 or limit > 20:
        raise ValueError("limit must be between 1 and 20.")

    if not dataset and not execution_id:
        raise ValueError(
            "Provide dataset or execution_id to scope the audit lookup."
        )

    sql = """
        SELECT
            execution_id,
            dag_id,
            dag_run_id,
            dataset,
            action,
            execution_mode,
            execution_status,
            approval_status,
            approved_by,
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
            created_at,
            updated_at
        FROM datapilot_audit.remediation_execution_summary
        WHERE (%s = '' OR dataset = %s)
          AND (%s = '' OR execution_id = %s)
        ORDER BY updated_at DESC
        LIMIT %s
    """

    connection_settings = {
        "host": os.environ["MCP_AUDIT_DB_HOST"],
        "port": int(os.environ.get("MCP_AUDIT_DB_PORT", "5432")),
        "dbname": os.environ["MCP_AUDIT_DB_NAME"],
        "user": os.environ["MCP_AUDIT_DB_USER"],
        "password": os.environ["MCP_AUDIT_DB_PASSWORD"],
        "connect_timeout": 5,
        "options": "-c default_transaction_read_only=on",
    }

    with psycopg.connect(**connection_settings, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            cur.execute(
                sql,
                (
                    dataset, dataset,
                    execution_id, execution_id,
                    limit,
                ),
            )
            rows = cur.fetchall()

    def json_safe(value):
        if isinstance(value, (datetime, date)):
            return value.isoformat()
        if isinstance(value, Decimal):
            return float(value)
        if isinstance(value, dict):
            return {key: json_safe(item) for key, item in value.items()}
        if isinstance(value, list):
            return [json_safe(item) for item in value]
        return value

    return {
        "read_only": True,
        "count": len(rows),
        "records": [json_safe(dict(row)) for row in rows],
    }

# ---------------------------------------------------------
# Tool: Approve Remediation
# ---------------------------------------------------------
@mcp.tool(title="Approve Remediation")
def approve_remediation(dataset: str, action: str, approved_by: str, plan_fingerprint: str) -> dict:
    """Approve only the exact remediation plan submitted for review."""

    validate_dataset(dataset)

    if action != "QUARANTINE_RECORDS":
        raise ValueError(f"Unsupported remediation action: {action}")

    if not approved_by or not approved_by.strip():
        raise ValueError("Approver identity is required.")

    if not re.fullmatch(r"[0-9a-f]{64}", plan_fingerprint):
        raise ValueError("Invalid plan fingerprint format.")

    requests_dir = (
        PROJECT_ROOT / "results" / "approvals" / "requests"
    )
    request_path = requests_dir / f"{dataset}_{plan_fingerprint}.json"

    if not request_path.is_file():
        raise FileNotFoundError("Matching approval request not found.")

    request = json.loads(request_path.read_text(encoding="utf-8"))

    if request.get("status") != "PENDING_REVIEW":
        raise ValueError("Approval request is not pending review.")

    if request.get("dataset") != dataset:
        raise ValueError("Approval request dataset mismatch.")

    if request.get("action") != action:
        raise ValueError("Approval request action mismatch.")

    if request.get("plan_fingerprint") != plan_fingerprint:
        raise ValueError("Approval request fingerprint mismatch.")

    if request.get("execution_scope") != "DRY_RUN_ONLY":
        raise ValueError("Unexpected execution scope.")

    relative_plan_path = Path(request.get("plan_path", ""))
    if relative_plan_path.is_absolute():
        raise ValueError("Absolute plan paths are not allowed.")

    plan_path = (PROJECT_ROOT / relative_plan_path).resolve()

    if not plan_path.is_relative_to(PROJECT_ROOT.resolve()):
        raise ValueError("Plan path escapes the project directory.")

    if not plan_path.is_file():
        raise FileNotFoundError("Reviewed remediation plan not found.")

    plan = json.loads(plan_path.read_text(encoding="utf-8"))

    if plan.get("dataset") != dataset:
        raise ValueError("Remediation plan dataset mismatch.")

    if plan.get("action") != action:
        raise ValueError("Remediation plan action mismatch.")

    if plan.get("status") != "PENDING_APPROVAL":
        raise ValueError("Remediation plan is not pending approval.")

    if plan.get("requires_approval") is not True:
        raise ValueError("Plan does not require approval.")

    if plan.get("execution_mode") != "DRY_RUN":
        raise ValueError("Plan is not configured for dry-run execution.")

    verified_fingerprint = validate_plan_fingerprint(plan)

    if verified_fingerprint != plan_fingerprint:
        raise ValueError("Reviewed plan fingerprint has changed.")

    approval_path = (
        PROJECT_ROOT / "results" / "approvals" / f"{dataset}.json"
    )
    # Authorize only the fingerprint-matched dry-run workflow.
    # The executor rejects APPLY under this approval scope.
    approval = {
        "dataset": dataset,
        "action": action,
        "status": "APPROVED",
        "approved_by": approved_by.strip(),
        "plan_fingerprint": verified_fingerprint,
        "execution_scope": "DRY_RUN_ONLY",
        "execution_authorized": True,
    }

    # Approval is recorded, but execution remains unauthorized.
    approval_path.parent.mkdir(parents=True, exist_ok=True)
    with approval_path.open("w", encoding="utf-8") as file:
        json.dump(approval, file, indent=2)
        file.write("\n")

    return approval




# ---------------------------------------------------------
# Tool: Reject Remediation
# ---------------------------------------------------------

@mcp.tool(title="Reject Remediation")
def reject_remediation(dataset: str, action: str, rejected_by: str, reason: str) -> dict:
    """
    Reject a remediation plan.

    This tool records rejection only.
    It does not execute anything.
    """

    validate_dataset(dataset)

    approval_path = (
        PROJECT_ROOT
        / "results"
        / "approvals"
        / f"{dataset}.json"
    )

    approval = {
        "dataset": dataset,
        "action": action,
        "status": "REJECTED",
        "rejected_by": rejected_by,
        "reason": reason,
        "execution_authorized": False,
    }

    approval_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    with approval_path.open("w", encoding="utf-8") as file:
        json.dump(approval, file, indent=2)

    return approval


# ---------------------------------------------------------
# Tool: Execute Remediation
# ---------------------------------------------------------

@mcp.tool(title="Execute Remediation")
def execute_remediation(dataset: str, action: str) -> dict:
    """
    Execute an approved remediation plan.

    The existing executor enforces approval and plan validation.
    Quarantine execution remains DRY_RUN; no S3 data is modified.
    """
    validate_dataset(dataset)

    if dataset != "olist_order_payments":
        raise ValueError(
            "Execution is currently restricted to "
            "olist_order_payments."
        )

    if action != "QUARANTINE_RECORDS":
        raise ValueError(
            "Only QUARANTINE_RECORDS is currently supported."
        )

    executor_path = (
        PROJECT_ROOT / "agents" / "remediation" / "executor.py"
    )

    if not executor_path.is_file():
        raise FileNotFoundError(
            f"Remediation executor not found: {executor_path}"
        )

    try:
        completed = subprocess.run(
            [
                "python3",
                str(executor_path),
                "--dataset",
                dataset,
                "--action",
                action,
            ],
            cwd=str(PROJECT_ROOT),
            capture_output=True,
            text=True,
            timeout=60,
            check=False,
        )
    except subprocess.TimeoutExpired as exc:
        raise RuntimeError(
            "Remediation executor timed out after 60 seconds."
        ) from exc

    if completed.returncode != 0:
        # Avoid returning arbitrary process output to the MCP client.
        raise RuntimeError(
            "Remediation execution failed. "
            f"Exit code: {completed.returncode}. "
            "Inspect the MCP server logs for details."
        )

    execution_result_path = (
        PROJECT_ROOT
        / "results"
        / "remediation"
        / f"{dataset}_execution.json"
    )

    if not execution_result_path.is_file():
        raise RuntimeError(
            "Executor finished without producing an execution record."
        )

    with execution_result_path.open("r", encoding="utf-8") as file:
        result = json.load(file)

    execution = result.get("execution", {})

    if (
        result.get("dataset") != dataset
        or result.get("action") != action
        or execution.get("mode") != "DRY_RUN"
        or execution.get("executed") is not False
        or execution.get("source_modification") is not False
    ):
        raise RuntimeError(
            "Execution record failed dry-run safety validation."
        )

    response = {
        "status": "DRY_RUN_COMPLETED",
        "dataset": dataset,
        "action": action,
        "executed": False,
        "s3_write_performed": False,
        "source_modified": False,
        "message": execution.get("message"),
        "audit_file": str(execution_result_path),
    }

    # Preserve the executor audit and add the MCP response.
    result["mcp_response"] = response

    temp_path = execution_result_path.with_suffix(".json.tmp")

    with temp_path.open("w", encoding="utf-8") as file:
        json.dump(result, file, indent=2)
        file.write("\n")
        file.flush()
        os.fsync(file.fileno())

    temp_path.replace(execution_result_path)
    return response


# ---------------------------------------------------------
# ASGI application
# ---------------------------------------------------------

transport_security = TransportSecuritySettings(
    enable_dns_rebinding_protection=True,
    allowed_hosts=[
        "127.0.0.1:8000",
        "localhost:8000",
        "mcp-server:8000",
    ],
)


app = mcp.streamable_http_app(
    transport_security=transport_security,
)