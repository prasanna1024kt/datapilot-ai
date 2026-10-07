import json
import subprocess
from pathlib import Path

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

@mcp.tool(
    title="Get DQ Result",
)
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
        raise FileNotFoundError(
            f"DQ result not found for dataset: {dataset}"
        )

    with result_file.open(
        "r",
        encoding="utf-8",
    ) as file:
        return json.load(file)


# ---------------------------------------------------------
# Tool: inspect_bronze
# ---------------------------------------------------------

@mcp.tool(
    title="Inspect Bronze Data",
)
def inspect_bronze(
    dataset: str,
    column: str,
    minimum: float,
    maximum: float,
) -> dict:
    """
    Inspect Bronze data using the existing Spark cluster.
    """

    validate_dataset(dataset)
    validate_column(column)

    output_filename = (
        f"mcp_bronze_{dataset}.json"
    )

    container_output = (
        "/opt/datapilot/results/rca/"
        f"{output_filename}"
    )

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

    with host_output.open(
        "r",
        encoding="utf-8",
    ) as file:
        return json.load(file)


# ---------------------------------------------------------
# Tool: inspect_silver
# ---------------------------------------------------------

@mcp.tool(
    title="Inspect Silver Data",
)
def inspect_silver(
    dataset: str,
    column: str,
    minimum: float,
    maximum: float,
) -> dict:
    """
    Inspect Silver data using the existing Spark cluster.
    """

    validate_dataset(dataset)
    validate_column(column)

    output_filename = (
        f"mcp_silver_{dataset}.json"
    )

    container_output = (
        "/opt/datapilot/results/rca/"
        f"{output_filename}"
    )

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

    with host_output.open(
        "r",
        encoding="utf-8",
    ) as file:
        return json.load(file)


# ---------------------------------------------------------
# Tool: get_invalid_records
# ---------------------------------------------------------

@mcp.tool(
    title="Get Invalid Records",
)
def get_invalid_records(
    dataset: str,
    column: str,
    minimum: float,
    maximum: float,
) -> dict:
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

    with output_file.open(
        "r",
        encoding="utf-8",
    ) as file:
        return json.load(file)


# ---------------------------------------------------------
# Tool: Approve Remediation
# ---------------------------------------------------------

@mcp.tool(
    title="Approve Remediation",
)
def approve_remediation(
    dataset: str,
    action: str,
    approved_by: str,
) -> dict:
    """
    Approve a remediation plan.

    This tool records approval only.
    It does not execute the remediation.
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
        "status": "APPROVED",
        "approved_by": approved_by,
        "execution_authorized": True,
    }

    approval_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    with approval_path.open(
        "w",
        encoding="utf-8",
    ) as file:
        json.dump(
            approval,
            file,
            indent=2,
        )

    return approval


# ---------------------------------------------------------
# Tool: Reject Remediation
# ---------------------------------------------------------

@mcp.tool(
    title="Reject Remediation",
)
def reject_remediation(
    dataset: str,
    action: str,
    rejected_by: str,
    reason: str,
) -> dict:
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

    with approval_path.open(
        "w",
        encoding="utf-8",
    ) as file:
        json.dump(
            approval,
            file,
            indent=2,
        )

    return approval


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