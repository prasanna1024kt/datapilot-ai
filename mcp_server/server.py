import json
import subprocess
from pathlib import Path

from mcp.server import MCPServer


# ---------------------------------------------------------
# Configuration
# ---------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parent.parent

DQ_RESULTS_DIR = PROJECT_ROOT / "results" / "dq"
RCA_RESULTS_DIR = PROJECT_ROOT / "results" / "rca"


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
def inspect_bronze(dataset: str,column: str,minimum: float,maximum: float,) -> dict:
    """
    Inspect Bronze data using the existing Spark cluster.
    """

    if not dataset:
        raise ValueError(
            "Dataset name cannot be empty."
        )

    if not column:
        raise ValueError(
            "Column name cannot be empty."
        )

    # Prevent path traversal.
    if (
        "/" in dataset
        or "\\" in dataset
        or ".." in dataset
        or "/" in column
        or "\\" in column
        or ".." in column
    ):
        raise ValueError(
            "Invalid dataset or column name."
        )

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
        "docker-compose",
        "exec",
        "-T",
        "airflow-scheduler",

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

@mcp.tool(
    title="Inspect Silver Data",
)
def inspect_silver(dataset: str,column: str,minimum: float,maximum: float,) -> dict:
    """
    Inspect Silver data using the existing Spark cluster.
    """

    if not dataset:
        raise ValueError(
            "Dataset name cannot be empty."
        )

    if not column:
        raise ValueError(
            "Column name cannot be empty."
        )

    if (
        "/" in dataset
        or "\\" in dataset
        or ".." in dataset
        or "/" in column
        or "\\" in column
        or ".." in column
    ):
        raise ValueError(
            "Invalid dataset or column name."
        )

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
        "docker-compose",
        "exec",
        "-T",
        "airflow-scheduler",
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
    

@mcp.tool(title="Get Invalid Records")
def get_invalid_records(dataset: str,column: str,minimum: float,maximum: float,) -> dict:
    """
    Retrieve actual invalid Bronze records for RCA investigation.
    """

    output_file = (
        PROJECT_ROOT
        / "results"
        / "rca"
        / f"mcp_invalid_{dataset}.json"
    )

    output_file.parent.mkdir(
        parents=True,
        exist_ok=True
    )

    command = [
        "docker-compose",
        "exec",
        "-T",
        "airflow-scheduler",

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
        f"/opt/datapilot/results/rca/mcp_invalid_{dataset}.json",
    ]

    subprocess.run(
        command,
        check=True
    )

    with open(
        output_file,
        "r"
    ) as file:

        return json.load(file)
# ---------------------------------------------------------
# ASGI application
# ---------------------------------------------------------

app = mcp.streamable_http_app()