import json
from pathlib import Path

from mcp.server import MCPServer


# ---------------------------------------------------------
# Configuration
# ---------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parent.parent

DQ_RESULTS_DIR = PROJECT_ROOT / "results" / "dq"


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

    # Prevent path traversal.
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
        encoding="utf-8"
    ) as file:
        return json.load(file)


# ---------------------------------------------------------
# ASGI application
# ---------------------------------------------------------

app = mcp.streamable_http_app()