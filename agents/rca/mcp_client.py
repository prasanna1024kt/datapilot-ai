import asyncio
import json
from typing import Any

from mcp import ClientSession
from mcp.client.streamable_http import streamable_http_client


MCP_SERVER_URL = "http://127.0.0.1:8000/mcp"


def extract_tool_result(result: Any) -> Any:
    """
    Extract the actual tool payload from an MCP CallToolResult.

    MCP tools may return structuredContent or text content.
    """

    # -----------------------------------------------------
    # Preferred: structured content
    # -----------------------------------------------------

    structured_content = getattr(
        result,
        "structuredContent",
        None
    )

    if structured_content is None:
        structured_content = getattr(
            result,
            "structured_content",
            None
        )

    if structured_content:
        if isinstance(structured_content, dict):
            # MCP structured content may wrap the actual
            # result under "result".
            if "result" in structured_content:
                return structured_content["result"]

            return structured_content

    # -----------------------------------------------------
    # Fallback: content blocks
    # -----------------------------------------------------

    content = getattr(
        result,
        "content",
        None
    )

    if content:
        for item in content:

            text = getattr(
                item,
                "text",
                None
            )

            if text is None:
                continue

            try:
                return json.loads(text)
            except json.JSONDecodeError:
                return text

    # -----------------------------------------------------
    # Last resort
    # -----------------------------------------------------

    if isinstance(result, dict):
        return result

    raise RuntimeError(
        "Unable to extract payload from MCP tool result."
    )


async def call_mcp_tool(
    tool_name: str,
    arguments: dict[str, Any],
) -> Any:
    """
    Connect to the DataPilot MCP server and invoke one tool.
    """

    async with streamable_http_client(
        MCP_SERVER_URL
    ) as (
        read_stream,
        write_stream,
    ):

        async with ClientSession(
            read_stream,
            write_stream,
        ) as session:

            await session.initialize()

            result = await session.call_tool(
                tool_name,
                arguments,
            )

            return extract_tool_result(result)


def call_tool(
    tool_name: str,
    arguments: dict[str, Any],
) -> Any:
    """
    Synchronous wrapper around the async MCP client.
    """

    return asyncio.run(
        call_mcp_tool(
            tool_name,
            arguments,
        )
    )
