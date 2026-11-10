
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator

from mcp import ClientSession
from mcp.client.streamable_http import streamable_http_client

from copilot.config.settings import get_settings


class MCPService:
    """Reusable asynchronous client for the DataPilot MCP server."""

    def __init__(self, server_url: str | None = None):
        settings = get_settings()
        self.server_url = server_url or settings.mcp_server_url

    @asynccontextmanager
    async def connect(self) -> AsyncIterator[ClientSession]:
        """Connect, initialize an MCP session, and clean up resources."""

        async with streamable_http_client(
            self.server_url
        ) as (read_stream, write_stream):
            async with ClientSession(
                read_stream,
                write_stream,
            ) as session:
                await session.initialize()
                yield session

    async def list_tools(self) -> list[dict[str, Any]]:
        """Discover tools exposed by the MCP server."""

        async with self.connect() as session:
            result = await session.list_tools()

            return [
                {
                    "name": tool.name,
                    "description": tool.description,
                }
                for tool in result.tools
            ]

    async def call_tool(
        self,
        tool_name: str,
        arguments: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Invoke an MCP tool and return its structured result."""

        async with self.connect() as session:
            result = await session.call_tool(
                tool_name,
                arguments=arguments or {},
            )

            return {
                "is_error": result.is_error,
                "structured_content": getattr(
                    result,
                    "structured_content",
                    None,
                ),
                "content": [
                    {
                        "type": item.type,
                        "text": getattr(item, "text", None),
                    }
                    for item in result.content
                ],
            }
