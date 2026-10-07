FROM datapilotai-spark:latest

USER root

# ---------------------------------------------------------
# MCP Server dependencies
# ---------------------------------------------------------

RUN pip3 install --no-cache-dir \
    mcp==2.2.0 \
    uvicorn

# ---------------------------------------------------------
# MCP Server environment
# ---------------------------------------------------------

ENV PYTHONPATH=/opt/datapilot

WORKDIR /opt/datapilot

USER spark