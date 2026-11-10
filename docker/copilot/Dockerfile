FROM python:3.11-slim

WORKDIR /app

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/app

COPY requirements-copilot.txt /app/copilot/requirements.txt

RUN pip install --no-cache-dir -r /app/copilot/requirements.txt

COPY copilot/ /app/copilot/
COPY docs/ /app/docs/
COPY agents/ /app/agents/
COPY tests/ /app/tests/

EXPOSE 8001

CMD ["uvicorn", "copilot.api.main:app", "--host", "0.0.0.0", "--port", "8001"]
