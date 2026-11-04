
from fastapi import FastAPI

from copilot.config.settings import get_settings

settings = get_settings()

app = FastAPI(
    title=settings.app_name,
    description="AI-powered Data Engineering and DataOps Copilot",
    version=settings.app_version,
)


@app.get("/")
async def root():
    return {
        "application": settings.app_name,
        "version": settings.app_version,
        "status": "running",
    }


@app.get("/health")
async def health():
    return {
        "status": "healthy",
        "service": "copilot-api",
        "environment": settings.environment,
    }
