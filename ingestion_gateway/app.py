import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI

from ingestion_gateway.core.config import ensure_shared_input_exists, get_settings
from ingestion_gateway.routers import ingest_part1, ingest_part2, polling


logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = get_settings()
    ensure_shared_input_exists(settings.shared_input_folder)
    logger.info("Shared input folder ready at %s", settings.shared_input_folder)
    yield


app = FastAPI(title="Ingestion Gateway", version="1.0.0", lifespan=lifespan)
app.include_router(ingest_part1.router)
app.include_router(ingest_part2.router)
app.include_router(polling.router)


@app.get("/health")
async def health():
    return {"status": "ok"}
