import logging
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

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

# --- CORS ---
default_origins = [
    "http://localhost:5173",
    "http://127.0.0.1:5173",
]

raw_origins = os.getenv("CORS_ORIGINS")
if raw_origins:
    origins = [o.strip() for o in raw_origins.split(",") if o.strip()]
else:
    origins = default_origins

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(ingest_part1.router)
app.include_router(ingest_part2.router)
app.include_router(polling.router)


@app.get("/health")
async def health():
    return {"status": "ok"}
