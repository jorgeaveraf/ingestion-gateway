import os
import shutil
from pathlib import Path
from typing import Iterable, Tuple
from uuid import uuid4

from fastapi import UploadFile

from ingestion_gateway.core.config import get_settings


def build_dag_run_id() -> str:
    """Generate a deterministic Airflow dag_run_id."""
    return f"gw_{uuid4().hex}"


def ensure_run_folder(dag_run_id: str) -> Path:
    settings = get_settings()
    base = Path(settings.shared_input_folder).resolve()
    run_folder = base / "runs" / dag_run_id
    run_folder.mkdir(parents=True, exist_ok=True)
    return run_folder


async def persist_files(run_folder: Path, files: Iterable[UploadFile]) -> Tuple[str, ...]:
    """Persist uploaded files into the run folder. Returns saved filenames."""
    saved: list[str] = []
    for upload in files:
        safe_name = os.path.basename(upload.filename or f"upload_{uuid4().hex}.csv")
        destination = run_folder / safe_name
        with destination.open("wb") as dest:
            content = await upload.read()
            dest.write(content)
        upload.file.seek(0)
        saved.append(destination.name)
    return tuple(saved)


def cleanup_run_folder(run_folder: Path) -> None:
    """
    Best-effort cleanup helper if gateway errors before handing off to Airflow.
    Airflow owns cleanup after a DAG run begins, so we only clean when the DAG was not triggered.
    """
    try:
        if run_folder.exists():
            shutil.rmtree(run_folder)
    except Exception:
        # Avoid raising during cleanup; log upstream.
        pass
