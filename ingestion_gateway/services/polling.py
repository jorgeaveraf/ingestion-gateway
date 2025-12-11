import asyncio
from datetime import datetime
from pathlib import Path
from typing import List, Optional

from ingestion_gateway.core.config import get_settings
from ingestion_gateway.core.models import PollResult
from ingestion_gateway.services.airflow_client import AirflowClient


def _collect_outputs(run_folder: Path) -> List[str]:
    outputs_dir = run_folder / "outputs"
    if not outputs_dir.exists():
        return []
    return [str(p) for p in outputs_dir.glob("*.csv")]


async def poll_dag_run(
    dag_id: str,
    dag_run_id: str,
    client: AirflowClient,
) -> PollResult:
    settings = get_settings()
    attempts = 0
    run_folder = Path(settings.shared_input_folder).resolve() / "runs" / dag_run_id

    while attempts < settings.max_poll_attempts:
        dag_run = await client.get_dag_run(dag_id, dag_run_id)
        state: str = dag_run.get("state", "").lower()
        if state in {"success", "failed"}:
            outputs = _collect_outputs(run_folder)
            return PollResult(
                dag_run_id=dag_run_id,
                state=state,
                details=dag_run,
                outputs=outputs or None,
                completed_at=datetime.fromisoformat(dag_run.get("end_date"))
                if dag_run.get("end_date")
                else None,
            )
        attempts += 1
        await asyncio.sleep(settings.poll_interval_seconds)

    return PollResult(
        dag_run_id=dag_run_id,
        state="running",
        details={"message": "Max polling attempts exceeded"},
    )
