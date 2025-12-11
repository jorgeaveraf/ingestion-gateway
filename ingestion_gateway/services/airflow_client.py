from typing import Any, Dict, Optional

import httpx

from ingestion_gateway.core.config import get_settings
from ingestion_gateway.core.models import AirflowDagRunRequest


class AirflowClient:
    def __init__(self, client: Optional[httpx.AsyncClient] = None) -> None:
        self.settings = get_settings()
        self._client = client or httpx.AsyncClient(
            base_url=str(self.settings.airflow_api_url),
            auth=(self.settings.airflow_api_user, self.settings.airflow_api_pass),
            timeout=self.settings.api_timeout,
        )

    async def trigger_dag(self, dag_id: str, dag_run: AirflowDagRunRequest) -> Dict[str, Any]:
        response = await self._client.post(
            f"/dags/{dag_id}/dagRuns",
            json=dag_run.model_dump(),
        )
        response.raise_for_status()
        return response.json()

    async def get_dag_run(self, dag_id: str, dag_run_id: str) -> Dict[str, Any]:
        response = await self._client.get(f"/dags/{dag_id}/dagRuns/{dag_run_id}")
        response.raise_for_status()
        return response.json()

    async def close(self) -> None:
        await self._client.aclose()
