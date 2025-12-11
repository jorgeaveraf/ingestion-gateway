from typing import AsyncGenerator

from ingestion_gateway.services.airflow_client import AirflowClient


async def get_airflow_client() -> AsyncGenerator[AirflowClient, None]:
    client = AirflowClient()
    try:
        yield client
    finally:
        await client.close()
