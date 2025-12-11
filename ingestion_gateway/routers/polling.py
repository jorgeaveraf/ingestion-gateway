from fastapi import APIRouter, Depends, HTTPException, Query

from ingestion_gateway.core.deps import get_airflow_client
from ingestion_gateway.core.models import PollResult
from ingestion_gateway.services.polling import poll_dag_run


router = APIRouter(prefix="/poll", tags=["polling"])

_DAG_MAP = {
    "part1": "part1_ingestion",
    "part2": "part2_qbo_export",
}


@router.get("/{dag_run_id}", response_model=PollResult)
async def poll_status(
    dag_run_id: str,
    dag: str = Query(..., description="part1 or part2"),
    client=Depends(get_airflow_client),
):
    if dag not in _DAG_MAP:
        raise HTTPException(status_code=400, detail="Invalid dag value. Use part1 or part2.")
    dag_id = _DAG_MAP[dag]
    result = await poll_dag_run(dag_id, dag_run_id, client)
    return result
