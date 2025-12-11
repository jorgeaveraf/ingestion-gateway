import httpx
from fastapi import APIRouter, Depends, File, Form, HTTPException, UploadFile, status

from ingestion_gateway.core.deps import get_airflow_client
from ingestion_gateway.core.models import AirflowDagRunRequest, IngestMetadata, IngestResponse
from ingestion_gateway.services.run_folder import build_dag_run_id, cleanup_run_folder, ensure_run_folder, persist_files
from ingestion_gateway.utils.validation import validate_csv_files


router = APIRouter(prefix="/ingest", tags=["ingestion"])


@router.post("/part2", response_model=IngestResponse)
async def ingest_part2(
    week_year: int = Form(...),
    week_num: int = Form(...),
    notify_email: str = Form(...),
    files: list[UploadFile] = File(...),
    client=Depends(get_airflow_client),
):
    meta = IngestMetadata(week_year=week_year, week_num=week_num, notify_email=notify_email)
    dag_id = "part2_qbo_export"
    dag_run_id = build_dag_run_id()
    run_folder = ensure_run_folder(dag_run_id)
    triggered = False
    try:
        valid_files = await validate_csv_files(files)
        await persist_files(run_folder, valid_files)
        dag_run = AirflowDagRunRequest(
            dag_run_id=dag_run_id,
            conf={
                "week_year": meta.week_year,
                "week_num": meta.week_num,
                "notify_email": meta.notify_email,
                "input_subdir": f"runs/{dag_run_id}",
            },
        )
        await client.trigger_dag(dag_id, dag_run)
        triggered = True
        return IngestResponse(run_id=dag_run_id, dag_run_id=dag_run_id, status="submitted")
    except httpx.HTTPStatusError as exc:
        raise HTTPException(
            status_code=exc.response.status_code,
            detail=f"Failed to trigger Airflow DAG {dag_id}: {exc.response.text}",
        ) from exc
    finally:
        if not triggered:
            cleanup_run_folder(run_folder)
