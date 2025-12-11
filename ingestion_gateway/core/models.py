from datetime import datetime
from typing import List, Optional

from pydantic import BaseModel, EmailStr, Field


class IngestMetadata(BaseModel):
    week_year: int = Field(..., ge=2000, le=2100)
    week_num: int = Field(..., ge=1, le=53)
    notify_email: EmailStr


class IngestResponse(BaseModel):
    run_id: str
    dag_run_id: str
    status: str


class PollResult(BaseModel):
    dag_run_id: str
    state: str
    details: Optional[dict] = None
    outputs: Optional[List[str]] = None
    completed_at: Optional[datetime] = None


class AirflowDagRunRequest(BaseModel):
    dag_run_id: str
    conf: dict
